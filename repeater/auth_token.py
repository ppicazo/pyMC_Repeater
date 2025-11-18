import logging
import os
import shlex
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from .meshcore_jwt import MeshcoreJWTError, create_auth_token

logger = logging.getLogger("MQTTAuthToken")


class AuthTokenError(RuntimeError):
    """Raised when MQTT auth token generation fails."""


class AuthTokenProvider:
    """Generate and cache MQTT auth tokens for LetsMesh/letsme.sh brokers."""

    def __init__(self, global_config: Optional[dict], mqtt_config: Optional[dict], node_name: str):
        self.node_name = node_name
        self.config = mqtt_config or {}
        self.global_config = global_config or {}
        self.auth_cfg = self.config.get("auth_token", {}) or {}
        legacy_flag = self.config.get("use_auth_token")

        if self.auth_cfg.get("enabled") is not None:
            self.enabled = bool(self.auth_cfg.get("enabled"))
        elif legacy_flag is not None:
            # Support legacy flat flag "use_auth_token"
            self.enabled = bool(legacy_flag)
            if self.enabled and not self.auth_cfg:
                # Allow legacy users to keep flat values
                self.auth_cfg = self.config
        else:
            self.enabled = False

        if not self.enabled:
            self.private_key_hex = None
            return

        self.username_template = self.auth_cfg.get("username_template", "v1_{PUBLIC_KEY}")
        self.token_env_var = self.auth_cfg.get("token_env_var")
        self.token_command = self.auth_cfg.get("token_command")
        self.token_ttl = int(self.auth_cfg.get("token_ttl", 3600))
        self.refresh_margin = int(self.auth_cfg.get("token_refresh_margin", 300))
        self.token_audience = self.auth_cfg.get("token_audience", "")
        self.token_owner = self.auth_cfg.get("token_owner", "")
        self.token_email = self.auth_cfg.get("token_email", "")
        self.use_tls = bool(self.config.get("use_tls", False))
        self.tls_verify = bool(self.config.get("tls_verify", True))

        self._lock = threading.Lock()
        self._cached_token: Optional[str] = None
        self._cached_expiry: float = 0.0

        self.private_key_hex = self._resolve_private_key()
        if not self.token_env_var and not self.token_command and not self.private_key_hex:
            raise AuthTokenError(
                "MQTT auth_token enabled but no private key, token command, or token env var configured"
            )

    def _resolve_private_key(self) -> Optional[str]:
        """Locate and normalize a private key for signing auth tokens."""
        key_hex = self.auth_cfg.get("private_key_hex")
        if key_hex:
            return self._normalize_key(key_hex)

        env_var = self.auth_cfg.get("private_key_env")
        if env_var:
            env_value = os.getenv(env_var)
            if env_value:
                return self._normalize_key(env_value)
            logger.warning("Auth token private key env var %s is not set", env_var)

        key_path = self.auth_cfg.get("private_key_path")
        if key_path:
            try:
                contents = Path(key_path).expanduser().read_text(encoding="utf-8")
                return self._normalize_key(contents)
            except FileNotFoundError:
                logger.error("Auth token private key file not found: %s", key_path)
            except OSError as exc:
                logger.error("Failed reading auth token private key file %s: %s", key_path, exc)

        if self.auth_cfg.get("use_mesh_identity_key", False):
            mesh_cfg = self.global_config.get("mesh", {})
            identity_key = mesh_cfg.get("identity_key")
            if isinstance(identity_key, bytes):
                return identity_key.hex()
            if isinstance(identity_key, str):
                return self._normalize_key(identity_key)

        return None

    @staticmethod
    def _normalize_key(raw: str) -> str:
        key = raw.strip().replace(" ", "")
        key = key.replace("\n", "").replace("\r", "")
        if not key:
            return key
        # Basic validation: hex string with even length
        if len(key) % 2 != 0:
            raise AuthTokenError("Private key must contain an even number of hex characters")
        try:
            int(key, 16)
        except ValueError as exc:
            raise AuthTokenError("Private key contains non-hex characters") from exc
        return key.upper()

    def can_generate_tokens(self) -> bool:
        if not self.enabled:
            return False
        if self.token_env_var or self.token_command:
            return True
        return bool(self.private_key_hex)

    def set_private_key(self, key_hex: str) -> None:
        self.private_key_hex = self._normalize_key(key_hex)
        self.invalidate_cache()

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cached_token = None
            self._cached_expiry = 0.0

    def get_credentials(self, public_key: str, *, force_refresh: bool = False) -> Tuple[str, str]:
        if not self.enabled:
            raise AuthTokenError("Auth tokens are not enabled")
        if not public_key:
            raise AuthTokenError("Public key is required for auth tokens")

        username = self._format_username(public_key)

        if self.token_env_var:
            token = self._load_token_from_env()
            return username, token

        if self.token_command:
            token, expiry = self._run_token_command(public_key)
            with self._lock:
                self._cached_token = token
                self._cached_expiry = expiry
            return username, token

        if not self.private_key_hex:
            raise AuthTokenError("No private key available for auth token generation")

        with self._lock:
            now = time.time()
            if (
                not force_refresh
                and self._cached_token
                and now < (self._cached_expiry - self.refresh_margin)
            ):
                return username, self._cached_token

            token, expiry = self._generate_token_locally(public_key)
            self._cached_token = token
            self._cached_expiry = expiry
            return username, token

    def _format_username(self, public_key: str) -> str:
        template = self.username_template or "v1_{PUBLIC_KEY}"
        username = template.replace("{PUBLIC_KEY}", public_key.upper())
        username = username.replace("{NODE_NAME}", self.node_name)
        return username

    def _load_token_from_env(self) -> str:
        value = os.getenv(self.token_env_var) if self.token_env_var else None
        if not value:
            raise AuthTokenError(f"Token env var {self.token_env_var} is not set or empty")
        return value.strip()

    def _command_context(self, public_key: str) -> dict:
        return {
            "PUBLIC_KEY": public_key.upper(),
            "PRIVATE_KEY": self.private_key_hex or "",
            "AUDIENCE": self.token_audience,
            "OWNER": self.token_owner,
            "EMAIL": self.token_email,
            "TTL": str(self.token_ttl),
            "NODE_NAME": self.node_name,
        }

    def _run_token_command(self, public_key: str) -> Tuple[str, float]:
        context = self._command_context(public_key)
        try:
            formatted = self.token_command.format(**context)
        except KeyError as exc:
            raise AuthTokenError(f"Unknown placeholder in token_command: {exc}") from exc

        args = shlex.split(formatted)
        try:
            result = subprocess.run(args, capture_output=True, text=True, check=False)
        except FileNotFoundError as exc:
            raise AuthTokenError(f"Token command not found: {args[0]}") from exc

        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise AuthTokenError(f"Token command failed ({result.returncode}): {stderr}")

        token = result.stdout.strip()
        if not token:
            raise AuthTokenError("Token command returned empty output")

        return token, time.time() + self.token_ttl

    def _generate_token_locally(self, public_key: str) -> Tuple[str, float]:
        secure_connection = self.use_tls and self.tls_verify
        now = int(time.time())

        claims: Dict[str, object] = {
            "publicKey": public_key.upper(),
            "iat": now,
        }

        if self.token_audience:
            claims["aud"] = self.token_audience

        if secure_connection:
            if self.token_owner:
                claims["owner"] = self.token_owner
            if self.token_email:
                claims["email"] = self.token_email.lower()
        elif self.token_owner or self.token_email:
            logger.debug("Skipping owner/email claims because TLS verification is disabled")

        if self.token_ttl > 0:
            claims["exp"] = now + self.token_ttl

        effective_ttl = self.token_ttl if self.token_ttl > 0 else 3600

        try:
            token = create_auth_token(claims, self.private_key_hex, public_key.upper())
        except MeshcoreJWTError as exc:
            raise AuthTokenError(f"Failed to generate auth token: {exc}") from exc

        return token, now + effective_ttl
