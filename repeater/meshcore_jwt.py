"""MeshCore JWT helper utilities.

This module ports the Ed25519 signing, verification, and JWT helper logic
from michaelhart/meshcore-decoder so we can generate LetsMesh-compatible
auth tokens without shelling out to the Node CLI.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from typing import Any, Dict, Optional

from nacl import exceptions as nacl_exceptions
from nacl.signing import SigningKey, VerifyKey


class MeshcoreJWTError(RuntimeError):
    """Raised when JWT signing/verification fails."""


HEADER_JSON = {"alg": "Ed25519", "typ": "JWT"}


def _normalize_hex(raw: str, *, expected_nibbles: Optional[int] = None) -> str:
    if raw is None:
        raise MeshcoreJWTError("Hex value is required")

    cleaned = raw.strip()
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]

    for ch in (" ", "\n", "\r", "\t", ":"):
        cleaned = cleaned.replace(ch, "")

    if not cleaned:
        raise MeshcoreJWTError("Hex value is empty")

    if len(cleaned) % 2 != 0:
        raise MeshcoreJWTError("Hex value must have an even number of characters")

    try:
        int(cleaned, 16)
    except ValueError as exc:
        raise MeshcoreJWTError("Hex value contains non-hex characters") from exc

    if expected_nibbles is not None and len(cleaned) != expected_nibbles:
        raise MeshcoreJWTError(
            f"Hex value must be {expected_nibbles // 2} bytes ({expected_nibbles} hex chars)"
        )

    return cleaned.upper()


def _hex_to_bytes(value: str) -> bytes:
    return bytes.fromhex(value)


def _bytes_to_hex(data: bytes) -> str:
    return data.hex().upper()


def _base64url_encode(data: bytes) -> str:
    encoded = base64.urlsafe_b64encode(data)
    return encoded.rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("-"):
            numeric = stripped[1:]
            if numeric.isdigit():
                return -int(numeric)
        elif stripped.isdigit():
            return int(stripped)
    return None


def _sign(message: bytes, private_key_hex: str, public_key_hex: str) -> str:
    private_hex = _normalize_hex(private_key_hex)
    public_hex = _normalize_hex(public_key_hex, expected_nibbles=64)

    private_bytes = _hex_to_bytes(private_hex)
    if len(private_bytes) not in (32, 64):
        raise MeshcoreJWTError("Private key must be 32 or 64 bytes long")

    seed = private_bytes[:32]
    signing_key = SigningKey(seed)
    derived_public_hex = _bytes_to_hex(signing_key.verify_key.encode())

    if derived_public_hex != public_hex:
        raise MeshcoreJWTError("Private key does not match provided public key")

    signature = signing_key.sign(message).signature
    return _bytes_to_hex(signature)


def _verify(signature_hex: str, message: bytes, public_key_hex: str) -> bool:
    try:
        signature_bytes = _hex_to_bytes(_normalize_hex(signature_hex))
    except MeshcoreJWTError:
        return False

    if len(signature_bytes) != 64:
        return False

    try:
        public_bytes = _hex_to_bytes(_normalize_hex(public_key_hex, expected_nibbles=64))
    except MeshcoreJWTError:
        return False

    try:
        VerifyKey(public_bytes).verify(message, signature_bytes)
        return True
    except nacl_exceptions.BadSignatureError:
        return False


def create_auth_token(
    payload: Dict[str, Any],
    private_key_hex: str,
    public_key_hex: str,
) -> str:
    """Create a signed MeshCore JWT."""

    claims = dict(payload or {})
    normalized_public = _normalize_hex(claims.get("publicKey", public_key_hex), expected_nibbles=64)
    claims["publicKey"] = normalized_public

    coerced_iat = _coerce_int(claims.get("iat"))
    claims["iat"] = coerced_iat if coerced_iat is not None else int(time.time())

    if "exp" in claims and claims["exp"] is not None:
        coerced_exp = _coerce_int(claims["exp"])
        if coerced_exp is None:
            raise MeshcoreJWTError("exp claim must be an integer")
        claims["exp"] = coerced_exp

    header_json = json.dumps(HEADER_JSON, separators=(",", ":"))
    payload_json = json.dumps(claims, separators=(",", ":"))

    header_b64 = _base64url_encode(header_json.encode("utf-8"))
    payload_b64 = _base64url_encode(payload_json.encode("utf-8"))

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature_hex = _sign(signing_input, private_key_hex, normalized_public)

    return f"{header_b64}.{payload_b64}.{signature_hex}"


def verify_auth_token(token: str, expected_public_key_hex: Optional[str] = None) -> Optional[Dict[str, Any]]:
    parts = token.split(".")
    if len(parts) != 3:
        return None

    header_b64, payload_b64, signature_hex = parts

    try:
        header_bytes = _base64url_decode(header_b64)
        payload_bytes = _base64url_decode(payload_b64)
        header = json.loads(header_bytes.decode("utf-8"))
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, binascii.Error, ValueError):
        return None

    if header.get("alg") != "Ed25519" or header.get("typ") != "JWT":
        return None

    public_key = payload.get("publicKey")
    if not isinstance(public_key, str):
        return None

    try:
        normalized_payload_key = _normalize_hex(public_key, expected_nibbles=64)
    except MeshcoreJWTError:
        return None

    if expected_public_key_hex:
        try:
            expected = _normalize_hex(expected_public_key_hex, expected_nibbles=64)
        except MeshcoreJWTError:
            return None
        if normalized_payload_key != expected:
            return None

    iat_value = _coerce_int(payload.get("iat"))
    if iat_value is None:
        return None
    payload["iat"] = iat_value

    if "exp" in payload and payload["exp"] is not None:
        exp_value = _coerce_int(payload["exp"])
        if exp_value is None:
            return None
        if int(time.time()) > exp_value:
            return None
        payload["exp"] = exp_value

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")

    if not _verify(signature_hex, signing_input, normalized_payload_key):
        return None

    payload["publicKey"] = normalized_payload_key
    return payload


def parse_auth_token(token: str) -> Optional[Dict[str, str]]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    return {"header": parts[0], "payload": parts[1], "signature": parts[2]}


def decode_auth_token_payload(token: str) -> Optional[Dict[str, Any]]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload_b64 = parts[1]
    try:
        payload_bytes = _base64url_decode(payload_b64)
        return json.loads(payload_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, binascii.Error, ValueError):
        return None
