"""
MQTT Publisher Module

Fault-tolerant MQTT publishing that operates independently from the main repeater.
All MQTT operations are non-blocking and failures will not affect repeater operation.
"""

import json
import logging
import queue
import string
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from repeater.auth_token import AuthTokenError, AuthTokenProvider
from pymc_core.protocol.constants import ROUTE_TYPE_DIRECT, ROUTE_TYPE_FLOOD

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    mqtt = None

logger = logging.getLogger("MQTTPublisher")


class MQTTPublisher:
    """
    Thread-safe MQTT publisher with automatic reconnection.
    
    Design principles:
    - Non-blocking: All operations use a background thread and queue
    - Fault-tolerant: Exceptions are caught and logged, never propagated
    - Independent: MQTT failures never affect the main repeater
    - Optional: Can be disabled entirely via configuration
    """

    def __init__(self, config: dict, node_name: str, public_key: str = "", global_config: Optional[dict] = None):
        """
        Initialize MQTT publisher.
        
        Args:
            config: MQTT configuration dictionary
            node_name: Name of this repeater node
            public_key: Public key of this node (for topic templates)
        """
        self.enabled = config.get("enabled", False)
        self.config = config or {}
        self.global_config = global_config or {}
        self.node_name = node_name
        self.public_key = public_key
        self.auth_token_provider: Optional[AuthTokenProvider] = None
        
        # Statistics
        self.packets_sent = 0
        self.packets_failed = 0
        self.connection_attempts = 0
        self.last_error = None
        self.connected = False
        self.last_connect_time = None
        
        # Thread-safe message queue
        self.message_queue = queue.Queue(maxsize=1000)
        self.worker_thread = None
        self.stop_event = threading.Event()
        
        # MQTT client
        self.client = None
        
        if not self.enabled:
            logger.info("MQTT publishing is disabled")
            return
            
        if not MQTT_AVAILABLE:
            logger.error("paho-mqtt package not installed. MQTT functionality disabled.")
            self.enabled = False
            return
        
        # Validate required config
        if not self.config.get("server"):
            logger.error("MQTT server not configured. MQTT functionality disabled.")
            self.enabled = False
            return
        
        if self.enabled:
            try:
                self.auth_token_provider = AuthTokenProvider(self.global_config, self.config, self.node_name)
            except AuthTokenError as exc:
                logger.error(f"Auth token configuration error: {exc}")
                self.enabled = False
                return
        
        # Initialize MQTT client
        try:
            self._init_mqtt_client()
        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}")
            self.enabled = False
            return
        
        # Start worker thread
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info(f"MQTT publisher initialized (server: {config.get('server')})")

    def _init_mqtt_client(self):
        """Initialize and configure MQTT client."""
        if not MQTT_AVAILABLE:
            return
            
        logger.debug("_init_mqtt_client")

        client_id = self._build_client_id()
        logger.debug(f"MQTT client ID: {client_id}")

        transport = self._resolve_transport()

        client_kwargs = {
            "client_id": client_id,
            "clean_session": True,
            "protocol": mqtt.MQTTv311,
        }
        if hasattr(mqtt, "CallbackAPIVersion"):
            client_kwargs["callback_api_version"] = mqtt.CallbackAPIVersion.VERSION2
        if transport == "websockets":
            client_kwargs["transport"] = "websockets"

        # Create client
        self.client = mqtt.Client(**client_kwargs)

        if transport == "websockets":
            ws_path = self._resolve_websocket_path()
            headers_cfg = self.config.get("websocket_headers")
            ws_headers = None
            if headers_cfg:
                if isinstance(headers_cfg, dict):
                    ws_headers = {str(k): str(v) for k, v in headers_cfg.items()}
                else:
                    logger.warning("websocket_headers must be a mapping; ignoring value of type %s", type(headers_cfg).__name__)
            self.client.ws_set_options(path=ws_path, headers=ws_headers)
        
        # Set TLS if enabled
        if self.config.get("use_tls", False):
            try:
                self.client.tls_set()
                if not self.config.get("tls_verify", True):
                    import ssl
                    self.client.tls_insecure_set(True)
            except Exception as e:
                logger.warning(f"Failed to set TLS: {e}")
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        # Set last will (status = offline)
        status_topic = self._format_topic(self.config.get("topic_status", "meshcore/status"))
        status_message = json.dumps({
            "origin": self.node_name,
            "origin_id": self.public_key,
            "timestamp": datetime.now().isoformat(),
            "status": "offline"
        })
        self.client.will_set(status_topic, status_message, qos=1, retain=True)

        # Ensure credentials (token or basic) are applied before connecting
        self._ensure_credentials(force_refresh=True)

    @staticmethod
    def _reason_code_value(reason_code) -> int:
        """Best-effort conversion of a Paho reason code to an int."""
        if reason_code is None:
            return 0
        value = getattr(reason_code, "value", reason_code)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _resolve_transport(self) -> str:
        configured = self.config.get("transport")
        if configured:
            transport = str(configured).strip().lower()
        else:
            transport = self._infer_transport_default()

        if transport not in ("tcp", "websockets"):
            logger.warning("Unknown MQTT transport '%s'; falling back to TCP", transport)
            return "tcp"
        return transport

    def _infer_transport_default(self) -> str:
        server = str(self.config.get("server", "")).lower()
        websocket_hints = self.config.get("websocket_path") or self.config.get("websocket_headers")

        if self._is_letsmesh_host(server):
            logger.info("Defaulting MQTT transport to WebSockets for LetsMesh host %s", server or "<unknown>")
            return "websockets"
        if websocket_hints:
            return "websockets"
        return "tcp"

    def _resolve_websocket_path(self) -> str:
        ws_path = self.config.get("websocket_path")
        if ws_path:
            return ws_path or "/"

        server = str(self.config.get("server", "")).lower()
        if self._is_letsmesh_host(server):
            logger.debug("Using default LetsMesh WebSocket path /mqtt")
            return "/mqtt"
        return "/"

    @staticmethod
    def _is_letsmesh_host(host: str) -> bool:
        return bool(host) and ("letsmesh" in host or "letsme.sh" in host)

    def _build_client_id(self) -> str:
        """Build a broker-friendly client identifier modeled after meshcoretomqtt."""
        prefix = str(self.config.get("client_id_prefix", "pymc_repeater")).strip()
        max_len = int(self.config.get("client_id_max_length", 23))
        if max_len < 8:
            max_len = 8

        preferred_source = (self.public_key or "").strip() or (self.node_name or "")
        if not preferred_source:
            preferred_source = "pymcrepeater"

        allowed_chars = set(string.ascii_letters + string.digits + "_-")
        sanitized_prefix = "".join(ch for ch in prefix if ch in allowed_chars)
        if not sanitized_prefix:
            sanitized_prefix = "pymc_"

        sanitized_source = "".join(ch for ch in preferred_source if ch in allowed_chars)
        if not sanitized_source:
            sanitized_source = "pymcrepeater"

        candidate = f"{sanitized_prefix}{sanitized_source}".upper()
        if not candidate or not candidate[0].isalpha():
            candidate = f"A{candidate}" if candidate else "APYMCPUB"

        if len(candidate) > max_len:
            trimmed = candidate[:max_len]
            logger.info("MQTT client ID trimmed to %s (max %s)", trimmed, max_len)
            return trimmed

        if candidate != f"{prefix}{preferred_source}".upper():
            logger.info("MQTT client ID sanitized to %s", candidate)

        return candidate

    def _format_topic(self, topic_template: str) -> str:
        """
        Format topic template with variables.
        
        Supported variables:
        - {NODE_NAME}: This repeater's name
        - {PUBLIC_KEY}: This node's public key
        """
        topic = topic_template.replace("{NODE_NAME}", self.node_name)
        topic = topic.replace("{PUBLIC_KEY}", self.public_key)
        return topic

    @staticmethod
    def _format_route(route_code: Optional[int]) -> str:
        """Return the MeshCore route label expected by downstream tools."""
        if route_code == ROUTE_TYPE_FLOOD:
            return "F"
        if route_code == ROUTE_TYPE_DIRECT:
            return "D"
        return "?"

    def _get_credentials(self, *, force_refresh: bool = False) -> Tuple[Optional[str], Optional[str]]:
        """Resolve MQTT credentials, refreshing auth tokens when needed."""
        if self.auth_token_provider and self.auth_token_provider.enabled:
            if not self.public_key:
                raise AuthTokenError("Public key unavailable for auth token generation")
            username, token = self.auth_token_provider.get_credentials(
                public_key=self.public_key,
                force_refresh=force_refresh,
            )
            return username, token

        username = self.config.get("username")
        password = self.config.get("password")
        return username, password

    def _ensure_credentials(self, *, force_refresh: bool = False) -> bool:
        """Apply credentials to the MQTT client if available."""
        if not self.client:
            return False
        try:
            username, password = self._get_credentials(force_refresh=force_refresh)
        except AuthTokenError as exc:
            logger.error(f"Failed to prepare MQTT credentials: {exc}")
            self.last_error = str(exc)
            return False

        if username:
            self.client.username_pw_set(username, password)
        return True

    def set_public_key(self, public_key: str):
        """Update the device public key for topic templating and auth tokens."""
        self.public_key = public_key or ""
        if self.auth_token_provider and self.auth_token_provider.enabled and self.client:
            self._ensure_credentials(force_refresh=True)

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """Callback for when client connects to MQTT broker."""
        rc = self._reason_code_value(reason_code)
        if rc == 0:
            self.connected = True
            self.last_connect_time = time.time()
            logger.info("Connected to MQTT broker")
            
            # Publish online status
            self._publish_status("online")
        else:
            self.connected = False
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(rc, f"Connection refused - code {rc}")
            logger.error(f"MQTT connection failed: {error_msg}")
            self.last_error = error_msg

    def _on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags=None,
        reason_code=None,
        properties=None,
    ):
        """Callback for when client disconnects from MQTT broker."""
        self.connected = False
        rc_source = reason_code
        # Backward-compatibility: Paho < 1.6 passes RC as third positional arg
        if rc_source is None and disconnect_flags is not None:
            rc_source = disconnect_flags
        rc = self._reason_code_value(rc_source)
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (code {rc})")
        else:
            logger.info("Disconnected from MQTT broker")

    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        """Callback for when a message is successfully published."""
        # This runs in the paho thread, so we just increment the counter
        pass

    def _publish_status(self, status: str):
        """Publish node status message."""
        if not self.client or not self.connected:
            return
            
        try:
            topic = self._format_topic(self.config.get("topic_status", "meshcore/status"))
            message = json.dumps({
                "origin": self.node_name,
                "origin_id": self.public_key,
                "timestamp": datetime.now().isoformat(),
                "status": status
            })
            
            qos = self.config.get("qos", 0)
            retain = self.config.get("retain", False)
            self.client.publish(topic, message, qos=qos, retain=retain)
        except Exception as e:
            logger.debug(f"Failed to publish status: {e}")

    def _worker_loop(self):
        """Background worker thread that processes message queue."""
        reconnect_delay = 5  # seconds
        max_reconnect_delay = 300  # 5 minutes
        
        while not self.stop_event.is_set():
            try:
                # Connect if not connected
                if not self.connected and self.client:
                    if self.auth_token_provider and self.auth_token_provider.enabled:
                        if not self.public_key:
                            logger.debug("Waiting for public key before creating MQTT auth token")
                            time.sleep(1)
                            continue
                        if not self.auth_token_provider.can_generate_tokens():
                            logger.error("Auth token provider is not ready; disabling MQTT publishing")
                            self.enabled = False
                            break
                        if not self._ensure_credentials():
                            time.sleep(min(reconnect_delay, 5))
                            continue
                    else:
                        # Apply static credentials if provided
                        self._ensure_credentials()

                    try:
                        server = self.config.get("server")
                        port = self.config.get("port", 1883)
                        keepalive = self.config.get("keepalive", 60)
                        
                        logger.info(f"Connecting to MQTT broker {server}:{port}...")
                        self.connection_attempts += 1
                        self.client.connect(server, port, keepalive)
                        self.client.loop_start()
                        
                        # Wait for connection (with timeout)
                        timeout = 10
                        start = time.time()
                        while not self.connected and time.time() - start < timeout:
                            time.sleep(0.1)
                        
                        if self.connected:
                            reconnect_delay = 5  # Reset delay on successful connect
                        else:
                            # Connection timeout
                            logger.warning("MQTT connection timeout")
                            self.client.loop_stop()
                            time.sleep(reconnect_delay)
                            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                            continue
                            
                    except Exception as e:
                        logger.error(f"Failed to connect to MQTT broker: {e}")
                        self.last_error = str(e)
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue
                
                # Process message queue
                try:
                    # Wait for message with timeout so we can check stop_event
                    topic, payload = self.message_queue.get(timeout=1.0)
                    
                    if self.connected and self.client:
                        try:
                            qos = self.config.get("qos", 0)
                            retain = self.config.get("retain", False)
                            logger.debug(
                                "Publishing MQTT message topic=%s qos=%s retain=%s bytes=%s",
                                topic,
                                qos,
                                retain,
                                len(payload) if isinstance(payload, (bytes, str)) else "?",
                            )
                            result = self.client.publish(topic, payload, qos=qos, retain=retain)
                            
                            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                                self.packets_sent += 1
                                logger.debug("MQTT publish succeeded (mid=%s, topic=%s)", result.mid, topic)
                            else:
                                self.packets_failed += 1
                                logger.warning(
                                    "MQTT publish failed (mid=%s, rc=%s, topic=%s)",
                                    getattr(result, "mid", "?"),
                                    result.rc,
                                    topic,
                                )
                        except Exception as e:
                            self.packets_failed += 1
                            logger.debug(f"Exception publishing message: {e}")
                    else:
                        # Not connected, try to put message back in queue
                        try:
                            logger.debug("MQTT not connected; requeueing topic %s", topic)
                            self.message_queue.put_nowait((topic, payload))
                        except queue.Full:
                            self.packets_failed += 1
                            logger.warning("MQTT queue full while requeueing topic %s; dropping message", topic)
                    
                    self.message_queue.task_done()
                    
                except queue.Empty:
                    # No messages, that's fine
                    pass
                    
            except Exception as e:
                # Catch-all to ensure thread never dies
                logger.error(f"Exception in MQTT worker thread: {e}")
                time.sleep(1)
        
        # Clean shutdown
        if self.client:
            try:
                self._publish_status("offline")
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass
        
        logger.info("MQTT worker thread stopped")

    def publish_packet(self, packet_data: Dict[str, Any]):
        """
        Publish a packet to MQTT (non-blocking).
        
        Args:
            packet_data: Dictionary containing packet information
        """
        if not self.enabled or not self.client:
            return
        
        try:
            # Capture timestamp once for consistency
            ts_value = packet_data.get("timestamp")
            if isinstance(ts_value, (int, float)):
                ts_dt = datetime.fromtimestamp(ts_value)
            else:
                ts_dt = datetime.now()
            timestamp_iso = ts_dt.isoformat()

            direction = "tx" if packet_data.get("transmitted") else "rx"
            length_field = str(packet_data.get("length", packet_data.get("payload_length", 0)))
            payload_len_field = str(packet_data.get("payload_length", 0))

            route_label = self._format_route(packet_data.get("route"))
            payload_hex = (
                packet_data.get("raw_packet")
                or packet_data.get("payload")
                or ""
            )
            if isinstance(payload_hex, str):
                payload_hex = payload_hex.upper()
            else:
                payload_hex = str(payload_hex).upper()

            message = {
                "origin": self.node_name,
                "origin_id": self.public_key,
                "timestamp": timestamp_iso,
                "type": "PACKET",
                "direction": direction,
                "time": ts_dt.strftime("%H:%M:%S"),
                "date": ts_dt.strftime("%d/%m/%Y"),
                "len": length_field,
                "packet_type": str(packet_data.get("type", 0)),
                "route": route_label,
                "payload_len": payload_len_field,
                "raw": payload_hex,
            }

            if direction == "rx":
                message["SNR"] = str(packet_data.get("snr", 0))
                message["RSSI"] = str(packet_data.get("rssi", 0))
                score_value = packet_data.get("score")
                if score_value is not None:
                    message["score"] = str(score_value)
                hash_value = packet_data.get("packet_hash")
                if hash_value:
                    message["hash"] = str(hash_value).upper()[:16]

            duration_ms = packet_data.get("tx_delay_ms")
            if duration_ms is not None:
                message["duration"] = str(duration_ms)
            
            # Add optional fields
            if packet_data.get("src_hash"):
                message["src_hash"] = packet_data["src_hash"]
            if packet_data.get("dst_hash"):
                message["dst_hash"] = packet_data["dst_hash"]
            if route_label == "D":
                path_hops = packet_data.get("original_path") or packet_data.get("forwarded_path")
                if path_hops:
                    hop_strings = [str(hop) for hop in path_hops if hop is not None]
                    if hop_strings:
                        message["path"] = " -> ".join(hop_strings)
            if "path" not in message and route_label == "D" and packet_data.get("path_hash"):
                message["path"] = packet_data["path_hash"]
            
            # Convert to JSON
            payload = json.dumps(message)
            
            # Determine topic
            topic = self._format_topic(self.config.get("topic_packets", "meshcore/packets"))
            
            # Queue for publishing (non-blocking)
            try:
                logger.debug("Queueing MQTT packet for topic %s", topic)
                self.message_queue.put_nowait((topic, payload))
            except queue.Full:
                # Queue is full, drop message (this prevents blocking)
                self.packets_failed += 1
                logger.warning("MQTT queue full, dropping packet for topic %s", topic)
                
        except Exception as e:
            # Never let exceptions escape
            logger.debug(f"Exception formatting MQTT message: {e}")
            self.packets_failed += 1

    def publish_raw_packet(self, raw_data: str):
        """
        Publish raw packet data to MQTT (for map.w0z.is compatibility).
        
        Args:
            raw_data: Raw hex packet data
        """
        if not self.enabled or not self.client:
            return
        
        try:
            message = {
                "origin": self.node_name,
                "origin_id": self.public_key,
                "timestamp": datetime.now().isoformat(),
                "raw": raw_data
            }
            
            payload = json.dumps(message)
            topic = self._format_topic(self.config.get("topic_raw", "meshcore/raw"))
            
            try:
                logger.debug("Queueing MQTT raw payload for topic %s", topic)
                self.message_queue.put_nowait((topic, payload))
            except queue.Full:
                self.packets_failed += 1
                logger.warning("MQTT queue full, dropping raw packet for topic %s", topic)
                
        except Exception as e:
            logger.debug(f"Exception formatting raw MQTT message: {e}")
            self.packets_failed += 1

    def stop(self):
        """Stop the MQTT publisher and clean up resources."""
        if not self.enabled:
            return
        
        logger.info("Stopping MQTT publisher...")
        self.stop_event.set()
        
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)
        
        logger.info("MQTT publisher stopped")
