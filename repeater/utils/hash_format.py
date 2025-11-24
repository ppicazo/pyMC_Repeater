"""Helpers for formatting packet hash values consistently."""

from typing import Optional


def format_packet_hash(packet, length: Optional[int] = None) -> str:
    """Return the packet hash as an uppercase hex string, optionally truncated."""
    if packet is None:
        return ""

    try:
        full_hash = packet.calculate_packet_hash().hex().upper()
    except AttributeError:
        return ""

    if length is not None:
        return full_hash[:length]
    return full_hash
