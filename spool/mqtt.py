"""Minimal MQTT 3.1.1 publisher used by the spool drainer."""

from __future__ import annotations

import socket
import struct


def encode_length(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def encode_string(value: str) -> bytes:
    raw = value.encode("utf-8")
    return struct.pack(">H", len(raw)) + raw


class MqttError(Exception):
    pass


class MqttPublisher:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str = "telemetry-spool",
        keepalive: int = 30,
        timeout: float = 5.0,
    ):
        self.host, self.port = host, port
        self.client_id, self.keepalive, self.timeout = client_id, keepalive, timeout
        self._sock: socket.socket | None = None
        self._packet_id = 0

    def connect(self) -> None:
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        payload = encode_string(self.client_id)
        remaining = 2 + len(encode_string("MQTT")) + 4 + len(payload)
        packet = (
            bytes([0x10])
            + encode_length(remaining)
            + encode_string("MQTT")
            + bytes([4, 0x02])
            + struct.pack(">H", self.keepalive)
            + payload
        )
        self._sock.sendall(packet)
        ack = self._sock.recv(4)
        if len(ack) < 4 or ack[0] != 0x20 or ack[3] != 0:
            raise MqttError("broker refused the connection")

    def close(self) -> None:
        if self._sock:
            try:
                self._sock.sendall(bytes([0xE0, 0x00]))
            except OSError:
                pass
            self._sock.close()
            self._sock = None

    def publish(self, topic: str, payload: str, qos: int = 1) -> None:
        if self._sock is None:
            raise MqttError("not connected")
        body = encode_string(topic)
        if qos:
            self._packet_id = self._packet_id % 0xFFFF + 1
            body += struct.pack(">H", self._packet_id)
        body += payload.encode()
        self._sock.sendall(bytes([0x30 | (qos << 1)]) + encode_length(len(body)) + body)
        if qos:
            ack = self._sock.recv(4)
            if len(ack) < 4 or ack[0] != 0x40:
                raise MqttError("missing PUBACK")
