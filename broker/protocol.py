import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Command(str, Enum):
    CONNECT = "CONNECT"
    DISCONNECT = "DISCONNECT"
    PUBLISH = "PUBLISH"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    ACK = "ACK"
    NACK = "NACK"

    OK = "OK"
    ERROR = "ERROR"
    MESSAGE = "MESSAGE"
    HEARTBEAT = "HEARTBEAT"


@dataclass
class Frame:
    command: str
    client_id: str = ""
    topic: str = ""
    payload: Any = None
    timestamp: str = ""
    message_id: str = ""

    def encode(self) -> bytes:
        raw = {
            "cmd": self.command,
            "cid": self.client_id,
            "tpc": self.topic,
            "pld": self.payload,
            "ts": self.timestamp,
            "mid": self.message_id,
        }
        body = json.dumps(raw, ensure_ascii=False)
        return f"{len(body):08d}{body}".encode("utf-8")

    @staticmethod
    def decode(raw_bytes: bytes) -> "Frame | None":
        data = raw_bytes.decode("utf-8")
        if len(data) < 8:
            return None
        try:
            length = int(data[:8])
        except ValueError:
            return None
        if len(data) < 8 + length:
            return None
        body = data[8:8 + length]
        raw = json.loads(body)
        return Frame(
            command=raw.get("cmd", ""),
            client_id=raw.get("cid", ""),
            topic=raw.get("tpc", ""),
            payload=raw.get("pld"),
            timestamp=raw.get("ts", ""),
            message_id=raw.get("mid", ""),
        )


class ProtocolError(Exception):
    pass
