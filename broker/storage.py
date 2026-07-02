import json
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from .protocol import Frame, Command


class MessageLog:
    """Almacenamiento simple en disco para trazabilidad del curso."""

    def __init__(self, log_path: str = "broker_data"):
        self.log_path = log_path
        self._lock = threading.Lock()
        os.makedirs(log_path, exist_ok=True)
        self._topics_file = os.path.join(log_path, "topics.json")
        self._messages_file = os.path.join(log_path, "messages.jsonl")
        self._init_files()

    def _init_files(self):
        if not os.path.exists(self._topics_file):
            with open(self._topics_file, "w", encoding="utf-8") as f:
                json.dump([], f)
        if not os.path.exists(self._messages_file):
            open(self._messages_file, "w", encoding="utf-8").close()

    def register_topic(self, topic: str):
        with self._lock:
            topics = self._load_topics()
            if topic not in topics:
                topics.append(topic)
                self._save_topics(topics)

    def _load_topics(self) -> list[str]:
        try:
            with open(self._topics_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _save_topics(self, topics: list[str]):
        with open(self._topics_file, "w", encoding="utf-8") as f:
            json.dump(topics, f, ensure_ascii=False, indent=2)

    def save_message(self, topic: str, payload: Any) -> str:
        mid = uuid.uuid4().hex[:12]
        ts = datetime.now(timezone.utc).isoformat()
        entry = {
            "message_id": mid,
            "topic": topic,
            "payload": payload,
            "timestamp": ts,
        }
        with self._lock:
            with open(self._messages_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        return mid

    def get_messages_for(self, topic: str, limit: int = 50) -> list[dict]:
        result = []
        try:
            with open(self._messages_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    entry = json.loads(line)
                    if entry.get("topic") == topic:
                        result.append(entry)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
        return result[-limit:]

    def get_topics(self) -> list[str]:
        return self._load_topics()
