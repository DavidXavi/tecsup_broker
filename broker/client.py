import socket
import threading
import time
from collections import defaultdict

from .protocol import Frame, Command


BUFFER_SIZE = 4096
RECV_TIMEOUT = 1.0


class BrokerClient:
    """Cliente del broker — un solo objeto sirve como publicador, suscriptor, o ambos."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5555):
        self.host = host
        self.port = port
        self.sock: socket.socket | None = None
        self.client_id: str = ""
        self.client_name: str = ""
        self._listeners: dict[str, list[callable]] = defaultdict(list)
        self._running = False
        self._thread: threading.Thread | None = None
        self._recv_lock = threading.Lock()
        self._socket_lock = threading.Lock()

    def connect(self) -> str:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sock.settimeout(RECV_TIMEOUT)

        welcome = self._recv_frame()
        if welcome and welcome.command == Command.OK:
            self.client_id = welcome.client_id
            self.client_name = welcome.payload.get("name", self.client_id) if welcome.payload else self.client_id
            self._running = True
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()
            print(f"[{self.client_name}] Conectado al broker. ID: {self.client_id}")
            return self.client_id
        raise ConnectionError("No se recibio respuesta del broker")

    def disconnect(self):
        self._running = False
        if self.sock:
            with self._socket_lock:
                try:
                    self._send_frame(Frame(command=Command.DISCONNECT, client_id=self.client_id))
                except OSError:
                    pass
                try:
                    self.sock.close()
                except OSError:
                    pass
                self.sock = None
        print(f"[{self.client_name}] Desconectado")

    def subscribe(self, topic: str):
        with self._socket_lock:
            self._send_frame(Frame(command=Command.SUBSCRIBE, client_id=self.client_id, topic=topic))
        # La respuesta OK la consume el listen loop y simplemente la ignorara (no es MESSAGE)
        print(f"[{self.client_name}] Suscrito a '{topic}'")

    def unsubscribe(self, topic: str):
        with self._socket_lock:
            self._send_frame(Frame(command=Command.UNSUBSCRIBE, client_id=self.client_id, topic=topic))

    def publish(self, topic: str, payload) -> str | None:
        """Publica un mensaje. Devuelve el message_id."""
        with self._socket_lock:
            self._send_frame(Frame(
                command=Command.PUBLISH,
                client_id=self.client_id,
                topic=topic,
                payload=payload,
            ))
        # La respuesta OK la consume el listen loop
        print(f"[{self.client_name}] Publicado en '{topic}'")
        return None

    def on_message(self, topic: str, callback: callable):
        self._listeners[topic].append(callback)

    def _listen_loop(self):
        """Hilo unico que lee del socket para evitar condiciones de carrera."""
        while self._running:
            try:
                frame = self._recv_frame()
                if frame is None:
                    continue
                if frame.command == Command.MESSAGE:
                    for cb in self._listeners.get(frame.topic, []):
                        cb(frame.topic, frame.payload, frame.message_id, frame.timestamp)
                    for cb in self._listeners.get("*", []):
                        cb(frame.topic, frame.payload, frame.message_id, frame.timestamp)
                # OK/ERROR/otros se ignoran en el listen loop
            except OSError:
                if self._running:
                    time.sleep(0.1)
                else:
                    break

    def _send_frame(self, frame: Frame):
        if self.sock:
            self.sock.sendall(frame.encode())

    def _recv_frame(self) -> Frame | None:
        if not self.sock:
            return None
        with self._recv_lock:
            return self._recv_one()

    def _recv_one(self) -> Frame | None:
        """Lee exactamente un frame del socket. Debe llamarse con el lock."""
        header = self._read_exact_nolock(8)
        if header is None:
            return None
        try:
            length = int(header.decode())
        except (ValueError, UnicodeDecodeError):
            return None
        if length <= 0 or length > 1024 * 1024:
            return None
        body = self._read_exact_nolock(length)
        if body is None:
            return None
        return Frame.decode(header + body)

    def _read_exact_nolock(self, n: int) -> bytes | None:
        data = bytearray()
        deadline = time.time() + 10.0
        while len(data) < n:
            try:
                chunk = self.sock.recv(n - len(data))
                if not chunk:
                    return None
                data.extend(chunk)
            except socket.timeout:
                if time.time() > deadline:
                    return None
                continue
            except OSError:
                return None
        return bytes(data)
