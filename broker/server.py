import selectors
import socket
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from .protocol import Frame, Command, ProtocolError
from .storage import MessageLog


BUFFER_SIZE = 4096


class BrokerServer:
    """Servidor del broker — el "cartero" que enruta mensajes entre publicadores y suscriptores."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5555):
        self.host = host
        self.port = port
        self.sel = selectors.DefaultSelector()
        self.storage = MessageLog()

        # client_id -> socket
        self.clients: dict[str, socket.socket] = {}
        # client_id -> name (human readable)
        self.client_names: dict[str, str] = {}
        # topic -> set of client_ids
        self.subscriptions: dict[str, set[str]] = defaultdict(set)
        # socket -> bytes buffer
        self.buffers: dict[socket.socket, bytearray] = defaultdict(bytearray)

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(64)
        server.setblocking(False)
        self.sel.register(server, selectors.EVENT_READ, data=None)
        print(f"[Broker] Escuchando en {self.host}:{self.port}")

        try:
            while True:
                events = self.sel.select(timeout=1.0)
                for key, mask in events:
                    if key.data is None:
                        self._accept(key.fileobj)
                    else:
                        self._handle_client(key.fileobj)
        except KeyboardInterrupt:
            print("\n[Broker] Apagando...")
        finally:
            self.sel.close()

    def _accept(self, sock: socket.socket):
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, data="client")
        cid = uuid.uuid4().hex[:8]
        self.clients[cid] = conn
        self.client_names[cid] = f"cliente-{cid[:4]}"
        print(f"[Broker] + {self.client_names[cid]} ({addr[0]}:{addr[1]})")
        self._send(conn, Frame(command=Command.OK, client_id=cid, payload={
            "message": "Conectado al broker",
            "name": self.client_names[cid],
        }))

    def _handle_client(self, sock: socket.socket):
        try:
            data = sock.recv(BUFFER_SIZE)
        except (ConnectionResetError, ConnectionAbortedError):
            self._disconnect(sock)
            return

        if not data:
            self._disconnect(sock)
            return

        self.buffers[sock].extend(data)

        while True:
            frame = Frame.decode(self.buffers[sock])
            if frame is None:
                break
            consumed = 8 + int(self.buffers[sock][:8].decode())
            self.buffers[sock] = self.buffers[sock][consumed:]
            self._dispatch(sock, frame)

    def _dispatch(self, sock: socket.socket, frame: Frame):
        cid = frame.client_id

        if frame.command == Command.SUBSCRIBE:
            topic = frame.topic
            self.subscriptions[topic].add(cid)
            self.storage.register_topic(topic)
            print(f"[Broker] {self.client_names.get(cid, cid)} se suscribe a '{topic}'")
            self._send(sock, Frame(command=Command.OK, payload={"message": f"Suscrito a '{topic}'"}))

        elif frame.command == Command.UNSUBSCRIBE:
            topic = frame.topic
            self.subscriptions[topic].discard(cid)
            self._send(sock, Frame(command=Command.OK, payload={"message": f"Desuscrito de '{topic}'"}))

        elif frame.command == Command.PUBLISH:
            topic = frame.topic
            payload = frame.payload
            mid = self.storage.save_message(topic, payload)
            ts = datetime.now(timezone.utc).isoformat()
            print(f"[Broker] {self.client_names.get(cid, cid)} publica en '{topic}': {str(payload)[:60]}")

            # Enrutar a suscriptores
            out = Frame(
                command=Command.MESSAGE,
                topic=topic,
                payload=payload,
                timestamp=ts,
                message_id=mid,
            )
            delivered = 0
            for sub_id in list(self.subscriptions.get(topic, set())):
                if sub_id in self.clients:
                    try:
                        self._send(self.clients[sub_id], out)
                        delivered += 1
                    except OSError:
                        pass

            # Confirmar al publicador
            self._send(sock, Frame(command=Command.OK, payload={
                "message_id": mid,
                "delivered_to": delivered,
            }))

        elif frame.command == Command.DISCONNECT:
            self._send(sock, Frame(command=Command.OK, payload={"message": "Adios"}))
            self._disconnect(sock)

        elif frame.command == Command.HEARTBEAT:
            self._send(sock, Frame(command=Command.OK, payload={"message": "pong"}))

    def _send(self, sock: socket.socket, frame: Frame):
        try:
            sock.sendall(frame.encode())
        except OSError:
            self._disconnect(sock)

    def _disconnect(self, sock: socket.socket):
        cid = None
        for c, s in list(self.clients.items()):
            if s is sock:
                cid = c
                break
        if cid:
            name = self.client_names.pop(cid, cid)
            del self.clients[cid]
            for subs in self.subscriptions.values():
                subs.discard(cid)
            print(f"[Broker] - {name} desconectado")
        try:
            self.sel.unregister(sock)
        except Exception:
            pass
        try:
            sock.close()
        except OSError:
            pass
        self.buffers.pop(sock, None)


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5555
    BrokerServer(host=host, port=port).start()


if __name__ == "__main__":
    main()
