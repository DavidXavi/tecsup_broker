"""Suscriptor de ejemplo — escucha mensajes de un topico y los muestra en pantalla."""

import sys
from broker import BrokerClient


recibidos = 0

def mostrar_mensaje(topic: str, payload, message_id: str, timestamp: str):
    global recibidos
    recibidos += 1
    print(f"\n>> [{topic}] Recibido #{recibidos}: {payload}")
    print(f"   ID: {message_id}  |  Hora: {timestamp}")


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5555
    topic = sys.argv[3] if len(sys.argv) > 3 else "noticias"

    client = BrokerClient(host=host, port=port)
    client.connect()

    client.on_message(topic, mostrar_mensaje)
    client.subscribe(topic)

    print(f"Escuchando '{topic}'. Ctrl+C para salir.\n")

    try:
        while True:
            pass  # El hilo de escucha hace el trabajo
    except KeyboardInterrupt:
        print("\nSaliendo...")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
