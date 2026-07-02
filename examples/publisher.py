"""Publicador de ejemplo — publica mensajes a un topico cada N segundos."""

import sys
import time
from broker import BrokerClient


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5555
    topic = sys.argv[3] if len(sys.argv) > 3 else "noticias"
    interval = float(sys.argv[4]) if len(sys.argv) > 4 else 2.0

    client = BrokerClient(host=host, port=port)
    client.connect()

    print(f"Publicando en '{topic}' cada {interval}s. Ctrl+C para salir.\n")

    count = 0
    try:
        while True:
            count += 1
            mensaje = {
                "numero": count,
                "contenido": f"Mensaje #{count} desde publicador de ejemplo",
                "fecha": time.strftime("%H:%M:%S"),
            }
            print(f"\n--- Mensaje #{count} ---")
            client.publish(topic, mensaje)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nSaliendo...")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
