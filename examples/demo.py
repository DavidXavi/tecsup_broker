"""Demo completa: levanta broker + publicador + suscriptor en una sola ventana."""

import threading
import time

from broker.server import BrokerServer
from broker import BrokerClient


def main():
    # 1. Iniciar broker en segundo plano
    broker = BrokerServer(host="127.0.0.1", port=5555)
    broker_thread = threading.Thread(target=broker.start, daemon=True)
    broker_thread.start()
    time.sleep(0.5)

    # 2. Crear un suscriptor
    sub = BrokerClient()
    sub.connect()

    def mostrar(topic, payload, mid, ts):
        print(f"   [SUSCRIPTOR] Recibido: {payload}")

    sub.on_message("demo", mostrar)
    sub.subscribe("demo")
    time.sleep(0.2)

    # 3. Crear un publicador
    pub = BrokerClient()
    pub.connect()
    time.sleep(0.1)

    print("\n--- Publicando 5 mensajes ---")
    for i in range(1, 6):
        pub.publish("demo", {"msg": f"Hola mundo #{i}", "valor": i * 10})
        time.sleep(0.5)

    print("\n--- Demo completada ---")
    sub.disconnect()
    pub.disconnect()


if __name__ == "__main__":
    main()
