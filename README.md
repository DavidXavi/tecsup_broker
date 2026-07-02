# Broker de Mensajeria — Arquitectura Cliente-Servidor

> Proyecto para curso de Arquitectura de Software. Broker propio desde cero, sin dependencias externas.

---

## La idea en una frase

Imagina un **cartero inteligente** que recibe paquetes de unos y los entrega solo a quienes los pidieron. Eso es un broker de mensajeria: desacopla quien envia de quien recibe.

---

## ¿Por qué no usar RabbitMQ o Kafka?

Porque la meta del curso es **entender como funciona por dentro**, no solo usarlo. Al construirlo tu mismo ves:

- Como se serializan los mensajes (protocolo de red)
- Como el servidor maneja multiples conexiones simultaneas (concurrencia)
- Como se enrutan mensajes entre publicadores y suscriptores (pub/sub)
- Como se persisten los datos en disco (storage)

---

## Arquitectura

```
┌──────────────────┐         ┌──────────────────┐
│   Publisher 1    │───────>│                  │
│  (publica en     │        │     BROKER       │
│   "noticias")    │        │                  │
└──────────────────┘        │  127.0.0.1:5555  │
                            │                  │
┌──────────────────┐        │  ┌────────────┐  │
│   Publisher 2    │───────>│  │  Selector   │  │
│  (publica en     │        │  │  (eventos)  │  │
│   "deportes")    │        │  └────────────┘  │
└──────────────────┘        │        │          │
                            │        v          │
                            │  ┌────────────┐  │
                            │  │   Router   │  │
                            │  │ pub → sub  │  │
                            │  └────────────┘  │
┌──────────────────┐        │        │          │        ┌──────────────────┐
│   Subscriber 1   │<───────│  ┌─────┴──────┐  │        │   Subscriber 2   │
│  (suscrito a     │        │  │  MessageLog │  │<───────│  (suscrito a     │
│   "noticias")    │        │  │  (disco)   │  │        │   "*" todo)      │
└──────────────────┘        │  └────────────┘  │        └──────────────────┘
                            └──────────────────┘
```

### Componentes

| Componente | Archivo | Funcion |
|------------|---------|---------|
| Protocolo | `broker/protocol.py` | Formato de mensajes: frames con prefijo de longitud + JSON |
| Servidor | `broker/server.py` | Acepta conexiones, enruta mensajes, maneja topicos y suscripciones |
| Cliente | `broker/client.py` | Libreria unica para publicar y suscribirse |
| Persistencia | `broker/storage.py` | Guarda topicos y mensajes en disco (JSON) |

### Flujo de un mensaje

```
1. Publisher envia PUBLISH {topic: "noticias", payload: {...}}
2. Broker lo recibe, lo guarda en disco (MessageLog)
3. Broker busca en subscriptions: quien esta suscrito a "noticias"?
4. Broker reenvia el MESSAGE a cada suscriptor
5. Broker confirma al publisher: OK, entregado a N suscriptores
```

### Formato del protocolo (wire format)

```
┌────────────┬─────────────────────┐
│  8 bytes   │     N bytes         │
│  longitud  │  JSON (cmd, tpc,    │
│  del JSON  │   pld, cid, etc.)   │
└────────────┴─────────────────────┘
```

Ejemplo de frame real:

```
00000067{"cmd":"PUBLISH","cid":"abc123","tpc":"noticias","pld":{"texto":"Hola"},"ts":"2026-07-01T...","mid":"a1b2c3d4"}
```

---

## Como ejecutar

### Requisitos

Python 3.10+

### Opcion A: Demo automatica (un solo comando)

```bash
python -m examples.demo
```

Levanta broker + publicador + suscriptor, envia 5 mensajes, muestra todo en pantalla.

### Opcion B: Manual (3 terminales)

**Terminal 1 — Broker:**

```bash
python -m broker.server
```

**Terminal 2 — Suscriptor:**

```bash
python -m examples.subscriber
```

**Terminal 3 — Publicador noticias:**

```bash
python -m examples.publisher
```

### Parametros personalizables

```bash
python -m broker.server 0.0.0.0 7777          # host y puerto
python -m examples.publisher 127.0.0.1 7777 clima 3.0   # topic="clima", cada 3s
python -m examples.subscriber 127.0.0.1 7777 clima      # escucha "clima"
```

---

## ¿Qué patrones de arquitectura se usan?

| Patron | Donde | Por que |
|--------|-------|---------|
| **Cliente-Servidor** | Broker ↔ Clientes | El servidor centraliza el enrutamiento |
| **Publicador-Suscriptor** | Publisher ↔ Broker ↔ Subscriber | Desacopla emisores de receptores |
| **Topico** | Ruteo por `topic` | Cada topico es un canal independiente |
| **Event Loop** | `selectors` en el servidor | Maneja muchas conexiones sin un hilo por conexion |
| **Protocolo de longitud prefijada** | `Frame.encode/decode` | Sabe exactamente donde termina cada mensaje |

---

## Broker vs SOA — ¿en qué se diferencian?

Son parecidos porque ambos desacoplan, pero resuelven problemas distintos:

| Dimension | Broker (Pub/Sub) | SOA (Service-Oriented Architecture) |
|-----------|-----------------|-------------------------------------|
| **¿Qué orquesta?** | Mensajes — datos que fluyen | Servicios — lógica de negocio |
| **Rol central** | Cartero mudo: entrega y se olvida | Orquestador inteligente: sabe de negocios |
| **Comunicación** | Uno publica, muchos reciben (1:N, asíncrono) | Request-response entre pares (1:1, síncrono) |
| **Acoplamiento** | El emisor no sabe quién recibe | El consumidor conoce el contrato (WSDL, REST) |
| **Estado/procesamiento** | El broker no transforma mensajes | El ESB puede transformar, enrutar, enriquecer |
| **Gobernanza** | Tópicos libres, sin schema formal | Catálogo de servicios, schemas tipados (XSD, WSDL) |
| **Entrega** | At-most-once → At-least-once → Exactly-once | Request-response inmediato con status code |
| **Desacople temporal** | El suscriptor puede estar offline y recibir después | Ambos deben estar online al mismo tiempo |

### Analogía

- **SOA** es como una **cadena de montaje** en una fábrica: cada estación sabe exactamente qué recibe, qué produce, y un supervisor controla el flujo completo de extremo a extremo.
- Un **broker** es el **sistema de correo interno** de esa misma fábrica: transporta papeles entre estaciones sin leerlos ni modificarlos. La fábrica (SOA) puede usar correo interno (broker), pero también otros mecanismos.

### ¿Compiten o se complementan?

**Se complementan.** SOA define contratos, esquemas y gobierna cómo los servicios colaboran. Un broker es la herramienta que SOA usa para la mensajería asíncrona. De hecho, en la práctica moderna (microservicios + event-driven), los servicios se comunican por REST/gRPC cuando necesitan respuesta inmediata, y por un broker cuando necesitan desacople y escalabilidad. El broker no reemplaza a SOA: es infraestructura dentro de SOA.

---

## Diagramas

Generados con Mermaid. Fuente editable en `diagrams/`:

| Diagrama | Archivo | Descripción |
|----------|---------|-------------|
| Arquitectura general | `diagrams/arquitectura.{mmd,png,svg}` | Publishers, Broker y Subscribers |
| Flujo de un mensaje | `diagrams/flujo-mensaje.{mmd,png,svg}` | Secuencia Pub → Broker → Subs |
| Servidor por dentro | `diagrams/servidor-interno.{mmd,png,svg}` | Event loop, router y storage |

Para regenerar: `mmdc -i diagrams/<archivo>.mmd -o diagrams/<archivo>.png -b transparent`

---

## Estructura del proyecto

```
arquitectura_t4/
├── broker/
│   ├── __init__.py      # Paquete
│   ├── protocol.py      # Formato de mensajes (Frame, Command)
│   ├── server.py        # Servidor del broker (selectors, enrutamiento)
│   ├── client.py        # Cliente unico (publicar + suscribir)
│   └── storage.py       # Persistencia en disco (topics.json, messages.jsonl)
├── examples/
│   ├── __init__.py
│   ├── demo.py          # Demo completa en una ventana
│   ├── publisher.py     # Publicador de ejemplo
│   └── subscriber.py    # Suscriptor de ejemplo
├── diagrams/
│   ├── arquitectura.{mmd,png,svg}   # Diagrama de arquitectura general
│   ├── flujo-mensaje.{mmd,png,svg}  # Diagrama de secuencia de un mensaje
│   └── servidor-interno.{mmd,png,svg} # Diagrama interno del servidor
├── README.md            # Este documento
├── presentacion.md      # Diapositivas para exponer
└── guia.html            # Guia interactiva en el navegador
```

---

## Decisiones de diseno (el "por que" detras de cada eleccion)

### ¿Por qué TCP y no HTTP?

HTTP es request-response: el cliente pregunta y el servidor responde. En un broker necesitamos que el servidor le **empuje** mensajes al suscriptor sin que este los pida. TCP nos da un canal bidireccional persistente.

### ¿Por qué `selectors` y no threads?

Un hilo por conexion es simple pero no escala. Con 10,000 suscriptores tendrias 10,000 hilos compitiendo. `selectors` usa **I/O no bloqueante**: un solo hilo atiende todas las conexiones, despertandose solo cuando hay datos listos. Es lo mismo que hacen Nginx, Node.js y Redis en sus nucleos.

### ¿Por qué JSON y no un protocolo binario?

Para un curso, la legibilidad manda. Abrir Wireshark o un `print` y ver `{"cmd":"PUBLISH",...}` es inmediato. En produccion usarias protobuf o MessagePack — pero aqui la meta es entender, no optimizar.

### ¿Por qué prefijo de longitud y no delimitadores?

Si usas `\n` como delimitador, el payload no puede contener saltos de linea. Con un prefijo de 8 digitos (`00000123{...}`), sabes exactamente cuantos bytes leer sin importar el contenido.

### ¿Por qué persistencia en JSON plano?

Para que puedas abrir `broker_data/messages.jsonl` con cualquier editor y ver el historial. En un sistema real usarias una write-ahead log o SQLite, pero para el curso esto es suficiente y didactico.

---

## Limitaciones conocidas (y como se arreglarian en produccion)

| Limitacion | Solucion productiva |
|------------|-------------------|
| Un solo proceso de broker | Clustering con Raft/Paxos para alta disponibilidad |
| Sin colas de mensajes pendientes (offline subscribers) | Almacenar mensajes por suscriptor con offset (como Kafka) |
| Sin autenticacion | TLS + tokens JWT |
| Sin garantia de entrega (at-most-once) | ACK explicito + reintentos con backoff |
| JSON como serializacion | Protobuf / Avro con schema registry |
| Persistencia en archivos planos | Write-Ahead Log + snapshots periodicos |
