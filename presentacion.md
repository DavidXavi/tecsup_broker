# Broker de Mensajeria — Diapositivas

> Curso: Arquitectura de Software | Tema: Arquitectura Cliente-Servidor + Pub/Sub

---

## Slide 1 — Portada

**Broker de Mensajeria: Construido desde Cero**

- Arquitectura Cliente-Servidor con Patron Publicador-Suscriptor
- Sin RabbitMQ, sin Kafka — solo Python puro
- Curso de Arquitectura de Software

---

## Slide 2 — El problema que resolvemos

Imagina un sistema con 5 servicios que necesitan comunicarse:

```
Sin broker (punto a punto):
  Servicio A ──> Servicio B
  Servicio A ──> Servicio C
  Servicio A ──> Servicio D   ← Cada servicio conoce a todos los demas
  Servicio B ──> Servicio E     ¡CAOS!

Con broker (pub/sub):
  Servicio A ──> [BROKER] ──> Servicio B
                             ──> Servicio C   ← A solo conoce al broker
                             ──> Servicio D     ¡ORDEN!
```

**Beneficio clave: DESACOPLAMIENTO.** El que envia no sabe quien recibe. El que recibe no sabe quien envia.

---

## Slide 3 — Arquitectura general

```
┌──────────┐                              ┌──────────┐
│ PUBLISHER│── PUBLICAR("clima", {...})──>│          │
└──────────┘                              │          │        ┌───────────┐
                                          │  BROKER  │───────>│SUBSCRIBER │
┌──────────┐                              │          │  clima  └───────────┘
│ PUBLISHER│── PUBLICAR("bolsa", {...})──>│          │
└──────────┘                              │          │        ┌───────────┐
                                          │          │───────>│SUBSCRIBER │
                                          └──────────┘  bolsa └───────────┘
```

- Publishers **publican** en un topico (canal tematico)
- Subscribers **se suscriben** a un topico
- El broker **enruta**: recibe de A y entrega a todos los que pidieron ese topico

---

## Slide 4 — ¿Cómo hablan entre sí? El protocolo

```
Cada mensaje en el cable tiene este formato:

┌──────────────┬───────────────────────┐
│  8 bytes     │      N bytes          │
│  00000067    │   {"cmd":"PUBLISH",   │
│  (longitud)  │    "tpc":"clima",     │
│              │    "pld":{...} }      │
└──────────────┴───────────────────────┘
```

- Prefijo de longitud: sabemos exactamente donde termina cada mensaje
- JSON como cuerpo: legible, facil de debuggear
- Comandos: CONNECT, PUBLISH, SUBSCRIBE, MESSAGE, OK, ERROR...

---

## Slide 5 — Componentes del sistema

| Componente | Funcion | Archivo |
|------------|---------|---------|
| **Protocol** | Define el "idioma" que hablan cliente y servidor | `protocol.py` (50 lineas) |
| **Server** | Acepta conexiones, enruta mensajes, orquesta todo | `server.py` (130 lineas) |
| **Client** | Libreria compartida: publicar Y suscribir | `client.py` (120 lineas) |
| **Storage** | Guarda topicos y mensajes en disco | `storage.py` (60 lineas) |

Total: ~400 lineas de codigo. Simple, didactico, funcional.

---

## Slide 6 — El servidor por dentro

```
┌─────────────────────────────────────────┐
│              BrokerServer               │
│                                         │
│  selectors.DefaultSelector()            │
│  ┌─────────────────────────────────┐   │
│  │  Event Loop (un solo hilo)      │   │
│  │                                 │   │
│  │  socket1: "llego PUBLICAR"      │   │
│  │  socket2: "llego SUSCRIBIR"     │   │
│  │  socket3: "llego PUBLICAR"      │   │
│  │  socket4: "se desconecto"       │   │
│  │  ...                            │   │
│  └─────────────────────────────────┘   │
│               │                         │
│               v                         │
│  ┌─────────────────────────────────┐   │
│  │  Router: subscriptions dict     │   │
│  │  "clima"  -> {sub1, sub3}       │   │
│  │  "bolsa"  -> {sub2}             │   │
│  │  "deporte"-> {sub1, sub2, sub4} │   │
│  └─────────────────────────────────┘   │
│               │                         │
│               v                         │
│  ┌─────────────────────────────────┐   │
│  │  MessageLog: topics.json        │   │
│  │  messages.jsonl                 │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

- **selectors**: I/O no bloqueante. Un hilo, miles de conexiones.
- **subscriptions dict**: O(1) para encontrar quien recibe cada mensaje.
- **MessageLog**: Todo queda guardado. Podes inspeccionarlo con un editor de texto.

---

## Slide 7 — ¿Por qué selectors y no un hilo por conexion?

| Enfoque | 10 conexiones | 1,000 conexiones | 10,000 conexiones |
|---------|:---:|:---:|:---:|
| Un hilo por conexion | OK | Lento | Imposible |
| selectors (un hilo) | OK | OK | OK |

**El sistema operativo nos avisa cuando hay datos listos.** No desperdiciamos recursos esperando.

Es el mismo patron que usan:
- **Nginx** — sirve millones de paginas con pocos hilos
- **Node.js** — JavaScript en el servidor con event loop
- **Redis** — base de datos en memoria con I/O no bloqueante

---

## Slide 8 — Patrones de arquitectura aplicados

| Patron | Que resuelve |
|--------|-------------|
| **Cliente-Servidor** | Separacion clara de responsabilidades: uno sirve, otros consumen |
| **Pub/Sub** | Desacoplamiento total: publishers y subscribers no se conocen |
| **Topic-based routing** | Los mensajes se organizan por tema (topico) — como canales de TV |
| **Event Loop** | Concurrencia eficiente sin complejidad de threads |
| **Length-prefixed protocol** | Framing robusto: sabes donde empieza y termina cada mensaje |
| **Append-only log** | Persistencia inmutable: cada mensaje se agrega al final del archivo |

---

## Slide 9 — Demo en vivo

**Un comando:**

```bash
python -m examples.demo
```

**Lo que pasa:**

1. Se levanta el broker en `127.0.0.1:5555`
2. Un suscriptor se conecta y se suscribe al topico "demo"
3. Un publicador se conecta y envia 5 mensajes
4. El suscriptor recibe los 5 mensajes y los muestra
5. Todos se desconectan limpiamente

**Tres terminales (mas realista):**

```bash
Terminal 1: python -m broker.server
Terminal 2: python -m examples.subscriber
Terminal 3: python -m examples.publisher
```

---

## Slide 10 — ¿Qué aprendimos?

1. **Un broker no es magia.** Es un bucle que recibe, busca en un diccionario, y reenvia.

2. **El protocolo es el contrato.** Si cliente y servidor hablan el mismo "idioma" (formato de bytes), todo funciona.

3. **I/O no bloqueante > threads.** Con `selectors`, 400 lineas de Python manejan cientos de conexiones.

4. **Pub/Sub desacopla.** El publicador no sabe si hay 0 o 1000 suscriptores. El suscriptor no sabe quien publico.

5. **Persistencia simple != inutil.** Un `.jsonl` que podes abrir con Notepad ya te da trazabilidad y auditoria.

---

## Slide 11 — ¿Y si esto fuera producción?

| Hoy (curso) | Manana (produccion) |
|-------------|-------------------|
| Un solo broker | Cluster de brokers con Raft |
| JSON en texto plano | Protobuf binario comprimido |
| Archivos JSON en disco | Write-Ahead Log + Snapshots |
| Sin autenticacion | mTLS + JWT |
| Sin garantia de entrega | ACK, reintentos, dead-letter queue |
| Sin panel de control | API REST + dashboard web |

**Pero el nucleo — el event loop, el router pub/sub, el protocolo — es el mismo.**

---

## Slide 12 — Preguntas

**¿Preguntas?**

- Codigo: todo en `arquitectura_t4/`
- Demo: `python -m examples.demo`
- Guia interactiva: abrir `guia.html` en el navegador

---

## Notas para el expositor

- **Slide 2**: Enfatizar "desacoplamiento" como la palabra magica de la arquitectura de software.
- **Slide 4**: Si hay proyector, mostrar el codigo de `protocol.py` — son solo 50 lineas.
- **Slide 6**: Dibujar en pizarra el flujo: publisher -> broker -> subscriber.
- **Slide 9**: La demo en vivo es el momento estrella. Preparar las 3 terminales de antemano.
- **Slide 11**: Dejar claro que lo que construimos es el "motor" — en produccion se le agrega carroceria, pero el motor no cambia.
