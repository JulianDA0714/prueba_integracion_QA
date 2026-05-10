# Plan completo — Taller de Pruebas de Integración

**RabbitMQ + Kafka con Node.js y Docker**

Sigue las fases en orden. Cada fase tiene su objetivo, los pasos concretos, y al final una sección de **Evidencias a capturar** — esas capturas son las que pegarás en el PDF final.

---

## Mapa general

| Fase | Qué hacemos | Tiempo estimado |
|---|---|---|
| **0** | Preparación: estructura de carpetas | 10 min |
| **1** | RabbitMQ: montar 3 contenedores y flujo feliz | 1.5–2 h |
| **2** | RabbitMQ: pruebas reactivas (worker apagado, múltiples workers) | 1 h |
| **3** | Kafka: montar 6 contenedores y flujo feliz | 2–3 h |
| **4** | Kafka: pruebas reactivas (consumer apagado, múltiples consumers) | 1 h |
| **5** | Documentación: 12 fichas de prueba + conclusiones + PDF | 2–3 h |
| **Total** | | 8–10 h |

Dividido entre 3 personas: ~3 h por cabeza.

---

## Fase 0 — Preparación

### 0.1 — Estructura de carpetas

```bash
mkdir -p ~/Documents/taller-qa-arquitecturas
cd ~/Documents/taller-qa-arquitecturas
mkdir rabbitmq-poc kafka-poc
mkdir -p evidencias/rabbitmq evidencias/kafka
```

### 0.2 — Subcarpetas de servicios

```bash
cd rabbitmq-poc
mkdir orders-api notification-worker
cd ../kafka-poc
mkdir orders-api inventory-consumer billing-consumer notification-consumer
cd ..
```

### 0.3 — Estructura final esperada

```
taller-qa-arquitecturas/
├── rabbitmq-poc/
│   ├── docker-compose.yml
│   ├── orders-api/
│   └── notification-worker/
├── kafka-poc/
│   ├── docker-compose.yml
│   ├── orders-api/
│   ├── inventory-consumer/
│   ├── billing-consumer/
│   └── notification-consumer/
└── evidencias/
    ├── rabbitmq/
    └── kafka/
```

---

# Fase 1 — RabbitMQ (montaje + flujo feliz)

## 1.1 — `docker-compose.yml`

Va en la **raíz** de `rabbitmq-poc/`.

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=admin
      - RABBITMQ_DEFAULT_PASS=admin
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  orders-api:
    build: ./orders-api
    container_name: orders-api
    ports:
      - "3000:3000"
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672
      - QUEUE_NAME=pedidos
      - PORT=3000
    depends_on:
      rabbitmq:
        condition: service_healthy

  notification-worker:
    build: ./notification-worker
    container_name: notification-worker
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672
      - QUEUE_NAME=pedidos
    depends_on:
      rabbitmq:
        condition: service_healthy
```

## 1.2 — `orders-api/package.json`

```json
{
  "name": "orders-api",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "amqplib": "^0.10.4",
    "express": "^4.19.2"
  }
}
```

## 1.3 — `orders-api/index.js`

```javascript
const express = require('express');
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());

let channel = null;

async function connectRabbitMQ(retries = 10) {
  for (let i = 0; i < retries; i++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log(`[orders-api] Conectado a RabbitMQ. Cola "${QUEUE_NAME}" lista.`);
      return;
    } catch (err) {
      console.log(`[orders-api] Intento ${i + 1}/${retries} fallido. Reintentando...`);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ');
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'orders-api' });
});

app.post('/orders', async (req, res) => {
  const { customerName, product, quantity } = req.body;

  if (!customerName || !product || !quantity) {
    return res.status(400).json({
      status: 'error',
      message: 'Faltan campos: customerName, product, quantity'
    });
  }

  if (!channel) {
    return res.status(503).json({
      status: 'error',
      message: 'Conexión con RabbitMQ no disponible'
    });
  }

  const orderId = 'ORD-' + Date.now();
  const message = {
    type: 'OrderCreatedMessage',
    orderId,
    customerName,
    product,
    quantity,
    createdAt: new Date().toISOString()
  };

  channel.sendToQueue(
    QUEUE_NAME,
    Buffer.from(JSON.stringify(message)),
    { persistent: true }
  );

  console.log(`[orders-api] Mensaje publicado en cola "${QUEUE_NAME}":`, orderId);

  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId
  });
});

async function start() {
  await connectRabbitMQ();
  app.listen(PORT, () => {
    console.log(`[orders-api] Escuchando en puerto ${PORT}`);
  });
}

start().catch(err => {
  console.error('[orders-api] Error fatal:', err);
  process.exit(1);
});
```

## 1.4 — `orders-api/Dockerfile`

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
```

## 1.5 — `orders-api/.dockerignore`

```
node_modules
npm-debug.log
.env
```

## 1.6 — `notification-worker/package.json`

```json
{
  "name": "notification-worker",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "amqplib": "^0.10.4"
  }
}
```

## 1.7 — `notification-worker/index.js`

```javascript
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'pedidos';
const WORKER_ID = process.env.HOSTNAME || 'worker-1';

async function connectRabbitMQ(retries = 10) {
  for (let i = 0; i < retries; i++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log(`[${WORKER_ID}] Conectado a RabbitMQ. Escuchando cola "${QUEUE_NAME}"...`);
      return channel;
    } catch (err) {
      console.log(`[${WORKER_ID}] Intento ${i + 1}/${retries} fallido. Reintentando...`);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ');
}

async function start() {
  const channel = await connectRabbitMQ();
  channel.prefetch(1);

  channel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    const data = JSON.parse(msg.content.toString());
    console.log(`[${WORKER_ID}] Mensaje recibido:`, data);
    console.log(`[${WORKER_ID}] Simulando envío de notificación a ${data.customerName}...`);

    await new Promise(r => setTimeout(r, 2000));

    console.log(`[${WORKER_ID}] Notificación enviada para pedido ${data.orderId}`);
    channel.ack(msg);
  });
}

start().catch(err => {
  console.error(`[${WORKER_ID}] Error fatal:`, err);
  process.exit(1);
});
```

## 1.8 — `notification-worker/Dockerfile`

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
CMD ["npm", "start"]
```

## 1.9 — `notification-worker/.dockerignore`

```
node_modules
npm-debug.log
.env
```

## 1.10 — Levantar todo

Desde `rabbitmq-poc/`:

```bash
docker compose up -d --build
docker compose ps
```

Deberías ver los 3 servicios corriendo: `rabbitmq`, `orders-api`, `notification-worker`.

Para ver logs en tiempo real:

```bash
docker compose logs -f
```

(Cierra con `Ctrl+C` cuando hayas visto suficiente, no detiene los contenedores.)

## 1.11 — Probar el flujo desde Postman

**Request:**
- Método: `POST`
- URL: `http://localhost:3000/orders`
- Headers: `Content-Type: application/json`
- Body (raw JSON):

```json
{
  "customerName": "Pedro",
  "product": "Libro de arquitectura",
  "quantity": 1
}
```

**Respuesta esperada (202):**

```json
{
  "status": "accepted",
  "message": "Pedido recibido y enviado a RabbitMQ",
  "orderId": "ORD-1234567890"
}
```

## 1.12 — Verificar en consola RabbitMQ

Abre en el navegador: **http://localhost:15672**
Usuario: `admin` · Contraseña: `admin`

- Ve a la pestaña **Queues and Streams**.
- Verifica que existe la cola `pedidos`.
- Mira los contadores **Ready / Unacked / Total**.

## 1.13 — Verificar logs del worker

```bash
docker compose logs notification-worker
```

Deberías ver `Mensaje recibido` y `Notificación enviada`.

## 📸 Evidencias a capturar — Fase 1

Guarda en `evidencias/rabbitmq/`:

1. `01-contenedores-corriendo.png` → salida de `docker compose ps`.
2. `02-postman-request.png` → Postman con el POST y la respuesta 202.
3. `03-rabbitmq-cola-pedidos.png` → consola RabbitMQ mostrando la cola.
4. `04-logs-orders-api.png` → terminal con los logs de la API publicando el mensaje.
5. `05-logs-worker.png` → terminal con los logs del worker procesando el mensaje.

---

# Fase 2 — RabbitMQ (pruebas reactivas)

## 2.1 — Prueba: consumidor apagado (CP-09 Resilient)

**Objetivo:** demostrar que la API sigue aceptando pedidos aunque el worker esté caído. Los mensajes se acumulan en la cola.

```bash
# Apagar solo el worker
docker compose stop notification-worker

# Enviar 3 pedidos desde Postman (cambia el customerName cada vez)
# - Pedro
# - María
# - Carlos
```

Verifica en consola RabbitMQ que la cola `pedidos` tiene **3 en Ready**.

```bash
# Volver a prender el worker
docker compose start notification-worker

# Ver logs
docker compose logs -f notification-worker
```

El worker procesa los 3 mensajes acumulados.

### 📸 Evidencias

6. `06-worker-apagado-cola-acumulada.png` → consola RabbitMQ con 3 mensajes en Ready.
7. `07-worker-recuperado-procesa.png` → logs del worker procesando los 3 mensajes después de volver a arrancar.

## 2.2 — Prueba: múltiples workers (CP-10 Elastic)

**Objetivo:** demostrar que RabbitMQ reparte la carga entre varios consumidores.

```bash
# Levantar 3 réplicas del worker
docker compose up -d --scale notification-worker=3

# Verificar
docker compose ps
```

Deberías ver 3 contenedores `notification-worker` (con sufijos `-1`, `-2`, `-3`).

Envía 6 pedidos seguidos desde Postman. Mira los logs:

```bash
docker compose logs notification-worker
```

Deberías ver que los mensajes se reparten entre los 3 workers (cada uno tomó 2 aproximadamente).

Vuelve a 1 worker:

```bash
docker compose up -d --scale notification-worker=1
```

### 📸 Evidencias

8. `08-multiples-workers-corriendo.png` → `docker compose ps` con 3 réplicas.
9. `09-distribucion-mensajes.png` → logs mostrando que cada worker procesó mensajes diferentes.

## 2.3 — Prueba: independencia productor-consumidor (CP-08 Message-driven)

**Objetivo:** evidenciar que la API responde **inmediatamente** sin esperar al worker.

Mira el código de `orders-api/index.js`: la respuesta `202 accepted` se devuelve **antes** de que el worker procese nada. Y el worker no tiene endpoint HTTP — solo escucha la cola.

### 📸 Evidencias

10. `10-codigo-api-no-llama-worker.png` → captura del código de la API mostrando que solo publica en la cola, no llama al worker.
11. `11-codigo-worker-sin-endpoints.png` → captura del código del worker mostrando que no expone HTTP.

## 2.4 — Bajar todo (cuando termines RabbitMQ)

```bash
docker compose down
```

> 💡 Si quieres también borrar las imágenes para empezar de cero: `docker compose down --rmi all`

---

# Fase 3 — Kafka (montaje + flujo feliz)

Trabajamos ahora desde `kafka-poc/`.

## 3.1 — `docker-compose.yml`

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: kafka
    ports:
      - "29092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - ALLOW_PLAINTEXT_LISTENER=yes

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=local
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092
    depends_on:
      - kafka

  orders-api:
    build: ./orders-api
    container_name: orders-api
    ports:
      - "3000:3000"
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - PORT=3000
    depends_on:
      - kafka

  inventory-consumer:
    build: ./inventory-consumer
    container_name: inventory-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=inventory-service-group
      - CONSUMER_NAME=inventory-consumer
    depends_on:
      - kafka

  billing-consumer:
    build: ./billing-consumer
    container_name: billing-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=billing-service-group
      - CONSUMER_NAME=billing-consumer
    depends_on:
      - kafka

  notification-consumer:
    build: ./notification-consumer
    container_name: notification-consumer
    environment:
      - KAFKA_BROKER=kafka:9092
      - KAFKA_TOPIC=orders.events
      - KAFKA_GROUP_ID=notification-service-group
      - CONSUMER_NAME=notification-consumer
    depends_on:
      - kafka
```

## 3.2 — `orders-api/package.json`

```json
{
  "name": "orders-api-kafka",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "kafkajs": "^2.2.4",
    "express": "^4.19.2"
  }
}
```

## 3.3 — `orders-api/index.js`

```javascript
const express = require('express');
const { Kafka } = require('kafkajs');
const { randomUUID } = require('crypto');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'orders-api',
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 10 }
});

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log('[orders-api] Conectado a Kafka. Tópico:', KAFKA_TOPIC);
}

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'orders-api' });
});

app.post('/orders', async (req, res) => {
  const { customerName, product, quantity, unitPrice } = req.body;

  if (!customerName || !product || !quantity || !unitPrice) {
    return res.status(400).json({
      status: 'error',
      message: 'Faltan campos: customerName, product, quantity, unitPrice'
    });
  }

  const orderId = 'ORD-' + Date.now();
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId: randomUUID(),
    data: {
      orderId,
      customerName,
      product,
      quantity,
      unitPrice,
      total: quantity * unitPrice
    }
  };

  await producer.send({
    topic: KAFKA_TOPIC,
    messages: [{ key: orderId, value: JSON.stringify(event) }]
  });

  console.log(`[orders-api] Evento OrderCreated publicado:`, orderId);

  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId
  });
});

async function start() {
  await connectProducer();
  app.listen(PORT, () => {
    console.log(`[orders-api] Escuchando en puerto ${PORT}`);
  });
}

start().catch(err => {
  console.error('[orders-api] Error fatal:', err);
  process.exit(1);
});
```

## 3.4 — `orders-api/Dockerfile`

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
```

## 3.5 — `orders-api/.dockerignore`

```
node_modules
npm-debug.log
.env
```

## 3.6 — Plantilla común para los 3 consumidores

Los tres consumidores (`inventory-consumer`, `billing-consumer`, `notification-consumer`) son **idénticos** salvo por el nombre y la simulación. Te paso la plantilla y luego solo cambias el mensaje en cada uno.

### `package.json` (igual para los 3)

```json
{
  "name": "consumer",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "kafkajs": "^2.2.4"
  }
}
```

### `Dockerfile` (igual para los 3)

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
CMD ["npm", "start"]
```

### `.dockerignore` (igual para los 3)

```
node_modules
npm-debug.log
.env
```

### `index.js` (plantilla — cambia la línea de simulación)

```javascript
const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID;
const CONSUMER_NAME = process.env.CONSUMER_NAME || 'consumer';

const SIMULATION_MESSAGE = 'Simulando acción del consumidor...'; // ← cámbialo en cada uno

const kafka = new Kafka({
  clientId: CONSUMER_NAME,
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 10 }
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

async function start() {
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });
  console.log(`[${CONSUMER_NAME}] Conectado. Grupo: ${KAFKA_GROUP_ID}. Escuchando ${KAFKA_TOPIC}...`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log(`[${CONSUMER_NAME}] Evento recibido: ${event.eventType}`);
      console.log(`[${CONSUMER_NAME}] Order ID: ${event.data.orderId}`);
      console.log(`[${CONSUMER_NAME}] ${SIMULATION_MESSAGE}`);
      console.log(`[${CONSUMER_NAME}] topic=${topic} partition=${partition} offset=${message.offset}`);
    }
  });
}

start().catch(err => {
  console.error(`[${CONSUMER_NAME}] Error fatal:`, err);
  process.exit(1);
});
```

### Mensaje de simulación de cada consumer

Cambia solo la constante `SIMULATION_MESSAGE`:

- **inventory-consumer:** `'Simulando reserva de inventario...'`
- **billing-consumer:** `'Simulando generación de factura...'`
- **notification-consumer:** `'Simulando envío de notificación al cliente...'`

## 3.7 — Levantar todo

```bash
cd kafka-poc
docker compose up -d --build
docker compose ps
```

Espera ~30 segundos para que Kafka termine de inicializar antes de probar.

## 3.8 — Probar desde Postman

**Request:**
- Método: `POST`
- URL: `http://localhost:3000/orders`
- Body:

```json
{
  "customerName": "Pedro",
  "product": "Libro de arquitectura",
  "quantity": 1,
  "unitPrice": 85000
}
```

**Respuesta:**

```json
{
  "status": "accepted",
  "message": "Pedido recibido y evento OrderCreated publicado en Kafka",
  "orderId": "ORD-..."
}
```

## 3.9 — Verificar Kafka UI

Abre **http://localhost:8080** en el navegador.

Revisa:
- Tópico `orders.events` existe.
- Cantidad de mensajes publicados.
- Contenido del JSON del evento (clic en un mensaje).
- Particiones del tópico.
- Consumer groups: deberías ver los 3 (`inventory-service-group`, `billing-service-group`, `notification-service-group`).

## 3.10 — Verificar que los 3 consumidores recibieron el evento

```bash
docker compose logs inventory-consumer
docker compose logs billing-consumer
docker compose logs notification-consumer
```

Cada uno debe mostrar el mismo evento. Esa es la prueba de que tienen consumer groups distintos.

## 📸 Evidencias a capturar — Fase 3

Guarda en `evidencias/kafka/`:

12. `12-contenedores-kafka.png` → `docker compose ps` mostrando los 6 servicios.
13. `13-postman-kafka.png` → Postman con el POST y respuesta 202.
14. `14-kafka-ui-topico.png` → Kafka UI mostrando el tópico `orders.events`.
15. `15-kafka-ui-mensaje.png` → Kafka UI con el JSON del evento abierto.
16. `16-kafka-ui-consumer-groups.png` → Kafka UI con los 3 consumer groups.
17. `17-logs-inventory.png` → logs del inventory-consumer recibiendo el evento.
18. `18-logs-billing.png` → logs del billing-consumer recibiendo el evento.
19. `19-logs-notification.png` → logs del notification-consumer recibiendo el evento.

---

# Fase 4 — Kafka (pruebas reactivas)

## 4.1 — Prueba: consumidor posterior (CP-09 Resilient + CP-12 Recuperación)

**Objetivo:** evidenciar que Kafka guarda los eventos en el log. Si un consumer está apagado, al volver puede leer todo desde el principio (`fromBeginning: true`).

```bash
# Apagar inventory-consumer
docker compose stop inventory-consumer

# Enviar 3 pedidos desde Postman
# Verificar en Kafka UI que los mensajes están en el tópico
# Verificar en logs que billing y notification SÍ los procesaron

docker compose logs billing-consumer
docker compose logs notification-consumer

# Volver a prender inventory
docker compose start inventory-consumer

# Ver logs: debe procesar los 3 acumulados
docker compose logs -f inventory-consumer
```

### 📸 Evidencias

20. `20-inventory-apagado.png` → `docker compose ps` con inventory detenido.
21. `21-eventos-en-topico.png` → Kafka UI con los mensajes acumulados.
22. `22-inventory-recupera.png` → logs de inventory procesando los eventos al volver.

## 4.2 — Prueba: múltiples consumidores en el mismo grupo (CP-10 Elastic)

**Objetivo:** demostrar que cuando varios consumers comparten grupo, Kafka **reparte** los eventos entre ellos (al revés que con grupos distintos).

```bash
# Levantar 2 réplicas de inventory-consumer
docker compose up -d --scale inventory-consumer=2

# Enviar 6 pedidos desde Postman
# Ver logs
docker compose logs inventory-consumer
```

Si tu tópico tiene solo 1 partición, solo uno de los 2 consumers va a recibir todos los eventos (el otro queda como respaldo idle). Para que se repartan necesitarías más particiones, pero **eso es justo el punto a documentar como hallazgo en las conclusiones**.

### 📸 Evidencias

23. `23-multiples-inventory.png` → 2 réplicas corriendo en `docker compose ps`.
24. `24-distribucion-en-grupo.png` → logs mostrando la distribución (o el comportamiento de respaldo).

## 4.3 — Prueba: desacoplamiento total (CP-08 + CP-11)

Lo mismo que en RabbitMQ: la API no llama a los consumers, solo publica en Kafka. Y los consumers no exponen HTTP.

### 📸 Evidencias

25. `25-codigo-api-kafka.png` → captura del código mostrando solo `producer.send()`.
26. `26-codigo-consumer-sin-http.png` → captura del código del consumer mostrando que solo se suscribe.

## 4.4 — Bajar todo

```bash
docker compose down
```

---

# Fase 5 — Documentación

## 5.1 — Las 12 fichas de prueba

Cada caso debe tener este formato (sección 7 del taller):

```
ID: CP-XX
Arquitectura: RabbitMQ / Kafka
Objetivo: …
Precondiciones: …
Datos de entrada: …
Pasos ejecutados: 1. … 2. … 3. …
Resultado esperado: …
Resultado obtenido: …
Evidencia: imagenes/XX-archivo.png
Estado: Cumple / No cumple
Análisis técnico: …
```

### Mapeo casos → evidencias

| ID | Arquitectura | Caso | Evidencias |
|---|---|---|---|
| CP-01 | RabbitMQ | Creación de pedido vía API | 02 |
| CP-02 | RabbitMQ | Envío de mensaje a la cola | 03, 04 |
| CP-03 | RabbitMQ | Consumo del mensaje | 05 |
| CP-04 | Kafka | Publicación de evento | 13, 14, 15 |
| CP-05 | Kafka | Consumo del evento | 17, 18, 19 |
| CP-06 | Ambas | Trazabilidad | 04+05 (RMQ), 17+18+19 (Kafka) |
| CP-07 | Ambas | Responsive | 02, 13 (la API responde 202 inmediato) |
| CP-08 | Ambas | Message-driven | 10+11 (RMQ), 25+26 (Kafka) |
| CP-09 | Ambas | Resilient | 06+07 (RMQ), 20+21+22 (Kafka) |
| CP-10 | Ambas | Elastic | 08+09 (RMQ), 23+24 (Kafka) |
| CP-11 | Ambas | Desacoplamiento temporal | 06+07 (RMQ), 21+22 (Kafka) |
| CP-12 | Ambas | Recuperación | 07 (RMQ), 22 (Kafka) |

## 5.2 — Sección de conclusiones

Responde las 3 preguntas del taller:

**¿Qué arquitectura es más robusta desde QA?**
Idea para la respuesta: Kafka es más robusto para auditar (log persistente, offsets, consumer groups visibles), pero RabbitMQ es más simple de operar y la consola es más directa para QA tradicional. Ambos sirven, depende del caso.

**¿Qué dificultades se presentaron en pruebas?**
Ideas: configuración de listeners en Kafka, comprender la diferencia entre cola tradicional y log de eventos, entender por qué con 1 partición no se reparten los eventos entre múltiples consumers del mismo grupo.

**¿Qué diferencias reales se evidenciaron?**
Ideas:
- RabbitMQ entrega y olvida (los mensajes se borran tras ack); Kafka conserva eventos en el log durante el retention.
- En RabbitMQ varios workers → reparto automático. En Kafka el reparto depende de particiones y consumer groups.
- En Kafka se puede agregar un consumer nuevo y leer desde el inicio del tópico; en RabbitMQ no es posible recuperar mensajes ya consumidos.

## 5.3 — Estructura del PDF final

```
1. Portada (nombres, materia, fecha, taller)
2. Introducción breve (1 párrafo: qué hicimos)
3. Montaje del entorno
   3.1 RabbitMQ — capturas 01
   3.2 Kafka — capturas 12
4. Casos de prueba funcionales
   CP-01 a CP-06 (fichas + capturas)
5. Casos de prueba reactivas
   CP-07 a CP-12 (fichas + capturas)
6. Análisis comparativo (tablita RabbitMQ vs Kafka)
7. Conclusiones
8. Anexo: docker-compose.yml de ambas arquitecturas
```

## 5.4 — Cómo generar el PDF

Recomiendo armarlo en **Google Docs** o **Word** y exportarlo a PDF al final. Razones:
- Pegas las capturas fácil.
- El profesor puede dejar comentarios si lo entregas como Doc compartido.
- La exportación a PDF queda profesional.

---

# Tips finales para sacar buena nota

1. **Capturas con timestamp visible.** Asegúrate de que se vea la hora del Mac en cada captura. Demuestra que las hiciste tú.

2. **No copies capturas genéricas.** El taller lo dice explícito: "no se aceptan capturas genéricas o copiadas".

3. **Logs en bruto, no editados.** Si pegas logs en el PDF, que se vean como salieron de la terminal (sin retoques).

4. **Comenta hallazgos extra.** Cuando notes algo curioso (ej. "los consumers en el mismo grupo no se repartieron porque solo hay 1 partición"), escríbelo. Eso muestra que entendiste, no solo seguiste pasos.

5. **No olvides la sección de errores frecuentes.** Si te encontraste con un error y lo solucionaste, documéntalo. Vale puntos en evidencia QA.

6. **Antes de entregar, revisa el checklist final** del taller (página 11 del PDF) y la rúbrica.

---

# Resumen de comandos útiles

```bash
# Levantar
docker compose up -d --build

# Ver estado
docker compose ps

# Ver logs
docker compose logs -f                         # todos
docker compose logs notification-worker        # uno solo

# Apagar uno
docker compose stop <servicio>

# Prender uno
docker compose start <servicio>

# Escalar (varios workers)
docker compose up -d --scale notification-worker=3

# Bajar todo
docker compose down

# Bajar todo incluyendo imágenes
docker compose down --rmi all
```

---

¿Listo? Empieza por la **Fase 0** y avanza con calma. Cuando quedes atascado en cualquier paso, me pegas el error y resolvemos.
