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
