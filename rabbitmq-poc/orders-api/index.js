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
