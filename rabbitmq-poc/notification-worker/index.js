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
