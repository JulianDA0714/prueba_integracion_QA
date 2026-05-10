const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID;
const CONSUMER_NAME = process.env.CONSUMER_NAME || 'notification-consumer';

const SIMULATION_MESSAGE = 'Simulando envío de notificación al cliente...';

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
