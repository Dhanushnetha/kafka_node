import dotenv from 'dotenv';
import kafka from '../../config/kafka.js';
dotenv.config();

const {
  TOPIC = 'node_demo',
  GROUP_ID = 'my-app'
} = process.env;

async function run() {
  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    // allowAutoTopicCreation: false,
    // optional: control session or heartbeat timeouts via config if needed
  });

  // Graceful shutdown
  const shutdown = async (signal) => {
    try {
      console.log(`\nReceived ${signal}. Disconnecting consumer…`);
      await consumer.disconnect();
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  await consumer.connect();

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: true  // set to false to start from latest
  });

  // If you want to manually manage commits, set eachBatch or eachMessage accordingly.
  // Here we use eachMessage (auto-commit is on by default in KafkaJS).
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const val = message.value?.toString();
      const off = message.offset;

      console.log(`Logging ...[${GROUP_ID}] ${topic}[p${partition}] @ offset ${off} key=${key ?? '(none)'} value=${val}`);
      // Do your business logic here.
      // If you switch to manual commits, you'd store state & commit offsets after processing.
    }
  });

  console.log(`Consumer running in group "${GROUP_ID}" on topic "${TOPIC}".`);
}

run().catch((err) => {
  console.error('Consumer error:', err);
  process.exit(1);
});
