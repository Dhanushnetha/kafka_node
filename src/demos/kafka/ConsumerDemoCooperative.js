import kafka from '../../config/kafka.js';
import logger from '../../config/logger.js';

const groupId = 'my-java-application';
const topic = 'demo_java';

async function main() {
  logger.info('I am a Kafka Consumer!');
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  consumer.on(consumer.events.REBALANCING, (e) => {
    logger.info({ event: 'REBALANCING', ...e.payload }, 'onPartitionsRevoked-like callback triggered');
  });
  consumer.on(consumer.events.REBALANCED, (e) => {
    logger.info({ event: 'REBALANCED', ...e.payload }, 'onPartitionsAssigned-like callback triggered');
  });

  const shutdown = async () => {
    logger.info("Detected a shutdown, let's exit by stopping consumer...");
    try {
      await consumer.stop();
    } finally {
      await consumer.disconnect();
      logger.info('The consumer is now gracefully shut down');
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = message.value?.toString();
      logger.info(`Key: ${key}, Value: ${value}`);
      logger.info(`Partition: ${partition}, Offset: ${message.offset}`);
    },
  });
}

main().catch((err) => {
  logger.error({ err }, 'Consumer error');
  process.exitCode = 1;
}); 