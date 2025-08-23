import kafka from '../../config/kafka.js';
import logger from '../../config/logger.js';

const groupId = 'my-java-application';
const topic = 'demo_java';

async function main() {
  logger.info('I am a Kafka Consumer!');
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

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