import kafka from '../../../config/kafka.js';
import logger from '../../../config/logger.js';

const topic = 'demo_java';
const partition = 0;
const offsetToReadFrom = '7';
const numberOfMessagesToRead = 5;

async function main() {
  const consumer = kafka.consumer({ groupId: undefined });
  await consumer.connect();
  // Use assign to a specific partition (no subscribe)
  await consumer.assign([{ topic, partition }]);
  await consumer.seek({ topic, partition, offset: offsetToReadFrom });

  let numberOfMessagesReadSoFar = 0;
  let keepOnReading = true;

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!keepOnReading) return;
      numberOfMessagesReadSoFar += 1;
      const key = message.key?.toString();
      const value = message.value?.toString();
      logger.info(`Key: ${key}, Value: ${value}`);
      logger.info(`Partition: ${partition}, Offset: ${message.offset}`);
      if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
        keepOnReading = false;
        await consumer.stop();
      }
    },
  });

  logger.info('Exiting the application');
  await consumer.disconnect();
}

main().catch((err) => {
  logger.error({ err }, 'Assign/Seek consumer error');
  process.exitCode = 1;
}); 