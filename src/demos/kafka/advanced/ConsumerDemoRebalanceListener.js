import kafka from '../../../config/kafka.js';
import logger from '../../../config/logger.js';

const groupId = 'my-fifth-application';
const topic = 'demo_java';

async function main() {
  logger.info('I am a Kafka Consumer with a Rebalance');
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();

  consumer.on(consumer.events.REBALANCING, (e) => {
    logger.info({ event: 'REBALANCING', ...e.payload }, 'onPartitionsRevoked callback triggered');
  });
  consumer.on(consumer.events.REBALANCED, (e) => {
    logger.info({ event: 'REBALANCED', ...e.payload }, 'onPartitionsAssigned callback triggered');
  });

  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: true,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
      for (const message of batch.messages) {
        const key = message.key?.toString();
        const value = message.value?.toString();
        logger.info(`Key: ${key}, Value: ${value}`);
        logger.info(`Partition: ${batch.partition}, Offset: ${message.offset}`);
        resolveOffset(message.offset);
        await heartbeat();
      }
      await commitOffsetsIfNecessary();
    },
  });
}

main().catch((err) => {
  logger.error({ err }, 'Consumer error');
  process.exitCode = 1;
}); 