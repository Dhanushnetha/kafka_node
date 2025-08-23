import kafka from '../../../config/kafka.js';
import logger from '../../../config/logger.js';

const topic = 'demo_java';

class ConsumerWorker {
  constructor(groupId) {
    this.groupId = groupId;
    this.consumer = kafka.consumer({ groupId });
  }

  async start() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic, fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key?.toString();
        const value = message.value?.toString();
        logger.info(
          `Getting consumer record key: '${key}', value: '${value}', partition: ${partition} and offset: ${message.offset}`
        );
      },
    });
  }

  async shutdown() {
    await this.consumer.stop();
    await this.consumer.disconnect();
    logger.info('Consumer closed');
  }
}

async function main() {
  const worker = new ConsumerWorker('my-sixth-application');
  await worker.start();

  const closer = async () => {
    try {
      await worker.shutdown();
    } catch (e) {
      logger.error({ err: e }, 'Error shutting down consumer');
    }
  };

  process.on('SIGINT', closer);
  process.on('SIGTERM', closer);
}

main().catch((err) => {
  logger.error({ err }, 'Consumer error');
  process.exitCode = 1;
}); 