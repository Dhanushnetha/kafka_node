import kafka from '../../config/kafka.js';
import logger from '../../config/logger.js';

const topic = 'demo_java';

async function main() {
  logger.info('I am a Kafka Producer!');
  const producer = kafka.producer();
  await producer.connect();
  try {
    for (let j = 0; j < 2; j++) {
      for (let i = 0; i < 10; i++) {
        const key = `id_${i}`;
        const value = `hello world ${i}`;
        const responses = await producer.send({ topic, messages: [{ key, value }] });
        for (const res of responses) {
          for (const p of res.partitions) {
            logger.info(`Key: ${key} | Partition: ${p.partition}`);
          }
        }
      }
    }
  } finally {
    await producer.disconnect();
  }
}

main().catch((err) => {
  logger.error({ err }, 'Producer error');
  process.exitCode = 1;
}); 