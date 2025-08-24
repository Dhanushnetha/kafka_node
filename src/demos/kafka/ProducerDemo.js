import kafka from '../../config/kafka.js';
import logger from '../../config/logger.js';

const topic = 'node_demo';

async function main() {
  logger.info('I am a Kafka Producer!');
  const producer = kafka.producer();
  await producer.connect();
  try {
    await producer.send({ topic, messages: [{ value: 'hello world' }] });
  } finally {
    await producer.disconnect();
  }
}

main().catch((err) => {
  logger.error({ err }, 'Producer error');
  process.exitCode = 1;
}); 