import kafka from '../../config/kafka.js';
import logger from '../../config/logger.js';

const topic = 'demo_java';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  logger.info('I am a Kafka Producer!');
  const producer = kafka.producer();
  await producer.connect();
  try {
    for (let j = 0; j < 10; j++) {
      for (let i = 0; i < 30; i++) {
        const value = `hello world ${i}`;
        const responses = await producer.send({ topic, messages: [{ value }] });
        for (const res of responses) {
          // for (const p of res.partitions) {
            logger.info(
              {
                topic: res.topicName,
                partition: res.partition,
                baseOffset: res.baseOffset,
                logAppendTime: res.logAppendTime,
              },
              'Received new metadata'
            );
          // }
        }
      }
      await sleep(500);
    }
  } finally {
    await producer.disconnect();
  }
}

main().catch((err) => {
  logger.error({ err }, 'Producer error');
  process.exitCode = 1;
}); 