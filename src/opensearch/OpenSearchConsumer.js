import dotenv from 'dotenv';
import kafka from '../config/kafka.js';
import logger from '../config/logger.js';
import { createOpenSearchClient } from './client.js';
import { ensureIndex } from './indexHelpers.js';

dotenv.config();

const groupId = process.env.KAFKA_GROUP_ID || 'consumer-opensearch-demo';
const topic = process.env.KAFKA_TOPIC || 'wikimedia.recentchange';
const indexName = process.env.OPENSEARCH_INDEX || 'wikimedia';

async function main() {
  const openSearch = createOpenSearchClient();
  const consumer = kafka.consumer({ groupId });

  // graceful shutdown
  const shutdown = async () => {
    logger.info("Detected a shutdown, stopping consumer and closing OpenSearch...");
    try {
      await consumer.stop();
    } finally {
      await consumer.disconnect();
      await openSearch.close();
      logger.info('The consumer is now gracefully shut down');
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  try {
    await ensureIndex(openSearch, indexName, logger);

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary }) => {
        const bulkBody = [];

        for (const message of batch.messages) {
          const value = message.value?.toString();
          if (!value) continue;

          // Strategy 2: Extract ID from JSON value: meta.id
          let id;
          try {
            id = JSON.parse(value)?.meta?.id;
          } catch {}

          const indexAction = { index: { _index: indexName } };
          if (id) indexAction.index._id = id;

          bulkBody.push(indexAction);
          bulkBody.push(value);

          resolveOffset(message.offset);
          await heartbeat();
        }

        if (bulkBody.length > 0) {
          const res = await openSearch.bulk({ body: bulkBody, refresh: false });
          const items = res.body?.items || [];
          logger.info(`Inserted ${items.length} record(s).`);

          await new Promise((r) => setTimeout(r, 1000));
          await commitOffsetsIfNecessary();
          logger.info('Offsets have been committed!');
        }
      },
    });
  } catch (err) {
    logger.error({ err }, 'Unexpected exception in the consumer');
    await shutdown();
  }
}

main().catch((err) => {
  logger.error({ err }, 'Consumer failed');
  process.exitCode = 1;
}); 