export async function ensureIndex(client, indexName, logger) {
  const exists = await client.indices.exists({ index: indexName });
  if (!exists.body) {
    await client.indices.create({ index: indexName });
    logger.info('The Wikimedia Index has been created!');
  } else {
    logger.info('The Wikimedia Index already exits');
  }
} 