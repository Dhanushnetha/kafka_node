import { Client } from '@opensearch-project/opensearch';
import dotenv from 'dotenv';

dotenv.config();

export function createOpenSearchClient() {
  const node = process.env.OPENSEARCH_URL || 'http://172.27.188.7:9200';
  const url = new URL(node);
  const auth = url.username && url.password ? { username: url.username, password: url.password } : undefined;

  return new Client({
    node: `${url.protocol}//${url.host}`,
    auth,
    ssl: url.protocol === 'https:' ? {} : undefined,
  });
} 