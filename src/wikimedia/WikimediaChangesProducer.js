import dotenv from 'dotenv';
import { createParser } from 'eventsource-parser';
import kafka from '../config/kafka.js';
import logger from '../config/logger.js';
import WikimediaChangeHandler from './WikimediaChangeHandler.js';

dotenv.config();

const topic = process.env.TOPIC || 'wikimedia.recentchange';
const streamUrl = process.env.WIKIMEDIA_STREAM_URL || 'https://stream.wikimedia.org/v2/stream/recentchange';
const runMinutes = Number(process.env.RUN_DURATION_MINUTES || '10');

async function streamSSE(url, onEvent) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch SSE: ${res.status} ${res.statusText}`);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const parser = createParser((event) => {
    if (event.type === 'event') onEvent(event.data);
  });

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    parser.feed(decoder.decode(value, { stream: true }));
  }
}

async function main() {
  const producer = kafka.producer();
  await producer.connect();
  const handler = new WikimediaChangeHandler(producer, topic);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), runMinutes * 60 * 1000);

  try {
    await streamSSE(streamUrl, (data) => handler.onMessage(data));
  } catch (e) {
    if (controller.signal.aborted) {
      logger.info('Run duration elapsed, stopping');
    } else {
      logger.error({ err: e }, 'Error in Stream Reading');
    }
  } finally {
    clearTimeout(timeout);
    await producer.disconnect();
  }
}

main().catch((err) => {
  logger.error({ err }, 'Producer failed');
  process.exitCode = 1;
}); 