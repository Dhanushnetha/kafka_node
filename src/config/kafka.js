import { Kafka, logLevel } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || 'cluster.playground.cdkt.io:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const useSsl = process.env.KAFKA_SSL ? process.env.KAFKA_SSL === 'true' : true;

const sasl = process.env.KAFKA_SASL_USERNAME && process.env.KAFKA_SASL_PASSWORD
  ? {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    }
  : undefined;

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'kafka-basics-node',
  brokers,
  // ssl: useSsl,
  // sasl,
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

export default kafka; 