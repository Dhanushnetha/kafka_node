import dotenv from 'dotenv';
import kafka from '../../config/kafka.js';
dotenv.config();

const {
  TOPIC = 'orders'
} = process.env;

async function run() {
  const producer = kafka.producer({
    // enable idempotence-like behavior with acks -1
    // KafkaJS uses acks config on send; idempotent producers proper are Java client-only,
    // but we can get strong durability with acks=-1.
    // allowAutoTopicCreation: false
  });

  await producer.connect();

  // Example payloads:
  // 1) Keyed messages (stick to a partition by key hashing)
  const keyed = [
    { key: 'user-1', value: JSON.stringify({ orderId: 101, amount: 499 }) },
    { key: 'user-1', value: JSON.stringify({ orderId: 102, amount: 299 }) },
    { key: 'user-2', value: JSON.stringify({ orderId: 201, amount: 999 }) },
    { key: 'user-4', value: JSON.stringify({ orderId: 201, amount: 444 }) },
    { key: 'user-5', value: JSON.stringify({ orderId: 201, amount: 55555 }) },
    { key: 'user-5', value: JSON.stringify({ orderId: 201, amount: 55555 }) },
    { key: 'user-5', value: JSON.stringify({ orderId: 201, amount: 55555 }) },
    { key: 'user-5', value: JSON.stringify({ orderId: 201, amount: 55555 }) },
  ];

  // 2) Direct partition targeting (overrides key hashing)
  // Send three messages to partition 2:
  const directPartition = Array.from({ length: 3 }, (_, i) => ({
    value: JSON.stringify({ note: `pinned to partition 2 #${i + 1}` }),
    partition: 2
  }));

  console.log('Sending keyed messages…');
  await producer.send({
    topic: TOPIC,
    acks: -1,
    messages: keyed
  });

  console.log('Sending direct-partition messages…');
  await producer.send({
    topic: TOPIC,
    acks: -1,
    messages: directPartition
  });

  console.log('Done producing. Press Ctrl+C to exit.');
  // keep process alive momentarily in case you want to observe
  setTimeout(async () => {
    await producer.disconnect();
    process.exit(0);
  }, 500);
}

run().catch(async (err) => {
  console.error('Producer error:', err);
  process.exit(1);
});


// import dotenv from 'dotenv';
// import { kafka } from './utils/kafka.js';
// dotenv.config();

// const {
//   TOPIC = 'orders'
// } = process.env;

// async function run() {
//   const producer = kafka.producer({
//     allowAutoTopicCreation: false
//   });

//   await producer.connect();

//   // Messages without key or partition → round-robin partitioner
//   const roundRobinMessages = Array.from({ length: 10 }, (_, i) => ({
//     value: JSON.stringify({ orderId: i + 1, amount: Math.floor(Math.random() * 1000) })
//   }));

//   console.log('Sending messages round-robin across partitions…');
//   await producer.send({
//     topic: TOPIC,
//     acks: -1,
//     messages: roundRobinMessages
//   });

//   console.log('Done producing. Press Ctrl+C to exit.');
//   setTimeout(async () => {
//     await producer.disconnect();
//     process.exit(0);
//   }, 500);
// }

// run().catch(async (err) => {
//   console.error('Producer error:', err);
//   process.exit(1);
// });




// kafka-console-producer.sh --bootstrap-server localhost:9092 --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner --topic orders