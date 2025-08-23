import dotenv from 'dotenv';
import kafka from '../../../config/kafka.js';
dotenv.config();

const {
  TOPIC = 'node_demo'
} = process.env;

async function createTopic() {
  const admin = kafka.admin();
  await admin.connect();

  // Create one topic with 3 partitions, RF=1 (single-broker)
  const res = await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: TOPIC,
        numPartitions: 3,y
        replicationFactor: 1,
        configEntries: [
          // Examples:
          // { name: 'cleanup.policy', value: 'delete' },
          // { name: 'retention.ms', value: '604800000' } // 7 days
        ]
      }
    ]
  });

  console.log(`createTopics result:`, res ? 'created or already existed' : 'no-op');
  await admin.disconnect();
}

async function describeTopic() {
  const admin = kafka.admin();
  await admin.connect();
  const md = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  console.log(JSON.stringify(md, null, 2));
  await admin.disconnect();
}

const cmd = process.argv[2] || '';
if (cmd === 'create') {
  createTopic().catch(console.error);
} else if (cmd === 'describe') {
  describeTopic().catch(console.error);
} else {
  console.log(`Usage:
  npm run admin:create   # create topic
  npm run admin:describe # describe topic
`);
}
