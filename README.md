# kafka-basics-node

Node.js equivalents of the `kafka-basics` Java demos using KafkaJS.

## Setup

1. Copy `.env.example` to `.env` and fill in your Kafka credentials.
2. Install deps:
   npm install

## Scripts

- Creating Topics and Describe
  - npm run admin:create
  - npm run admin:describe

- Producers:
  - npm run producer
  - npm run producer:basic
  - npm run producer:callback
  - npm run producer:keys

- Consumers:
  - npm run consumer
  - npm run consumer:basic
  - npm run consumer:shutdown
  - npm run consumer:cooperative

- Advanced:
  - npm run advanced:assign-seek
  - npm run advanced:rebalance-listener
  - npm run advanced:threads

Topics default to `node_demo` to mirror the Java demos. 