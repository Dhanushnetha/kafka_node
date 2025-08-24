import logger from '../config/logger.js';

export default class WikimediaChangeHandler {
  constructor(producer, topic) {
    this.producer = producer;
    this.topic = topic;
  }

  async onMessage(data) {
    logger.info(data);
    await this.producer.send({ topic: this.topic, messages: [{ value: data }] });
  }
} 