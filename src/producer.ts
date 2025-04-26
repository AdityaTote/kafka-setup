import { Producer } from "kafkajs";
import { kafka } from "./cliennt";
import { IEventMessage, ITopicMessages } from "./types";

export class EventProducer {
  private producer: Producer;
  private connected = false;

  constructor() {
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
      transactionTimeout: 30000,
    });
  }

  private async connect(): Promise<void> {
    if (!this.connected) {
      try {
        await this.producer.connect();
        this.connected = true;
      } catch (error) {
        console.error("Error connecting producer: ", error);
        throw error;
      }
    }
  }

  private async disconnect(): Promise<void> {
    if (this.connected) {
      try {
        await this.producer.disconnect();
        this.connected = false;
      } catch (error) {
        console.error("Error disconnecting producer: ", error);
        throw error;
      }
    }
  }

  public async sendMessage(
    topic: string,
    messages: IEventMessage[]
  ): Promise<void> {
    try {
      if (!this.connected) {
        await this.connect();
      }
      await this.producer.send({
        topic,
        messages,
      });
      await this.disconnect();
    } catch (error) {
      console.error("Error sending message: ", error);
      throw error;
    }
  }

  public async sendBatch(topicMessages: ITopicMessages[]): Promise<void> {
    try {
      if (!this.connected) {
        await this.connect();
      }
      await this.producer.sendBatch({
        topicMessages,
      });
    } catch (error) {
      console.error("Error sending batch message: ", error);
      throw error;
    }
  }
}
