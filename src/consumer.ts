import { Consumer } from "kafkajs";
import { kafka } from "./cliennt";
import { IEventMessage } from "./types";

export class EventConsumer {
  private consumer: Consumer;
  private connected = false;
  private isSubscribed = false;

  constructor() {
    this.consumer = kafka.consumer({
      groupId: "test-1",
      allowAutoTopicCreation: false,
    });
  }

  private async connect(): Promise<void> {
    if (!this.connected) {
      try {
        await this.consumer.connect();
        this.connected = true;
      } catch (error) {
        console.error("Error connecting consumer: ", error);
        throw error;
      }
    }
  }

  private async subscribe(topic: string) {
    if (!this.isSubscribed) {
      if (!this.connected) {
        await this.connect();
      }
      try {
        await this.consumer.subscribe({
          topic: topic,
        });
      } catch (error) {
        console.error("Error subscribing to topic: ", error);
        throw error;
      }
    }
  }

  public async messageHandler(
    topic: string = "resources-avability",
    onMessage: (message: IEventMessage) => void | Promise<void>
  ) {
    if (!this.connected) {
      await this.connect();
    }
    if (!this.isSubscribed) {
      await this.subscribe(topic);
    }
    try {
      await this.consumer.run({
        eachMessage: async ({
          topic,
          partition,
          message,
          heartbeat,
          pause,
        }) => {
          const convertedHeaders: Record<string, string> = {};
          if (message.headers) {
            Object.entries(message.headers).forEach(([key, value]) => {
              convertedHeaders[key] = value?.toString() || "";
            });
          }
          const msg: IEventMessage = {
            key: message.key?.toString() || "",
            value: message.value?.toString() || "",
            headers: convertedHeaders,
          };
          await Promise.resolve(onMessage(msg));
        },
      });
    } catch (error) {
      console.error("Error running consumer: ", error);
      throw error;
    }
  }
}
