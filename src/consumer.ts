import { Consumer, RetryOptions } from "kafkajs";
import { kafka } from "./client";
import { IEventMessage, ITopicMessages } from "./types";

const retry = async <T>(
  operation: () => Promise<T>,
  retries = 5,
  delay = 1000,
  factor = 2
): Promise<T> => {
  try {
    return await operation();
  } catch (error) {
    if (retries <= 0) throw error;
    console.log(`Retrying operation after ${delay}ms...`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    return retry(operation, retries - 1, delay * factor, factor);
  }
};

export class EventConsumer {
  private consumer: Consumer;
  private connected = false;
  private subscribedTopics: Set<string> = new Set();

  constructor(groupId: string = "test-1") {
    const retryOptions: RetryOptions = {
      initialRetryTime: 300,
      retries: 10,
      maxRetryTime: 30000,
      factor: 0.2,
    };

    this.consumer = kafka.consumer({
      groupId,
      retry: retryOptions,
    });
  }

  private async connect(): Promise<void> {
    if (!this.connected) {
      try {
        console.log("Connecting Kafka consumer...");
        await retry(async () => await this.consumer.connect());
        this.connected = true;
        console.log("Kafka consumer connected successfully");
      } catch (error) {
        console.error("Error connecting consumer: ", error);
        throw error;
      }
    }
  }

  private async subscribe(topic: string): Promise<void> {
    if (!this.subscribedTopics.has(topic)) {
      if (!this.connected) {
        await this.connect();
      }
      try {
        console.log(`Subscribing to topic: ${topic}`);
        await retry(async () => await this.consumer.subscribe({ topic }));
        this.subscribedTopics.add(topic);
        console.log(`Successfully subscribed to topic: ${topic}`);
      } catch (error) {
        console.error(`Error subscribing to topic ${topic}: `, error);
        throw error;
      }
    }
  }

  public async messageHandler(
    topic: string = "resources-avability",
    onMessage: (message: IEventMessage) => void | Promise<void>
  ): Promise<void> {
    try {
      if (!this.connected) {
        await this.connect();
      }

      await this.subscribe(topic);

      console.log(`Starting consumer for topic: ${topic}`);
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
          try {
            // const convertedHeaders: Record<string, string> = {};
            // if (message.headers) {
            //   Object.entries(message.headers).forEach(([key, value]) => {
            //     convertedHeaders[key] = value?.toString() || "";
            //   });
            // }

            // const msg: IEventMessage = {
            //   key: message.key?.toString() || "",
            //   value: message.value?.toString() || "",
            //   headers: convertedHeaders,
            // };
            // console.log(`Received message on topic ${topic}:`, msg);
            console.log(
              `Topic: ${topic}, Partition: ${partition}, message: ${JSON.stringify(
                message.value?.toString() || ""
              )} `
            );
            // await Promise.resolve(onMessage(msg));
            await heartbeat();
          } catch (error) {
            console.error(`Error processing message: ${error}`);
          }
        },
      });
    } catch (error) {
      console.error(`Error in messageHandler for topic ${topic}: `, error);
      throw error;
    }
  }

  public async receiveBatch(
    topic: string,
    onMessage: (message: ITopicMessages) => void | Promise<void>
  ): Promise<void> {
    try {
      if (!this.connected) {
        await this.connect();
      }

      await this.subscribe(topic);

      console.log(`Starting batch consumer for topic: ${topic}`);
      await this.consumer.run({
        eachBatch: async ({
          batch,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
        }) => {
          try {
            console.log(
              `Received batch of ${batch.messages.length} messages from topic ${batch.topic}`
            );

            const messages: ITopicMessages = {
              topic: batch.topic,
              messages: batch.messages.map((message) => {
                const convertedHeaders: Record<string, string> = {};
                if (message.headers) {
                  Object.entries(message.headers).forEach(([key, value]) => {
                    convertedHeaders[key] = value?.toString() || "";
                  });
                }
                return {
                  key: message.key?.toString() || "",
                  value: message.value?.toString() || "",
                  partition: batch.partition,
                  headers: convertedHeaders,
                };
              }),
            };

            await Promise.resolve(onMessage(messages));

            batch.messages.forEach((message) => {
              resolveOffset(message.offset);
            });

            await commitOffsetsIfNecessary();
            await heartbeat();
          } catch (error) {
            console.error(`Error processing batch: ${error}`);
          }
        },
      });
    } catch (error) {
      console.error(`Error in receiveBatch for topic ${topic}: `, error);
      throw error;
    }
  }

  public async disconnect(): Promise<void> {
    if (this.connected) {
      try {
        console.log("Disconnecting Kafka consumer...");
        await this.consumer.disconnect();
        this.connected = false;
        this.subscribedTopics.clear();
        console.log("Kafka consumer disconnected successfully");
      } catch (error) {
        console.error("Error disconnecting consumer: ", error);
        throw error;
      }
    }
  }
}
