import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "admin-client",
  brokers: ["localhost:9092"],
});
