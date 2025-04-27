import express from "express";
import { EventProducer } from "./producer";
import { EventConsumer } from "./consumer";
import { IEventMessage, ITopicMessages } from "./types";
import { PORT } from "./constants";

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.send("Hello World!");
});

app.post("/produce/messgae", async (req, res) => {
  const input: IEventMessage[] = req.body;
  /*
  payload: {
  key: string;
  value: string;
  partition?: number;
  headers?: Record<string, string>
  }
*/
  try {
    await new EventProducer().sendMessage("resources-avability", input);
    res.status(200).json({
      message: "Message sent successfully",
    });
    return;
  } catch (error) {
    console.error("Error producing message: ", error);
    res.status(500).json({
      message: "Error producing message",
      error: error,
    });
    return;
  }
});

app.post("/produce/batch", async (req, res) => {
  const input: ITopicMessages[] = req.body;
  /*
  payload: {
  topic: string;
  messages: [{
    key: string;
    value: string;
    partition?: number;
    headers?: Record<string, string>
  }]
  }
*/
  try {
    await new EventProducer().sendBatch(input);
    res.status(200).json({
      message: "Batch message sent successfully",
    });
    return;
  } catch (error) {
    console.error("Error producing batch message: ", error);
    res.status(500).json({
      message: "Error producing batch message",
      error: error,
    });
    return;
  }
});

app.get("/consume/messages", async (req, res) => {
  const messages: IEventMessage[] = [];
  try {
    await new EventConsumer().messageHandler(
      "resources-avability",
      (message) => {
        messages.push(message);
        console.log("Message received:", {
          key: message.key,
          value: message.value,
          headers: message.headers,
        });
      }
    );
    res.status(200).json({
      message: "Message consumed successfully",
      data: messages,
    });
    return;
  } catch (error) {
    console.error("Error consuming message: ", error);
    res.status(500).json({
      message: "Error consuming message",
      error: error,
    });
    return;
  }
});

app.get("/consume/batch", async (req, res) => {
  const messages: ITopicMessages[] = [];
  try {
    await new EventConsumer().reciveBatch("resources-avability", (message) => {
      messages.push(message);
      console.log("Batch message received:", {
        topic: message.topic,
        messages: message.messages,
      });
    });
    res.status(200).json({
      message: "Batch message consumed successfully",
      data: messages,
    });
    return;
  } catch (error) {
    console.error("Error consuming batch message: ", error);
    res.status(500).json({
      message: "Error consuming batch message",
      error: error,
    });
    return;
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
