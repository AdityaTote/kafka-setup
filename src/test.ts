import { EventConsumer } from "./consumer";
import { EventProducer } from "./producer";
import { IEventMessage } from "./types";

async function produceMessage() {
  const message: IEventMessage[] = [
    {
      key: "key1",
      value: "value1",
      partition: 0,
      headers: {
        header1: "headerValue1",
      },
    },
    {
      key: "key2",
      value: "value2",
      partition: 0,
      headers: {
        header2: "headerValue2",
      },
    },
    {
      key: "key3",
      value: "value3",
      partition: 0,
      headers: {
        header3: "headerValue3",
      },
    },
    {
      key: "key4",
      value: "value4",
      partition: 0,
      headers: {
        header4: "headerValue4",
      },
    },
    {
      key: "key5",
      value: "value5",
      partition: 0,
      headers: {
        header5: "headerValue5",
      },
    },
  ];

  const producer = await new EventProducer().sendMessage(
    "resources-avability",
    message
  );

  console.log("Message sent successfully");
}

produceMessage()
  .then(() => {
    console.log("Message produced successfully");
  })
  .catch((error) => {
    console.error("Error producing message: ", error);
  });
