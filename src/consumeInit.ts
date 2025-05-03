import { EventConsumer } from "./consumer";

const consumer = async () => {
  const consumer = await new EventConsumer().messageHandler(
    "resources-avability",
    (message) => {
      console.log("Message received:", {
        key: message.key,
        value: message.value,
        headers: message.headers,
      });
    }
  );
};

consumer()
  .then(() => {
    console.log("Message consumed successfully");
  })
  .catch((error) => {
    console.error("Error consuming message: ", error);
  });
