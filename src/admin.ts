import { kafka } from "./client";

/*
This is a admin file to create a topic in kafka.
This file is used to create a topic.
execute this file while creating a topic.
run this file using the command below:
ts-node src/admin.ts
*/

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");
  admin.connect();
  console.log("Adming Connection Success...");
  console.log("Creating Topic [resources-avability]");
  try {
    await admin.createTopics({
      topics: [
        {
          topic: "resources",
          numPartitions: 2,
        },
      ],
    });
    console.log("Topic Created Success [resources-avability]");

    console.log("Disconnecting Admin..");
    await admin.disconnect();
  } catch (error) {
    console.log(error);
  }
}

init();
