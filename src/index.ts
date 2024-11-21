import { EventEmitter } from "events";
import PubSubApiClient from "./client";
import readline from 'readline';
import fs from 'fs';
import path from 'path';

const eventType = "/event/TCSNALA_Workorder_XOi__e"
async function run() {
  const logFilePath = path.join(__dirname, '..', 'output-logs.json');

  if (!fs.existsSync(logFilePath)) {
    fs.writeFileSync(logFilePath, '[]', 'utf8');
    console.log('Created file for logging outputs: output-logs.txt');
  } else {
    console.log('Logs will be written to existing file: output-logs.txt');
  }

  function appendEventToFile(logFilePath: string, event: any, logType: string) {
    let logs: any[] = [];
    if (fs.existsSync(logFilePath)) {
      const fileContents = fs.readFileSync(logFilePath, 'utf8');
      logs = JSON.parse(fileContents || '[]');
    }
    // Add Log_Meta key
    const createdDate = event.payload.CreatedDate;
    const eventTime = new Date(createdDate).toISOString();
    const loggedAt = new Date().toISOString();
    const updatedEvent = {
      Log_Meta: {
        Event_Actual_Time: eventTime,
        Event_Logged_At: loggedAt,
        Subscriber_Type: logType,
      },
      ...event,
    };
    logs.push(updatedEvent);
    fs.writeFileSync(logFilePath, JSON.stringify(logs, null, 2), 'utf8');
  }

  function handleEvent(event: any, logFilePath: string, logType: string) {
    console.log(
      JSON.stringify(
        event,
        (key, value) => (typeof value === "bigint" ? value.toString() : value),
        2
      )
    );
    appendEventToFile(logFilePath, event, logType);
  }

  try {
    const client = new PubSubApiClient();
    await client.connect();
    let eventEmitter: EventEmitter;

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });

    console.log("\n\nPlease select an option:");
    console.log("1. Open stream and subscribe to events starting now");
    console.log("2. Open stream from a specific replayId");
    console.log("3. Only pull in 1st event after a specific replayId - stream will close after 1 event");

    rl.question('Enter your choice (1, 2, or 3): ', async (answer: string) => {
      switch (answer) {
        case '1':
          console.log("You selected option 1: Open stream and subscribe to events starting now");
          eventEmitter = await client.subscribe(eventType);
          eventEmitter.on("data", (event) => handleEvent(event, logFilePath, answer));
          break;
        case '2':
          rl.question('Enter the replayId to start from: ', async (replayId: string) => {
            console.log(`You selected option 2: Open stream from replayId ${replayId}`);
            eventEmitter = await client.subscribeFromReplayId(eventType, null, parseInt(replayId)); // set to null to subscribe to all events
            eventEmitter.on("data", (event) => handleEvent(event, logFilePath, answer));
            rl.close();
          });
          break;
        case '3':
          rl.question('Enter the replayId from which to pull 1 event: ', async (replayId: string) => {
            console.log(`You selected option 3: Only pull in events from replayId ${replayId}`);
            eventEmitter = await client.subscribeFromReplayId(eventType, 1, parseInt(replayId));
            eventEmitter.on("data", (event) => {
              handleEvent(event, logFilePath, answer);
              client.disconnect();
            });
            rl.close();
          });
          break;
        default:
          console.log("Invalid selection. Please enter 1, 2, or 3.");
          rl.close();
      }
    });
  } catch (error) {
    console.error(error);
  }
}

run();