import PubSubApiClient from "./client";

async function run() {
  try {
    const client = new PubSubApiClient();
    await client.connect();

    // Subscribe to account change events
    const eventEmitter = await client.subscribe(
      "/event/TCSNALA_Workorder_XOi__e"
    );

    // Handle incoming events
    eventEmitter.on("data", (event) => {
      // Safely log event as a JSON string
      console.log(
        JSON.stringify(
          event,
          (key, value) =>
            /* Convert BigInt values into strings and keep other types unchanged */
            typeof value === "bigint" ? value.toString() : value,
          2
        )
      );
    });
  } catch (error) {
    console.error(error);
  }
}

run();
