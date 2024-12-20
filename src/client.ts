import fs from "fs";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
// @ts-ignore
import avro from "avro-js";
// @ts-ignore
import certifi from "certifi";
// eslint-disable-next-line no-unused-vars
import { connectivityState } from "@grpc/grpc-js";

import SchemaCache from "./utils/schemaCache";
import EventParseError from "./utils/eventParseError";
import PubSubEventEmitter from "./utils/pubSubEventEmitter";
import { CustomLongAvroType } from "./utils/avroHelper";
import Configuration from "./utils/configuration";
import {
  PubSubClient, FetchRequest
} from './pubsub_api'
import {
  parseEvent,
  encodeReplayId,
  decodeReplayId,
} from "./utils/eventParser";
import SalesforceAuth from "./utils/auth";
import path from "path";


const PROTO_PATH = path.resolve(__dirname, "./pubsub_api.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;

/**
 * @typedef {Object} PublishResult
 * @property {number} replayId
 * @property {string} correlationKey
 * @global
 */

/**
 * @typedef {Object} Logger
 * @property {Function} debug
 * @property {Function} info
 * @property {Function} error
 * @property {Function} warn
 * @protected
 */

/**
 * Maximum event batch size suppported by the Pub/Sub API as documented here:
 * https://developer.salesforce.com/docs/platform/pub-sub-api/guide/flow-control.html
 */
const MAX_EVENT_BATCH_SIZE = 100;

/**
 * Client for the Salesforce Pub/Sub API
 * @alias PubSubApiClient
 * @global
 */
export default class PubSubApiClient {
  /**
   * gRPC client
   * @type {Object}
   */
  // @ts-ignore
  #client;

  /**
   * Schema cache
   * @type {SchemaCache}
   */
  #schemaChache;

  /**
   * Map of subscribitions indexed by topic name
   * @type {Map<string,Object>}
   */
  #subscriptions;

  #logger;

  /**
   * Builds a new Pub/Sub API client
   * @param {Logger} [logger] an optional custom logger. The client uses the console if no value is supplied.
   */
  constructor(logger = console) {
    this.#logger = logger;
    this.#schemaChache = new SchemaCache();
    this.#subscriptions = new Map();
    // Check and load config
    try {
      Configuration.load();
    } catch (error) {
      this.#logger.error(error);
      // @ts-ignore
      throw new Error("Failed to initialize Pub/Sub API client", {
        cause: error,
      });
    }
  }

  /**
   * Authenticates with Salesforce then, connects to the Pub/Sub API.
   * @returns {Promise<void>} Promise that resolves once the connection is established
   * @memberof PubSubApiClient.prototype
   */
  async connect() {
    if (Configuration.isUserSuppliedAuth()) {
      throw new Error(
        'You selected user-supplied authentication mode so you cannot use the "connect()" method. Use "connectWithAuth(...)" instead.'
      );
    }

    // Connect to Salesforce to obtain an access token
    let conMetadata;
    try {
      conMetadata = await SalesforceAuth.authenticate();
      this.#logger.info(
        `Connected to Salesforce org ${conMetadata.instanceUrl} as ${conMetadata.username}`
      );
    } catch (error) {
      // @ts-ignore
      throw new Error("Failed to authenticate with Salesforce", {
        cause: error,
      });
    }
    return this.#connectToPubSubApi(conMetadata);
  }

  /**
   * Connects to the Pub/Sub API with user-supplied authentication.
   * @param {string} accessToken Salesforce access token
   * @param {string} instanceUrl Salesforce instance URL
   * @param {string} [organizationId] optional organization ID. If you don't provide one, we'll attempt to parse it from the accessToken.
   * @returns {Promise<void>} Promise that resolves once the connection is established
   * @memberof PubSubApiClient.prototype
   */
  // @ts-ignore
  async connectWithAuth(accessToken, instanceUrl, organizationId) {
    if (!instanceUrl || !instanceUrl.startsWith("https://")) {
      throw new Error(
        `Invalid Salesforce Instance URL format supplied: ${instanceUrl}`
      );
    }
    let validOrganizationId = organizationId;
    if (!organizationId) {
      try {
        validOrganizationId = accessToken.split("!").at(0);
      } catch (error) {
        throw new Error(
          "Unable to parse organizationId from given access token",
          // @ts-ignore
          {
            cause: error,
          }
        );
      }
    }
    if (
      validOrganizationId.length !== 15 &&
      validOrganizationId.length !== 18
    ) {
      throw new Error(
        `Invalid Salesforce Org ID format supplied: ${validOrganizationId}`
      );
    }
    return this.#connectToPubSubApi({
      accessToken,
      instanceUrl,
      organizationId: validOrganizationId,
    });
  }

  /**
   * Connects to the Pub/Sub API.
   * @param {import('./auth.js').ConnectionMetadata} conMetadata
   * @returns {Promise<void>} Promise that resolves once the connection is established
   */
  // @ts-ignore
  async #connectToPubSubApi(conMetadata) {
    // Connect to Pub/Sub API
    try {
      // Read certificates
      const rootCert = fs.readFileSync(certifi);

      // Load proto definition

      // @ts-ignore
      const sfdcPackage = protoDescriptor.eventbus.v1;

      // Prepare gRPC connection
      // @ts-ignore
      const metaCallback = (_params, callback) => {
        const meta = new grpc.Metadata();
        meta.add("accesstoken", conMetadata.accessToken);
        meta.add("instanceurl", conMetadata.instanceUrl);
        meta.add("tenantid", conMetadata.organizationId);
        callback(null, meta);
      };
      const callCreds =
        grpc.credentials.createFromMetadataGenerator(metaCallback);
      const combCreds = grpc.credentials.combineChannelCredentials(
        grpc.credentials.createSsl(rootCert),
        callCreds
      );

      // Return pub/sub gRPC client
      // this.#client = new sfdcPackage.PubSub(
      //   Configuration.getPubSubEndpoint(),
      //   combCreds
      // );
      // @ts-ignore
      this.#client = new PubSubClient(Configuration.getPubSubEndpoint(), combCreds)
      this.#logger.info(
        `Connected to Pub/Sub API endpoint ${Configuration.getPubSubEndpoint()}`
      );
    } catch (error) {
      // @ts-ignore
      throw new Error("Failed to connect to Pub/Sub API", {
        cause: error,
      });
    }
  }

  /**
   * Get connectivity state from current channel.
   * @returns {Promise<connectivityState>} Promise that holds channel's connectivity information {@link connectivityState}
   * @memberof PubSubApiClient.prototype
   */
  async getConnectivityState() {
    return this.#client?.getChannel()?.getConnectivityState(false);
  }

  /**
   * Subscribes to a topic and retrieves all past events in retention window.
   * @param {string} topicName name of the topic that we're subscribing to
   * @param {number | null} [numRequested] optional number of events requested. If not supplied or null, the client keeps the subscription alive forever.
   * @returns {Promise<PubSubEventEmitter>} Promise that holds an emitter that allows you to listen to received events and stream lifecycle events
   * @memberof PubSubApiClient.prototype
   */
  // @ts-ignore
  async subscribeFromEarliestEvent(topicName, numRequested = null) {
    return this.#subscribe({
      topicName,
      numRequested,
      replayPreset: 1,
    });
  }

  /**
   * Subscribes to a topic and retrieves past events starting from a replay ID.
   * @param {string} topicName name of the topic that we're subscribing to
   * @param {number | null} numRequested number of events requested. If null, the client keeps the subscription alive forever.
   * @param {number} replayId replay ID
   * @returns {Promise<PubSubEventEmitter>} Promise that holds an emitter that allows you to listen to received events and stream lifecycle events
   * @memberof PubSubApiClient.prototype
   */
  // @ts-ignore
  async subscribeFromReplayId(topicName, numRequested, replayId) {
    return this.#subscribe({
      topicName,
      numRequested,
      replayPreset: 2,
      replayId: encodeReplayId(replayId),
    });
  }

  /**
   * Subscribes to a topic.
   * @param {string} topicName name of the topic that we're subscribing to
   * @param {number | null} [numRequested] optional number of events requested. If not supplied or null, the client keeps the subscription alive forever.
   * @returns {Promise<PubSubEventEmitter>} Promise that holds an emitter that allows you to listen to received events and stream lifecycle events
   * @memberof PubSubApiClient.prototype
   */
  // @ts-ignore
  async subscribe(topicName, numRequested = null) {
    return this.#subscribe({
      topicName,
      numRequested,
    });
  }

  /**
   * Subscribes to a topic using the gRPC client and an event schema
   * @param {object} subscribeRequest subscription request
   * @return {PubSubEventEmitter} emitter that allows you to listen to received events and stream lifecycle events
   */
  // @ts-ignore
  async #subscribe(subscribeRequest) {
    console.log("subscribeRequest:", subscribeRequest); 
    let { topicName, numRequested } = subscribeRequest;
    try {
      // Check number of requested events
      let isInfiniteEventRequest = false;
      if (numRequested === null || numRequested === undefined) {
        isInfiniteEventRequest = true;
        subscribeRequest.numRequested = numRequested = MAX_EVENT_BATCH_SIZE;
      } else {
        if (typeof numRequested !== "number") {
          throw new Error(
            `Expected a number type for number of requested events but got ${typeof numRequested}`
          );
        }
        if (!Number.isSafeInteger(numRequested) || numRequested < 1) {
          throw new Error(
            `Expected an integer greater than 1 for number of requested events but got ${numRequested}`
          );
        }
        if (numRequested > MAX_EVENT_BATCH_SIZE) {
          this.#logger.warn(
            `The number of requested events for ${topicName} exceeds max event batch size (${MAX_EVENT_BATCH_SIZE}).`
          );
        }
      }
      // Check client connection
      if (!this.#client) {
        throw new Error("Pub/Sub API client is not connected.");
      }

      // Check for an existing subscription
      let subscription = this.#subscriptions.get(topicName);

      // Send subscription request
      if (!subscription) {
        subscription = this.#client.subscribe();
        this.#subscriptions.set(topicName, subscription);
      }
      console.log("subscribeRequest:", subscribeRequest);
      const subscribeRequest2 = FetchRequest.fromPartial(subscribeRequest);
      subscription.write(subscribeRequest2);
      this.#logger.info(
        `Subscribe request sent for ${numRequested} events from ${topicName}...`
      );

      // Listen to new events
      const eventEmitter = new PubSubEventEmitter(topicName, numRequested);
      // @ts-ignore
      subscription.on("data", async (data) => {
        const latestReplayId = decodeReplayId(data.latestReplayId);
        if (data.events) {
          this.#logger.info(
            `Received ${data.events.length} events, latest replay ID: ${latestReplayId}`
          );
          for (const event of data.events) {
            try {
              // Load event schema from cache or from the gRPC client
              const schema = await this.#getEventSchemaFromId(
                event.event.schemaId
              );
              // Parse event thanks to schema
              //console.log("Step-1", schema);
              //console.log("Step-2", event);
              // @ts-ignore
              const parsedEvent = parseEvent(schema, event, console);
              this.#logger.debug(parsedEvent);
              eventEmitter.emit("data", parsedEvent);
            } catch (error) {
              // Report event parsing error with replay ID if possible
              let replayId;
              try {
                replayId = decodeReplayId(event.replayId);
                // eslint-disable-next-line no-empty, no-unused-vars
              } catch (error) {}
              const message = replayId
                ? `Failed to parse event with replay ID ${replayId}`
                : `Failed to parse event with unknown replay ID (latest replay ID was ${latestReplayId})`;
              const parseError = new EventParseError(
                message,
                error as Error,
                replayId,
                event,
                latestReplayId
              );
              eventEmitter.emit("error", parseError);
              this.#logger.error(parseError);
            }

            // Handle last requested event
            if (
              eventEmitter.getReceivedEventCount() ===
              eventEmitter.getRequestedEventCount()
            ) {
              if (isInfiniteEventRequest) {
                // Request additional events
                this.requestAdditionalEvents(
                  eventEmitter,
                  MAX_EVENT_BATCH_SIZE
                );
              } else {
                // Emit a 'lastevent' event when reaching the last requested event count
                // @ts-ignore
                eventEmitter.emit("lastevent");
              }
            }
          }
        } else {
          // If there are no events then, every 270 seconds (or less) the server publishes a keepalive message with
          // the latestReplayId and pendingNumRequested (the number of events that the client is still waiting for)
          this.#logger.debug(
            `Received keepalive message. Latest replay ID: ${latestReplayId}`
          );
          data.latestReplayId = latestReplayId; // Replace original value with decoded value
          eventEmitter.emit("keepalive", data);
        }
      });
      subscription.on("end", () => {
        this.#subscriptions.delete(topicName);
        this.#logger.info("gRPC stream ended");
        // @ts-ignore
        eventEmitter.emit("end");
      });
      // @ts-ignore
      subscription.on("error", (error) => {
        this.#logger.error(`gRPC stream error: ${JSON.stringify(error)}`);
        eventEmitter.emit("error", error);
      });
      // @ts-ignore
      subscription.on("status", (status) => {
        this.#logger.info(`gRPC stream status: ${JSON.stringify(status)}`);
        eventEmitter.emit("status", status);
      });
      return eventEmitter;
    } catch (error) {
      // @ts-ignore
      throw new Error(`Failed to subscribe to events for topic ${topicName}`, {
        cause: error,
      });
    }
  }

  /**
   * Request additional events on an existing subscription.
   * @param {PubSubEventEmitter} eventEmitter event emitter that was obtained in the first subscribe call
   * @param {number} numRequested number of events requested.
   */
  // @ts-ignore
  async requestAdditionalEvents(eventEmitter, numRequested) {
    const topicName = eventEmitter.getTopicName();

    // Retrieve existing subscription
    const subscription = this.#subscriptions.get(topicName);
    if (!subscription) {
      throw new Error(
        `Failed to request additional events for topic ${topicName}, no active subscription found.`
      );
    }

    // Request additional events
    eventEmitter._resetEventCount(numRequested);
    subscription.write({
      topicName,
      numRequested: numRequested,
    });
    this.#logger.debug(
      `Resubscribing to a batch of ${numRequested} events for: ${topicName}`
    );
  }

  /**
   * Retrieves an event schema from the cache based on its ID.
   * If it's not cached, fetches the shema with the gRPC client.
   * @param {string} schemaId ID of the schema that we're fetching
   * @returns {Promise<Schema>} Promise holding parsed event schema
   */
  // @ts-ignore
  async #getEventSchemaFromId(schemaId) {
    let schema = this.#schemaChache.getFromId(schemaId);
    if (!schema) {
      try {
        schema = await this.#fetchEventSchemaFromIdWithClient(schemaId);
        this.#schemaChache.set(schema);
      } catch (error) {
        // @ts-ignore
        throw new Error(`Failed to load schema with ID ${schemaId}`, {
          cause: error,
        });
      }
    }
    return schema;
  }

  /**
   * Requests the event schema from an ID using the gRPC client
   * @param {string} schemaId schema ID that we're fetching
   * @returns {Promise<Schema>} Promise holding parsed event schema
   */
  // @ts-ignore
  async #fetchEventSchemaFromIdWithClient(schemaId) {
    return new Promise((resolve, reject) => {
      // @ts-ignore
      this.#client.getSchema({ schemaId }, (schemaError, res) => {
        if (schemaError) {
          reject(schemaError);
        } else {
          //console.log("fetchEventSchemaFromIdWithClient:", res.schemaJson);
          const schemaType = avro.parse(res.schemaJson, {
            registry: { long: CustomLongAvroType },
          });
          resolve({
            id: schemaId,
            type: schemaType,
          });
        }
      });
    });
  }

  /**
   * Disconnects the Pub/Sub API client.
   * @returns {Promise<void>} Promise that resolves once the client is disconnected
   * @memberof PubSubApiClient.prototype
   */
  async disconnect() {
    if (this.#client) {
      try {
        // Close all active subscriptions
        for (const [topicName, subscription] of this.#subscriptions.entries()) {
          subscription.end();
          this.#subscriptions.delete(topicName);
        }
        this.#logger.info("All subscriptions have been closed.");

        // Close the gRPC client
        this.#client.close();
        this.#client = null;
        this.#logger.info("Disconnected from Pub/Sub API.");
      } catch (error) {
        throw new EventParseError("Failed to disconnect from Pub/Sub API", error as Error);
      }
    } else {
      this.#logger.warn("Client is not connected.");
    }
  }
}
