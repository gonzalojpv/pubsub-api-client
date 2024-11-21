## Prerequisites

- Node.js, TypeScript
- Salesforce API credentials (instance URL, consumer key, and consumer secret)

## Installation

1. Clone the repository:
   ```
   git clone git@github.com:gonzalojpv/pubsub-api-client.git
   ```
   
2. Install dependencies:
   ```
   cd pubsub-api-client
   npm install
   ```

## Configuration

Create a `.env` file in the root of the project and add the following environment variables:
   ```
SALESFORCE_AUTH_TYPE=oauth-client-credentials
SALESFORCE_LOGIN_URL=
SALESFORCE_CLIENT_ID=
SALESFORCE_CLIENT_SECRET=

PUB_SUB_ENDPOINT=

   ```


## Usage

Run script:
   ```
   npx ts-node ./src/index.ts
   ```
   Once the script runs, you will be prompted to select one of the following options:

   1. Open stream and subscribe to events starting now
      - This option will open a stream and start subscribing to events from the current moment onwards.

   2. Open stream from a specific replayId
      - This option will prompt you to enter a replayId. The stream will then open and start subscribing to events from the specified replayId.

   3. Only pull in 1st event after a specific replayId - stream will close after 1 event
      - This option will prompt you to enter a replayId. The stream will open and pull in only the first event after the specified replayId, then the stream will close automatically.

