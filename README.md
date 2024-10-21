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
