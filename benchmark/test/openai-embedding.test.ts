
import { afterAll, beforeAll, expect, test } from "vitest";

import {
  closeDownTypesenseServer,
  env,
  fetchNode,
  openAIProxy,
  restartTypesenseServer,
  startTypesenseServer,
} from "@/global";
import { delay } from "@/utils/base";

const collectionName = "openai_collection";

beforeAll(async () => {
  openAIProxy.start();

  const res = await startTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

afterAll(async () => {
  await openAIProxy.stop();

  const res = await closeDownTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

test("OpenAI embedding num_dimensions", async () => {
  // Create the collection
  const collectionCreationRes = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: collectionName,
      fields: [
        {
          name: "product_name",
          type: "string",
          facet: false,
        },
        {
          name: "embedding",
          type: "float[]",
          num_dim: env.TYPESENSE_NUM_DIM,
          embed: {
            from: ["product_name"],
            model_config: {
              model_name: "openai/text-embedding-3-large",
              api_key: "random",
            },
          },
        },
      ],
    },
  });

  if (collectionCreationRes.isErr()) {
    throw new Error(collectionCreationRes.error.message);
  }

  await delay(1000);

  const collection = collectionCreationRes.value;

  // Get the number of dimensions from the collection
  let fetchCollectionRes = await fetchNode({
    port: 8108,
    endpoint: `collections/${collection.name}`,
    method: "GET",
  });

  if (fetchCollectionRes.isErr()) {
    throw new Error(fetchCollectionRes.error.message);
  }

  expect(fetchCollectionRes.value.fields?.find((field) => field.name === "embedding")?.num_dim).toBe(
    env.TYPESENSE_NUM_DIM,
  );

  // Restart the Typesense server
  await restartTypesenseServer();

  // Get the number of dimensions from the collection
  fetchCollectionRes = await fetchNode({
    port: 8108,
    endpoint: `collections/${collection.name}`,
    method: "GET",
  });

  if (fetchCollectionRes.isErr()) {
    throw new Error(fetchCollectionRes.error.message);
  }

  expect(fetchCollectionRes.value.fields?.find((field) => field.name === "embedding")?.num_dim).toBe(
    env.TYPESENSE_NUM_DIM,
  );
});
