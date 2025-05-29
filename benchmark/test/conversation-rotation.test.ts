import type { NodeConfig } from "@/services/typesense-process";

import { afterAll, beforeAll, expect, test } from "vitest";

import {
  closeDownTypesenseServer,
  fetchNode,
  globalTypesenseManager,
  openAIProxy,
  startTypesenseServer,
} from "@/global";

const collectionName = "base_collection";
const conversationStoreName = "conversation_store";
const conversationModelName = "gpt-4-turbo";

let nodes: NodeConfig[];
beforeAll(async () => {
  openAIProxy.start();

  const res = await startTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }

  nodes = res.value;
});

afterAll(async () => {
  await openAIProxy.stop();

  const res = await closeDownTypesenseServer();

  if (res.isErr()) {
    throw new Error(res.error.message);
  }
});

test("Conversation with rotation", async () => {
  // Setup
  const collectionCreationRes = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: collectionName,
      fields: [
        {
          name: "title",
          type: "string",
          facet: false,
        },
        {
          name: "text",
          type: "string",
          facet: false,
        },
      ],
    },
  });

  if (collectionCreationRes.isErr()) {
    throw new Error(collectionCreationRes.error.message);
  }

  const conversationStoreRes = await fetchNode({
    port: 8108,
    endpoint: "collections",
    method: "POST",
    body: {
      name: conversationStoreName,
      fields: [
        {
          name: "conversation_id",
          type: "string",
        },
        {
          name: "model_id",
          type: "string",
        },
        {
          name: "role",
          type: "string",
          index: false,
        },
        {
          name: "message",
          type: "string",
          index: false,
        },
        {
          name: "timestamp",
          type: "int32",
        },
      ],
    },
  });

  if (conversationStoreRes.isErr()) {
    throw new Error(conversationStoreRes.error.message);
  }

  const conversationModelRes = await fetchNode({
    port: 8108,
    endpoint: "conversations/models",
    method: "POST",
    body: {
      id: conversationModelName,
      system_prompt:
        "You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.",
      history_collection: "conversation_store",
      model_name: "openai/gpt-4-turbo",
      max_bytes: 16384,
      api_key: "random",
    },
  });

  if (conversationModelRes.isErr()) {
    throw new Error(conversationModelRes.error.message);
  }

  // Check that the conversation model is available on all nodes
  await Promise.all(
    nodes.map(async (node) => {
      const modelRes = await fetchNode({
        port: node.http,
        endpoint: `conversations/models/${conversationModelName}`,
        method: "GET",
      });

      if (modelRes.isErr()) {
        throw new Error(modelRes.error.message);
      }

      expect(modelRes.value.id).toBe(conversationModelName);
    }),
  );

  // Restart the nodes one by one and check that the conversation model is available on all nodes
  for (const node of nodes) {
    const res = await globalTypesenseManager.restartProcess(node.http);

    if (res.isErr()) {
      throw new Error(res.error.message);
    }

    await Promise.all(
      nodes.map(async (node) => {
        const modelRes = await fetchNode({
          port: node.http,
          endpoint: `conversations/models/${conversationModelName}`,
          method: "GET",
        });

        if (modelRes.isErr()) {
          throw new Error(modelRes.error.message);
        }

        expect(modelRes.value.id).toBe(conversationModelName);
      }),
    );
  }
});
