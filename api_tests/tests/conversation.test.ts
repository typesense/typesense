import { describe, it, expect } from "bun:test";
import { Phases, Filters } from "../src/constants";
import { z } from "zod";
import { fetchMultiNode } from "../src/request";

const CreateConversationModelResponse = z.object({
    api_key: z.string(),
    history_collection: z.string(),
    id: z.string(),
    max_bytes: z.number(),
    model_name: z.string(),
    system_prompt: z.string(),
    ttl: z.number(),
})

describe(Phases.MULTI_FRESH, () => {
  it(Filters.SECRETS + "conversation with rotation", async () => {
    await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "base_collection",
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
      })
    });

    await fetchMultiNode(1, "/collections", {
      method: "POST",
      body: JSON.stringify({
        name: "conversation_store",
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
      }),
    });

    const res = await fetchMultiNode(1, "/conversations/models", {
      method: "POST",
      body: JSON.stringify({
        id: "model-1",
        system_prompt:
          "You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.",
        history_collection: "conversation_store",
        model_name: "openai/gpt-3.5-turbo-0125",
        max_bytes: 16384,
        api_key: Bun.env.OPEN_AI_API_KEY ?? "sk-random",
      }),
    });

    expect(res.ok).toBe(true);
    const data = CreateConversationModelResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("model-1");
    expect(data.data?.model_name).toBe("openai/gpt-3.5-turbo-0125");
    expect(data.data?.system_prompt).toBe("You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.");
    expect(data.data?.history_collection).toBe("conversation_store");
    expect(data.data?.max_bytes).toBe(16384);
    expect(data.data?.ttl).toBe(86400);
  })
})

describe(Phases.MULTI_RESTARTED, async () => {
  it(Filters.SECRETS + "validate conversation with rotation",async () => {
    const res = await fetchMultiNode(1, "/conversations/models/model-1", {
      method: "GET",
    });
    expect(res.ok).toBe(true);
    const data = CreateConversationModelResponse.safeParse(await res.json());
    expect(data.success).toBe(true);
    expect(data.data?.id).toBe("model-1");
    expect(data.data?.model_name).toBe("openai/gpt-3.5-turbo-0125");
    expect(data.data?.system_prompt).toBe("You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.");
    expect(data.data?.history_collection).toBe("conversation_store");
    expect(data.data?.max_bytes).toBe(16384);
    expect(data.data?.ttl).toBe(86400);

    const res2 = await fetchMultiNode(2, "/conversations/models/model-1", {
      method: "GET",
    });
    expect(res2.ok).toBe(true);
    const data2 = CreateConversationModelResponse.safeParse(await res2.json());
    expect(data2.success).toBe(true);
    expect(data2.data?.id).toBe("model-1");
    expect(data2.data?.model_name).toBe("openai/gpt-3.5-turbo-0125");
    expect(data2.data?.system_prompt).toBe("You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.");
    expect(data2.data?.history_collection).toBe("conversation_store");
    expect(data2.data?.max_bytes).toBe(16384);
    expect(data2.data?.ttl).toBe(86400);

    const res3 = await fetchMultiNode(3, "/conversations/models/model-1", {
      method: "GET",
    });
    expect(res3.ok).toBe(true);
    const data3 = CreateConversationModelResponse.safeParse(await res3.json());
    expect(data3.success).toBe(true);
    expect(data3.data?.id).toBe("model-1");
    expect(data3.data?.model_name).toBe("openai/gpt-3.5-turbo-0125");
    expect(data3.data?.system_prompt).toBe("You are an assistant for question-answering like Paul Graham. You can only make conversations based on the provided context. If a response cannot be formed strictly using the context, politely say you don't have knowledge about that topic. Do not answer questions that are not strictly on the topic of Paul Graham'''s essays.");
    expect(data3.data?.history_collection).toBe("conversation_store");
    expect(data3.data?.max_bytes).toBe(16384);
    expect(data3.data?.ttl).toBe(86400);
  })
})