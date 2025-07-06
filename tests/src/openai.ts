import { execSync } from "child_process";
import { existsSync, readFileSync } from "fs";
import { createServer as createHttpServer } from "http";
import { createServer as createHttpsServer, request as httpsRequest } from "https";
import { Socket } from "net";
import { join } from "path";
import { URL } from "url";
import type { ErrorWithMessage } from "@/error";
import type { IncomingMessage, RequestOptions, ServerResponse } from "http";
import type { RequestOptions as HttpsRequestOptions } from "https";
import type OpenAI from "openai";
import type { Ora } from "ora";

import { faker } from "@faker-js/faker";
import { ResultAsync } from "neverthrow";

import { toErrorWithMessage } from "@/error";
import { logger } from "@/logger";

type ModelsResponse = OpenAI.PageResponse<OpenAI.Models.Model>;
type ChatCompletionResponse = OpenAI.Chat.Completions.ChatCompletion;
type ChatCompletionChunkResponse = OpenAI.Chat.Completions.ChatCompletionChunk;
type EmbeddingCreateResponse = OpenAI.Embeddings.CreateEmbeddingResponse;
type MockResponse = ModelsResponse | ChatCompletionResponse | ChatCompletionChunkResponse | EmbeddingCreateResponse;

const DEFAULT_MOCK_RESPONSES: Record<string, MockResponse> = {
  "/v1/models": {
    object: "list",
    data: [
      {
        id: "gpt-4-turbo",
        object: "model",
        created: 1687883450,
        owned_by: "openai",
      },
      {
        id: "gpt-4",
        object: "model",
        created: 1687882410,
        owned_by: "openai",
      },
      {
        id: "gpt-3.5-turbo",
        object: "model",
        created: 1677649963,
        owned_by: "openai",
      },
    ],
  },
  "/v1/chat/completions": {
    id: "chatcmpl-123",
    object: "chat.completion",
    created: 1677858242,
    model: "gpt-4-turbo",
    usage: {
      prompt_tokens: 13,
      completion_tokens: 7,
      total_tokens: 20,
    },
    choices: [
      {
        message: {
          role: "assistant",
          refusal: null,
          content: "Hello! How can I assist you today?",
        },
        logprobs: null,
        finish_reason: "stop",
        index: 0,
      },
    ],
  },
};

export class OpenAIProxy {
  private proxyServer;
  private mockServer;
  private readonly proxyPort: number;
  private readonly mockPort: number;
  private mockResponses: Record<string, MockResponse>;

  constructor(
    private readonly spinner: Ora,
    private readonly workingDirectory: string,
    private readonly numDim: number,
    proxyPort = 8443,
    mockPort = 8444,
  ) {
    this.proxyPort = proxyPort;
    this.mockPort = mockPort;
    this.numDim = numDim;
    this.mockResponses = { ...DEFAULT_MOCK_RESPONSES, ...this.generateEmbeddingsResponse() };

    const certificates = this.generateCertificates();
    this.mockServer = createHttpsServer(certificates, this.handleMockRequest.bind(this));
    this.proxyServer = createHttpServer(this.handleProxyRequest.bind(this));
    this.proxyServer.on("connect", this.handleConnect.bind(this));
  }

  private generateEmbeddingsResponse(): { "/v1/embeddings": OpenAI.Embeddings.CreateEmbeddingResponse } {
    return {
      "/v1/embeddings": {
        model: "gpt-3.5-turbo",
        object: "list",
        usage: {
          prompt_tokens: 13,
          total_tokens: 13,
        },
        data: [
          {
            embedding: Array.from({ length: this.numDim }, () =>
              faker.number.float({ min: -1, max: 1, fractionDigits: 6 }),
            ),
            index: 0,
            object: "embedding",
          },
        ],
      },
    };
  }
  private generateCertificates() {
    const certPath = join(this.workingDirectory, "cert.pem");
    const keyPath = join(this.workingDirectory, "key.pem");

    if (!existsSync(certPath) || !existsSync(keyPath)) {
      this.spinner.info("Generating self-signed certificate...");
      execSync(
        'openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"',
        {
          cwd: this.workingDirectory,
          stdio: "ignore",
        },
      );
    }

    return {
      key: readFileSync(keyPath),
      cert: readFileSync(certPath),
    };
  }

  private handleMockRequest(req: IncomingMessage, res: ServerResponse) {
    logger.info(`[OpenAI Interceptor]: Handling mock request: ${req.method} ${req.url}`);

    if (!req.url) {
      res.writeHead(404);
      res.end(JSON.stringify({ error: { message: "Not found" } }));
      return;
    }

    const mockResponse = this.mockResponses[req.url];
    if (!mockResponse) {
      res.writeHead(404);
      res.end(JSON.stringify({ error: { message: "Not found" } }));
      return;
    }

    if (req.method === "POST" && req.url === "/v1/chat/completions") {
      this.handleChatCompletions(req, res, mockResponse);
    } else {
      this.sendMockResponse(res, mockResponse);
    }
  }

  private handleChatCompletions(req: IncomingMessage, res: ServerResponse, mockResponse: MockResponse) {
    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk.toString();
    });

    req.on("end", () => {
      try {
        const requestData = JSON.parse(body) as OpenAI.Chat.Completions.ChatCompletion;
        logger.info("[OpenAI Interceptor]: Received chat request:", requestData);

        if (!("model" in mockResponse && "model" in requestData)) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: { message: "Missing model parameter" } }));
          return;
        }

        const response = {
          ...mockResponse,
          model: requestData.model || mockResponse.model,
        };

        this.sendMockResponse(res, response);
      } catch (error) {
        logger.error("Error parsing request body:", error);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: { message: "Invalid request body" } }));
      }
    });
  }

  private sendMockResponse(res: ServerResponse, response: MockResponse) {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(JSON.stringify(response));
  }

  private handleProxyRequest(req: IncomingMessage, res: ServerResponse) {
    if (!req.url || !req.headers.host) {
      res.writeHead(400);
      res.end("Bad Request");
      return;
    }

    const targetUrl = new URL(req.url.startsWith("http") ? req.url : `http://${req.headers.host}${req.url}`);

    if (targetUrl.hostname === "api.openai.com") {
      this.forwardToMock(req, res, targetUrl);
    } else {
      // Forward to original destination
      const options: RequestOptions = {
        hostname: targetUrl.hostname,
        port: targetUrl.port || 80,
        path: targetUrl.pathname + targetUrl.search,
        method: req.method,
        headers: req.headers,
      };

      const proxyReq = httpsRequest(options, (proxyRes: IncomingMessage) => {
        res.writeHead(proxyRes.statusCode ?? 500, proxyRes.headers);
        proxyRes.pipe(res);
      });

      proxyReq.on("error", (error: Error) => {
        logger.error("Proxy request error:", error);
        res.writeHead(500);
        res.end("Proxy Error");
      });

      req.pipe(proxyReq);
    }
  }

  private forwardToMock(req: IncomingMessage, res: ServerResponse, targetUrl: URL) {
    const options: HttpsRequestOptions = {
      hostname: "localhost",
      port: this.mockPort,
      path: targetUrl.pathname,
      method: req.method,
      headers: {
        ...req.headers,
        host: "localhost",
      },
    };

    const proxyReq = httpsRequest(options, (proxyRes: IncomingMessage) => {
      res.writeHead(proxyRes.statusCode ?? 500, proxyRes.headers);
      proxyRes.pipe(res);
    });

    proxyReq.on("error", (error: Error) => {
      logger.error("Mock request error:", error);
      res.writeHead(500);
      res.end("Mock Server Error");
    });

    req.pipe(proxyReq);
  }

  private handleConnect(req: IncomingMessage, clientSocket: Socket, head: Buffer) {
    if (!req.url) {
      clientSocket.end();
      return;
    }

    const [targetHost, targetPortStr] = req.url.split(":");
    const targetPort = targetPortStr ? parseInt(targetPortStr) || 443 : 443;

    if (targetHost === "api.openai.com") {
      const mockSocket = new Socket();
      mockSocket.connect(this.mockPort, "localhost", () => {
        clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        mockSocket.write(head);
        mockSocket.pipe(clientSocket);
        clientSocket.pipe(mockSocket);
      });

      mockSocket.on("error", (error: Error) => {
        logger.error("Mock socket error:", error);
        clientSocket.end();
      });
    } else {
      if (!targetHost) {
        clientSocket.end();
        return;
      }

      const serverSocket = new Socket();
      serverSocket.connect(targetPort, targetHost, () => {
        clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
        serverSocket.write(head);
        serverSocket.pipe(clientSocket);
        clientSocket.pipe(serverSocket);
      });

      serverSocket.on("error", (error: Error) => {
        logger.error("Server socket error:", error);
        clientSocket.end();
      });
    }

    clientSocket.on("error", (error: Error) => {
      logger.error("Client socket error:", error);
      clientSocket.end();
    });
  }

  public setMockResponse(path: string, response: MockResponse): void {
    this.mockResponses[path] = response;
  }

  public start(): ResultAsync<void, ErrorWithMessage> {
    return ResultAsync.fromPromise(
      new Promise<void>((resolve, reject) => {
        this.mockServer
          .listen(this.mockPort, () => {
            this.spinner.text = `\n Mock HTTPS server running on port ${this.mockPort}`;
            this.proxyServer
              .listen(this.proxyPort, () => {
                this.spinner.succeed(`Proxy server running on port ${this.proxyPort}`);
                resolve();
              })
              .on("error", reject);
          })
          .on("error", reject);
      }),
      toErrorWithMessage,
    );
  }

  public stop(): ResultAsync<void, ErrorWithMessage> {
    return ResultAsync.fromPromise(
      new Promise<void>((resolve, reject) => {
        this.mockServer.close((err) => {
          if (err) reject(err);
          this.proxyServer.close((err) => {
            if (err) reject(err);
            this.spinner.succeed("Servers stopped");
            resolve();
          });
        });
      }),
      toErrorWithMessage,
    );
  }
}
