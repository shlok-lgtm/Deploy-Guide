#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import { registerTools } from "./tools.js";

const SERVER_NAME = "basis-protocol";
const SERVER_VERSION = "1.1.0";
const SERVER_DESCRIPTION =
  "Decision integrity infrastructure for on-chain finance. Verifiable risk scores for stablecoins (SII), DeFi protocols (PSI), and wallet risk profiles. Composite risk via CQI.";

function createServer(): McpServer {
  const server = new McpServer(
    {
      name: SERVER_NAME,
      version: SERVER_VERSION,
    },
    {
      capabilities: {
        tools: {},
      },
      instructions: SERVER_DESCRIPTION,
    },
  );

  registerTools(server);
  return server;
}

const isHttp = process.argv.includes("--http");

if (isHttp) {
  const rawPort = process.env["PORT"] ?? "3000";
  const port = Number(rawPort);

  const app = express();
  app.use(express.json());

  app.post("/mcp", async (req, res) => {
    const server = createServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    res.on("close", () => {
      transport.close().catch(() => {});
      server.close().catch(() => {});
    });
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  });

  app.get("/mcp", async (req, res) => {
    const server = createServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    res.on("close", () => {
      transport.close().catch(() => {});
      server.close().catch(() => {});
    });
    await server.connect(transport);
    await transport.handleRequest(req, res);
  });

  app.delete("/mcp", async (req, res) => {
    const server = createServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    res.on("close", () => {
      transport.close().catch(() => {});
      server.close().catch(() => {});
    });
    await server.connect(transport);
    await transport.handleRequest(req, res);
  });

  app.get("/health", (_req, res) => {
    res.json({
      status: "ok",
      server: SERVER_NAME,
      version: SERVER_VERSION,
    });
  });

  app.listen(port, () => {
    process.stderr.write(
      `[basis-mcp] HTTP server listening on port ${port} — endpoint: /mcp\n`,
    );
  });
} else {
  const server = createServer();
  const transport = new StdioServerTransport();

  process.stderr.write(
    `[basis-mcp] Starting stdio transport — ${SERVER_NAME} v${SERVER_VERSION}\n`,
  );

  await server.connect(transport);
}
