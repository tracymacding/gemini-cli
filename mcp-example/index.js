#!/usr/bin/env node

/**
 * MCP Server for StarRocks Monitoring
 * Provides a tool to check the liveness of Backend (BE) nodes.
 *
 * NOTICE: This entire StarRocksMcpServer implementation has been commented out.
 * Please use index-expert-enhanced.js for active StarRocks MCP functionality.
 */

/*
// ORIGINAL STARROCKS MCP SERVER IMPLEMENTATION - COMMENTED OUT

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import mysql from 'mysql2/promise';

class StarRocksMcpServer {
  constructor() {
    this.server = new Server(
      {
        name: 'starrocks-mcp-server',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupHandlers();
  }

  setupHandlers() {
    // List available tools
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: 'get_starrocks_backends',
            description: 'Checks the status of all StarRocks Backend (BE/Compute Node) nodes and returns their liveness and other details.',
            inputSchema: {
              type: 'object',
              properties: {},
              required: []
            }
          },
          // ... all other tool definitions would go here
        ]
      };
    });

    // Execute tool calls
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      // ... all tool handling logic would go here
    });
  }

  // ... all handler methods would go here ...

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('StarRocks MCP Server running on stdio');
  }
}

// Start the server
const server = new StarRocksMcpServer();
server.run().catch((error) => {
  console.error('Server error:', error);
  process.exit(1);
});

// END OF COMMENTED OUT IMPLEMENTATION
*/

// ============================================================================
// NOTICE: StarRocksMcpServer implementation has been commented out
// ============================================================================

console.error('┌─────────────────────────────────────────────────────────────┐');
console.error('│                        NOTICE                               │');
console.error('├─────────────────────────────────────────────────────────────┤');
console.error('│  StarRocksMcpServer implementation has been commented out   │');
console.error('│  This file is no longer functional as an MCP server        │');
console.error('│                                                             │');
console.error('│  For active StarRocks MCP functionality, please use:       │');
console.error('│  → index-expert-enhanced.js                                 │');
console.error('│                                                             │');
console.error('│  This provides enhanced multi-expert functionality with:   │');
console.error('│  • Storage Expert                                           │');
console.error('│  • Compaction Expert                                        │');
console.error('│  • Import Expert                                            │');
console.error('│  • Coordinated Multi-Expert Analysis                       │');
console.error('└─────────────────────────────────────────────────────────────┘');

// Exit gracefully
process.exit(0);