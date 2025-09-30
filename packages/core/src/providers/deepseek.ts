/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  ModelProvider,
  ProviderConfig,
  LLMClient,
  GenerateRequest,
  GenerateResponse,
  GenerateStreamChunk,
  ModelInfo,
  ToolCall,
} from './base.js';

/**
 * DeepSeek provider implementation
 *
 * Supports DeepSeek's models via their OpenAI-compatible API
 * Documentation: https://platform.deepseek.com/api-docs
 */
export class DeepSeekProvider implements ModelProvider {
  readonly id = 'deepseek';
  readonly name = 'DeepSeek';
  readonly description = 'DeepSeek AI models - high performance with competitive pricing';

  readonly models: ModelInfo[] = [
    {
      id: 'deepseek-chat',
      name: 'DeepSeek Chat',
      description: 'Advanced conversational AI with excellent reasoning capabilities',
      contextWindow: 128000,
      maxOutputTokens: 4096,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.14,
        outputTokensPer1M: 0.28,
        currency: 'USD',
      },
    },
    {
      id: 'deepseek-coder',
      name: 'DeepSeek Coder',
      description: 'Specialized model for code generation and understanding',
      contextWindow: 128000,
      maxOutputTokens: 4096,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.14,
        outputTokensPer1M: 0.28,
        currency: 'USD',
      },
    },
    {
      id: 'deepseek-reasoner',
      name: 'DeepSeek Reasoner',
      description: 'Advanced reasoning model with chain-of-thought capabilities',
      contextWindow: 128000,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.55,
        outputTokensPer1M: 2.19,
        currency: 'USD',
      },
    },
  ];

  readonly defaultModel = 'deepseek-chat';

  isConfigured(): boolean {
    return !!(
      process.env['DEEPSEEK_API_KEY'] ||
      process.env['DEEPSEEK_KEY']
    );
  }

  async initialize(config: ProviderConfig): Promise<void> {
    if (!config.apiKey && !this.isConfigured()) {
      throw new Error(
        'DeepSeek API key not found. Please set DEEPSEEK_API_KEY environment variable.'
      );
    }
  }

  async createClient(config: ProviderConfig): Promise<LLMClient> {
    const apiKey = config.apiKey ||
      process.env['DEEPSEEK_API_KEY'] ||
      process.env['DEEPSEEK_KEY'];

    if (!apiKey) {
      throw new Error('DeepSeek API key is required');
    }

    return new DeepSeekClient({
      apiKey,
      endpoint: config.endpoint || 'https://api.deepseek.com',
      ...config.options,
    });
  }

  validateModel(modelName: string): boolean {
    return this.models.some(model => model.id === modelName);
  }

  getModelInfo(modelName: string): ModelInfo | undefined {
    return this.models.find(model => model.id === modelName);
  }
}

/**
 * DeepSeek client implementation using OpenAI-compatible API
 */
class DeepSeekClient implements LLMClient {
  readonly provider = 'deepseek';
  private apiKey: string;
  private endpoint: string;

  constructor(config: { apiKey: string; endpoint: string }) {
    this.apiKey = config.apiKey;
    this.endpoint = config.endpoint;
  }

  async generate(request: GenerateRequest): Promise<GenerateResponse> {
    const deepseekRequest = this.convertRequest(request);

    const response = await fetch(`${this.endpoint}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`,
      },
      body: JSON.stringify(deepseekRequest),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`DeepSeek API error: ${response.status} - ${error}`);
    }

    const data = await response.json();
    return this.convertResponse(data);
  }

  async *generateStream(request: GenerateRequest): AsyncIterable<GenerateStreamChunk> {
    const deepseekRequest = {
      ...this.convertRequest(request),
      stream: true,
    };

    const response = await fetch(`${this.endpoint}/v1/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`,
        'Accept': 'text/event-stream',
      },
      body: JSON.stringify(deepseekRequest),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`DeepSeek streaming API error: ${response.status} - ${error}`);
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('Failed to get response stream reader');
    }

    const decoder = new TextDecoder();
    let buffer = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6).trim();
            if (data === '[DONE]') {
              return;
            }

            try {
              const event = JSON.parse(data);
              const chunk = this.convertStreamChunk(event);
              if (chunk) {
                yield chunk;
              }
            } catch (e) {
              // Ignore invalid JSON chunks
            }
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  supportsCapability(capability: keyof import('./base.js').ModelCapabilities): boolean {
    switch (capability) {
      case 'textGeneration':
      case 'streaming':
      case 'systemMessages':
      case 'tools':
        return true;
      case 'vision':
        return false;
      default:
        return false;
    }
  }

  private convertRequest(request: GenerateRequest): any {
    const messages = request.messages.map(msg => {
      if (msg.role === 'system') {
        return {
          role: 'system',
          content: msg.content,
        };
      } else if (msg.role === 'assistant') {
        // Handle tool calls
        if (msg.toolCalls && msg.toolCalls.length > 0) {
          return {
            role: 'assistant',
            content: msg.content || null,
            tool_calls: msg.toolCalls.map(tc => ({
              id: tc.id,
              type: 'function',
              function: {
                name: tc.name,
                arguments: typeof tc.arguments === 'string'
                  ? tc.arguments
                  : JSON.stringify(tc.arguments),
              }
            }))
          };
        }
        return {
          role: 'assistant',
          content: msg.content,
        };
      } else if (msg.role === 'tool' && msg.toolCallId) {
        return {
          role: 'tool',
          tool_call_id: msg.toolCallId,
          content: typeof msg.content === 'string'
            ? msg.content
            : JSON.stringify(msg.content),
        };
      } else {
        return {
          role: 'user',
          content: typeof msg.content === 'string'
            ? msg.content
            : this.convertMultiModalContent(msg.content),
        };
      }
    });

    // Add system message if provided separately
    if (request.systemPrompt && !messages.some(m => m.role === 'system')) {
      messages.unshift({
        role: 'system',
        content: request.systemPrompt,
      });
    }

    const deepseekRequest: any = {
      model: request.model,
      messages,
      max_tokens: request.maxTokens || 4096,
      temperature: request.temperature,
      top_p: request.topP,
    };

    // Add tools if present
    if (request.tools && request.tools.length > 0) {
      deepseekRequest.tools = request.tools.map(tool => ({
        type: 'function',
        function: {
          name: tool.name,
          description: tool.description,
          parameters: tool.parameters,
        },
      }));
    }

    return deepseekRequest;
  }

  private convertMultiModalContent(content: any[]): string {
    // DeepSeek currently doesn't support multimodal content
    // Extract text parts only
    const textParts = content.filter(item => item.type === 'text');
    return textParts.map(item => item.text).join('\n');
  }

  private convertResponse(data: any): GenerateResponse {
    const choice = data.choices?.[0];
    const message = choice?.message;

    const response: GenerateResponse = {
      text: message?.content || '',
      finishReason: this.convertFinishReason(choice?.finish_reason),
    };

    // Add usage data if available
    if (data.usage) {
      response.usage = {
        inputTokens: data.usage.prompt_tokens,
        outputTokens: data.usage.completion_tokens,
        totalTokens: data.usage.total_tokens,
      };
    }

    // Handle tool calls
    if (message?.tool_calls && message.tool_calls.length > 0) {
      const toolCalls: ToolCall[] = message.tool_calls.map((call: any) => ({
        id: call.id,
        name: call.function?.name,
        arguments: call.function?.arguments,
      }));
      response.toolCalls = toolCalls;
    }

    return response;
  }

  private convertStreamChunk(data: any): GenerateStreamChunk | null {
    const choice = data.choices?.[0];
    if (!choice) return null;

    const delta = choice.delta;
    if (!delta) return null;

    const chunk: GenerateStreamChunk = {
      delta: delta.content || '',
    };

    // Handle finish reason in the final chunk
    if (choice.finish_reason) {
      chunk.finishReason = this.convertFinishReason(choice.finish_reason);
    }

    // Handle tool calls in streaming
    if (delta.tool_calls) {
      // For tool calls, we set the first one as delta
      const firstCall = delta.tool_calls[0];
      if (firstCall) {
        chunk.toolCallDelta = {
          id: firstCall.id,
          name: firstCall.function?.name,
          arguments: firstCall.function?.arguments,
        };
      }
    }

    // Add usage data if available (usually in the final chunk)
    if (data.usage) {
      chunk.usage = {
        inputTokens: data.usage.prompt_tokens,
        outputTokens: data.usage.completion_tokens,
        totalTokens: data.usage.total_tokens,
      };
    }

    return chunk;
  }

  private convertFinishReason(finishReason: string): any {
    switch (finishReason) {
      case 'stop':
        return 'stop';
      case 'length':
        return 'length';
      case 'tool_calls':
        return 'tool_calls';
      case 'content_filter':
        return 'content_filter';
      default:
        return 'stop';
    }
  }
}