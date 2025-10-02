/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import {
  GenerateContentResponse
} from '@google/genai';
import type {
  GenerateContentParameters,
  CountTokensParameters,
  EmbedContentParameters,

  CountTokensResponse,
  EmbedContentResponse} from '@google/genai';
import type { ContentGenerator } from './contentGenerator.js';

export interface DeepSeekConfig {
  apiKey: string;
  baseURL?: string;
  model: string;
}

/**
 * ContentGenerator implementation for DeepSeek API
 */
export class DeepSeekContentGenerator implements ContentGenerator {
  private config: DeepSeekConfig;
  private toolCallAccumulator: Map<
    string,
    {
      id?: string;
      name?: string;
      arguments: string;
    }
  > = new Map();
  private toolCallIdMap: Map<string, string> = new Map();
  private apiUrl: string;

  constructor(config: DeepSeekConfig) {
    this.config = {
      baseURL: 'https://api.deepseek.com/v1/chat/completions',
      ...config,
    };
    this.apiUrl =
      this.config.baseURL || 'https://api.deepseek.com/v1/chat/completions';
  }

  async generateContent(
    request: GenerateContentParameters,
    _userPromptId: string,
  ): Promise<GenerateContentResponse> {
    const requestBody = this.convertToDeepSeekRequest(request);

    const response = await fetch(this.apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.config.apiKey}`,
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`DeepSeek API Error: ${response.status} ${errorText}`);
    }

    const data = await response.json();
    return this.convertFromDeepSeekResponse(data);
  }

  async generateContentStream(
    request: GenerateContentParameters,
    _userPromptId: string,
  ): Promise<AsyncGenerator<GenerateContentResponse>> {
    const requestBody = {
      ...this.convertToDeepSeekRequest(request),
      stream: true,
    };

    const response = await fetch(this.apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${this.config.apiKey}`,
        Accept: 'text/event-stream',
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      // Log the actual request that failed
      console.error(
        '[DeepSeek ERROR] Request messages:',
        JSON.stringify(requestBody.messages, null, 2),
      );
      throw new Error(
        `DeepSeek Streaming API Error: ${response.status} ${errorText}`,
      );
    }

    return this.parseSSEStream(response);
  }

  private async *parseSSEStream(
    response: Response,
  ): AsyncGenerator<GenerateContentResponse> {
    if (!response.body) {
      throw new Error('Response body is null');
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    // Reset accumulator for new stream
    this.toolCallAccumulator.clear();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.trim() === '') continue;

          if (line.startsWith('data: ')) {
            const dataStr = line.slice(6).trim();

            if (dataStr === '[DONE]') {
              return;
            }

            try {
              const data = JSON.parse(dataStr);
              const geminiResponse = this.convertStreamChunkToGemini(data);
              if (geminiResponse) {
                yield geminiResponse;
              }
            } catch (parseError) {
              console.error(
                'Failed to parse SSE data:',
                parseError,
                'Data:',
                dataStr,
              );
              continue;
            }
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  async countTokens(
    request: CountTokensParameters,
  ): Promise<CountTokensResponse> {
    // DeepSeek doesn't have a dedicated token counting API
    // We'll estimate based on content length
    const content = this.extractTextFromRequest(request);
    const estimatedTokens = Math.ceil(content.length / 4); // Rough estimation

    return {
      totalTokens: estimatedTokens,
    };
  }

  async embedContent(
    _request: EmbedContentParameters,
  ): Promise<EmbedContentResponse> {
    throw new Error('Embedding is not supported by DeepSeek provider');
  }

  private convertToDeepSeekRequest(request: GenerateContentParameters): any {
    const messages: Array<{
      role: string;
      content: any;
      tool_calls?: any;
      tool_call_id?: string;
    }> = [];

    // Convert system instruction
    if (request.config?.systemInstruction) {
      const systemContent = this.extractTextFromPart(
        request.config.systemInstruction,
      );
      if (systemContent) {
        messages.push({ role: 'system', content: systemContent });
      }
    }

    // Convert contents
    const contentsArray = Array.isArray(request.contents)
      ? request.contents
      : [request.contents];

    // Debug logging
    if (process.env['DEBUG']) {
      console.error(
        '[DeepSeek] Converting contents:',
        JSON.stringify(contentsArray, null, 2),
      );
    }

    for (const content of contentsArray) {
      // Handle case where content is an array of Parts (function responses)
      if (Array.isArray(content)) {
        for (const part of content) {
          if (part.functionResponse) {
            // Prioritize ID from response, then from map, then fallback
            const functionName = part.functionResponse.name || '';
            const toolCallId: string =
              part.functionResponse.id ||
              this.toolCallIdMap.get(functionName) ||
              `call_${functionName}`;
            messages.push({
              role: 'tool',
              tool_call_id: toolCallId,
              content: JSON.stringify(part.functionResponse.response),
            });
          }
        }
        continue;
      }

      if (content && typeof content === 'object' && 'role' in content) {
        const role = content.role === 'model' ? 'assistant' : 'user';

        // Handle parts
        for (const part of content.parts || []) {
          if (part.text) {
            messages.push({ role, content: part.text });
          } else if (part.functionCall) {
            // Convert function call to DeepSeek format
            // Use the actual ID if available (from history), otherwise generate one
            const toolCallId =
              part.functionCall.id ||
              `call_${part.functionCall.name}_${Math.random().toString(36).substr(2, 9)}`;

            // Save the ID for later use when processing functionResponse
            if (part.functionCall.name) {
              this.toolCallIdMap.set(part.functionCall.name, toolCallId);
            }

            messages.push({
              role: 'assistant',
              content: null,
              tool_calls: [
                {
                  id: toolCallId,
                  type: 'function',
                  function: {
                    name: part.functionCall.name,
                    arguments: JSON.stringify(part.functionCall.args || {}),
                  },
                },
              ],
            });
          } else if (part.functionResponse) {
            // Convert function response to DeepSeek format
            // Prioritize ID from response, then from map, then fallback
            const functionName = part.functionResponse.name || '';
            const toolCallId: string =
              part.functionResponse.id ||
              this.toolCallIdMap.get(functionName) ||
              `call_${functionName}`;
            messages.push({
              role: 'tool',
              tool_call_id: toolCallId,
              content: JSON.stringify(part.functionResponse.response),
            });
          }
        }
      } else {
        // This is a PartUnion, treat as user content
        const text = this.extractTextFromPart(content);
        if (text) {
          messages.push({ role: 'user', content: text });
        }
      }
    }

    const requestBody: any = {
      model: this.config.model,
      messages,
      max_tokens: request.config?.maxOutputTokens || 4096,
      temperature: request.config?.temperature,
      top_p: request.config?.topP,
    };

    // Debug logging
    if (process.env['DEBUG']) {
      console.error(
        '[DeepSeek] Final messages:',
        JSON.stringify(messages, null, 2),
      );
    }

    // Add tools if present
    if (request.config?.tools && request.config.tools.length > 0) {
      const tools = request.config.tools.flatMap((tool: any) => (
          tool.functionDeclarations?.map((func: any) => {
            const parameters = func.parameters ??
              func.parametersJsonSchema ?? { type: 'object', properties: {} };
            return {
              type: 'function',
              function: {
                name: func.name,
                description: func.description,
                parameters,
              },
            };
          }) ?? []
        ));

      if (tools.length > 0) {
        requestBody.tools = tools;
      }
    }

    return requestBody;
  }

  private convertFromDeepSeekResponse(data: any): GenerateContentResponse {
    const choice = data.choices?.[0];
    if (!choice || !choice.message) {
      throw new Error('Invalid response from DeepSeek API');
    }

    const response = new GenerateContentResponse();
    const message = choice.message;
    const parts: any[] = [];

    // Handle text content
    if (message.content) {
      parts.push({ text: message.content });
    }

    // Handle tool calls
    if (message.tool_calls && message.tool_calls.length > 0) {
      for (const toolCall of message.tool_calls) {
        parts.push({
          functionCall: {
            id: toolCall.id,
            name: toolCall.function?.name,
            args: toolCall.function?.arguments
              ? JSON.parse(toolCall.function.arguments)
              : {},
          },
        });
      }
    }

    // Ensure valid parts
    const validParts = this.ensureValidParts(parts);

    response.candidates = [
      {
        content: {
          parts: validParts,
          role: 'model',
        },
        finishReason: this.mapFinishReason(choice.finish_reason) || 'STOP',
        index: 0,
      },
    ];

    // Add usage metadata
    if (data.usage) {
      response.usageMetadata = {
        promptTokenCount: data.usage.prompt_tokens || 0,
        candidatesTokenCount: data.usage.completion_tokens || 0,
        totalTokenCount: data.usage.total_tokens || 0,
      };
    }

    return response;
  }

  private convertStreamChunkToGemini(
    data: any,
  ): GenerateContentResponse | null {
    const choice = data.choices?.[0];
    if (!choice || !choice.delta) {
      return null;
    }

    const response = new GenerateContentResponse();
    const delta = choice.delta;
    const parts: any[] = [];

    // Handle text content
    if (delta.content) {
      parts.push({ text: delta.content });
    }

    // Handle tool calls with accumulation
    if (delta.tool_calls) {
      for (const toolCall of delta.tool_calls) {
        const toolCallId = `tool_${toolCall.index || 0}`;

        // Initialize or get existing accumulator
        if (!this.toolCallAccumulator.has(toolCallId)) {
          this.toolCallAccumulator.set(toolCallId, {
            id: undefined,
            name: undefined,
            arguments: '',
          });
        }

        const accumulator = this.toolCallAccumulator.get(toolCallId)!;

        // Accumulate tool call ID
        if (toolCall.id) {
          accumulator.id = toolCall.id;
        }

        // Accumulate function name
        if (toolCall.function?.name) {
          accumulator.name = toolCall.function.name;
        }

        // Accumulate function arguments
        if (toolCall.function?.arguments) {
          accumulator.arguments += toolCall.function.arguments;
        }

        // Try to parse accumulated arguments
        if (accumulator.name && accumulator.arguments.trim()) {
          try {
            const parsedArgs = JSON.parse(accumulator.arguments);
            parts.push({
              functionCall: {
                id: accumulator.id,
                name: accumulator.name,
                args: parsedArgs,
              },
            });
            // Clear the accumulator after successful parsing
            this.toolCallAccumulator.delete(toolCallId);
          } catch (_parseError) {
            // Arguments are not complete yet, continue accumulating
            // Don't add to parts yet
          }
        }
      }
    }

    if (parts.length === 0 && !choice.finish_reason) {
      return null;
    }

    response.candidates = [
      {
        content: {
          parts: parts.length > 0 ? parts : [{ text: '' }],
          role: 'model',
        },
        finishReason: choice.finish_reason
          ? this.mapFinishReason(choice.finish_reason)
          : undefined,
        index: 0,
      },
    ];

    // Add usage metadata if available
    if (data.usage) {
      response.usageMetadata = {
        promptTokenCount: data.usage.prompt_tokens || 0,
        candidatesTokenCount: data.usage.completion_tokens || 0,
        totalTokenCount: data.usage.total_tokens || 0,
      };
    }

    return response;
  }

  private extractTextFromPart(part: any): string {
    if (typeof part === 'string') {
      return part;
    }
    if (part && typeof part === 'object') {
      if (part.text) {
        return part.text;
      }
      if (part.parts) {
        return part.parts
          .map((p: any) => this.extractTextFromPart(p))
          .filter(Boolean)
          .join('\n');
      }
    }
    return '';
  }

  private extractTextFromRequest(request: any): string {
    let text = '';

    if (request.systemInstruction) {
      text += this.extractTextFromPart(request.systemInstruction) + '\n';
    }

    if (request.contents) {
      for (const content of request.contents) {
        if (content.parts) {
          text +=
            content.parts
              .map((part: any) => this.extractTextFromPart(part))
              .filter(Boolean)
              .join('\n') + '\n';
        }
      }
    }

    return text;
  }

  private ensureValidParts(parts: any[]): any[] {
    if (!parts || parts.length === 0) {
      return [{ text: ' ' }]; // Minimal valid part
    }

    const validParts = parts.filter((part) => {
      if (!part || Object.keys(part).length === 0) {
        return false;
      }
      // Remove parts with empty text (but keep functionCall and functionResponse)
      if (
        part.text !== undefined &&
        part.text === '' &&
        !part.functionCall &&
        !part.functionResponse
      ) {
        return false;
      }
      return true;
    });

    // If all parts were filtered out, return a minimal valid part
    if (validParts.length === 0) {
      return [{ text: ' ' }];
    }

    return validParts;
  }

  private mapFinishReason(reason: string | null | undefined): any {
    switch (reason) {
      case 'stop':
        return 'STOP';
      case 'length':
      case 'max_tokens':
        return 'MAX_TOKENS';
      case 'tool_calls':
        return 'STOP';
      case 'content_filter':
        return 'SAFETY';
      case null:
      case undefined:
      case '':
        return 'STOP';
      default:
        return 'STOP';
    }
  }
}
