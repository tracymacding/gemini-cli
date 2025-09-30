/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { CountTokensResponse, GenerateContentResponse, EmbedContentResponse } from '@google/genai';
import type { GenerateContentParameters, CountTokensParameters, EmbedContentParameters } from '@google/genai';
import type { ContentGenerator } from './contentGenerator.js';


export interface AlibabaConfig {
  apiKey: string;
  baseURL?: string;
  model: string;
}

/**
 * ContentGenerator implementation for Alibaba DashScope API
 */
export class AlibabaContentGenerator implements ContentGenerator {
  private config: AlibabaConfig;
  private apiUrl: string;
  private apiKey: string;

  constructor(config: AlibabaConfig) {
    this.config = {
      baseURL: 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation',
      ...config,
    };
    this.apiUrl = this.config.baseURL || 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation';
    this.apiKey = this.config.apiKey || '';
  }


  async generateContent(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<GenerateContentResponse> {
    const requestBody = this.convertToAlibabaRequest(request);


    const response = await fetch(this.config.baseURL!, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`,
        'X-DashScope-SSE': 'disable',
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Alibaba API Error: ${response.status} ${errorText}`);
    }

    const data = await response.json();


    return this.convertFromAlibabaResponse(data);
  }

  async generateContentStream(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<AsyncGenerator<GenerateContentResponse>> {
    try {
      // Use non-streaming API for now and convert to streaming format
      const response = await this.generateContent(request, userPromptId);

      // Ensure the response has the correct format
      if (!response.candidates || response.candidates.length === 0) {
        throw new Error('Invalid response structure from Alibaba API');
      }

      // Convert single response to async generator that mimics Gemini's streaming exactly
      const self = this;
      return (async function*() {
        // Create streaming response with proper Gemini-like structure
        const streamResponse = new GenerateContentResponse();
        streamResponse.candidates = response.candidates?.map(candidate => {
          // Ensure finishReason is always set and properly typed
          const finishReason = candidate.finishReason || 'STOP';


          return {
            ...candidate,
            finishReason: finishReason as any,
            // Ensure content structure matches Gemini format
            content: {
              ...candidate.content,
              parts: self.ensureValidParts(candidate.content?.parts || []),
              role: candidate.content?.role || 'model'
            }
          };
        });

        // Add usage metadata if not present (Gemini always includes this)
        if (!streamResponse.usageMetadata && response.usageMetadata) {
          streamResponse.usageMetadata = response.usageMetadata;
        } else if (!streamResponse.usageMetadata) {
          // Estimate usage metadata if missing
          const textContent = streamResponse.candidates?.[0]?.content?.parts
            ?.filter(part => part.text)
            ?.map(part => part.text)
            ?.join(' ') || '';

          streamResponse.usageMetadata = {
            promptTokenCount: Math.ceil(textContent.length / 4),
            candidatesTokenCount: Math.ceil(textContent.length / 4),
            totalTokenCount: Math.ceil(textContent.length / 2),
          };
        }

        yield streamResponse;
      })();
    } catch (error) {
      // Create an error response in the expected format
      const errorResponse = new GenerateContentResponse();
      const errorMessage = error instanceof Error ? error.message : String(error);
      errorResponse.candidates = [
        {
          content: {
            parts: [{ text: `Error: ${errorMessage}` }],
            role: 'model',
          },
          finishReason: 'STOP' as any,
          index: 0,
        },
      ];

      return (async function*() {
        yield errorResponse;
      })();
    }
  }

  // Implement real streaming HTTP request like Gemini's implementation
  async requestStreamingPost(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<AsyncGenerator<GenerateContentResponse>> {
    const requestBody = this.convertToAlibabaRequest(request);
    // Enable streaming for DashScope API
    requestBody.parameters = {
      ...requestBody.parameters,
      stream: true,
      incremental_output: true, // Each chunk contains only newly generated content
    };

    const response = await fetch(this.apiUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.apiKey}`,
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream', // Important for SSE
        'User-Agent': 'GeminiCLI/Alibaba',
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Alibaba Streaming API Error: ${response.status} ${errorText}`);
    }

    // Parse SSE stream similar to Gemini's implementation
    return this.parseSSEStream(response);
  }

  async parseSSEStream(response: Response): Promise<AsyncGenerator<GenerateContentResponse>> {
    if (!response.body) {
      throw new Error('Response body is null');
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    const self = this;
    return (async function*(): AsyncGenerator<GenerateContentResponse> {
      try {
        while (true) {
          const { done, value } = await reader.read();

          if (done) {
            break;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');

          // Keep the last incomplete line in the buffer
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.trim() === '') {
              continue; // Skip empty lines
            }

            if (line.startsWith('data: ')) {
              const dataStr = line.slice(6).trim();

              if (dataStr === '[DONE]') {
                return; // End of stream
              }

              try {
                const data = JSON.parse(dataStr);
                const geminiResponse = self.convertAlibabaToGeminiStreamResponse(data);
                if (geminiResponse) {
                  yield geminiResponse;
                }
              } catch (parseError) {
                console.error('Failed to parse SSE data:', parseError, 'Data:', dataStr);
                continue;
              }
            }
          }
        }
      } finally {
        reader.releaseLock();
      }
    })();
  }



  async countTokens(request: CountTokensParameters): Promise<CountTokensResponse> {
    // Alibaba doesn't have a dedicated token counting API
    // We'll estimate based on content length
    const content = this.extractTextFromRequest(request);
    const estimatedTokens = Math.ceil(content.length / 4); // Rough estimation: 4 chars per token

    return {
      totalTokens: estimatedTokens,
    };
  }

  async embedContent(request: EmbedContentParameters): Promise<EmbedContentResponse> {
    throw new Error('Embedding is not supported by Alibaba provider');
  }

  // Convert DashScope streaming response to Gemini format
  convertAlibabaToGeminiStreamResponse(data: any): GenerateContentResponse | null {
    if (!data || !data.output) {
      return null;
    }

    const response = new GenerateContentResponse();

    // DashScope streaming response structure:
    // {
    //   "output": {
    //     "text": "generated text chunk",
    //     "finish_reason": "null" | "stop" | "length"
    //   },
    //   "usage": {
    //     "output_tokens": 10,
    //     "input_tokens": 5,
    //     "total_tokens": 15
    //   }
    // }

    const outputText = data.output.text || '';
    const finishReason = data.output.finish_reason;

    response.candidates = [
      {
        content: {
          parts: [{ text: outputText }],
          role: 'model'
        },
        // Map DashScope finish reasons to Gemini format
        finishReason: this.mapDashScopeFinishReason(finishReason),
        index: 0,
      }
    ];

    // Add usage metadata if available
    if (data.usage) {
      response.usageMetadata = {
        promptTokenCount: data.usage.input_tokens || 0,
        candidatesTokenCount: data.usage.output_tokens || 0,
        totalTokenCount: data.usage.total_tokens || 0,
      };
    }

    return response;
  }

  // Map DashScope finish reasons to Gemini FinishReason format
  private mapDashScopeFinishReason(finishReason: string): any {
    switch (finishReason) {
      case 'stop':
        return 'STOP';
      case 'length':
        return 'MAX_TOKENS';
      case null:
      case 'null':
        return undefined; // Still generating
      default:
        return 'OTHER';
    }
  }

  convertToAlibabaRequest(request: GenerateContentParameters): any {
    const messages: Array<{ role: string; content: string | Array<any> }> = [];

    // Convert system instruction
    if (request.config?.systemInstruction) {
      const systemContent = this.extractTextFromPart(request.config.systemInstruction);
      if (systemContent) {
        messages.push({ role: 'system', content: systemContent });
      }
    }

    // Convert contents - handle ContentListUnion which can be Content | Content[] | PartUnion | PartUnion[]
    const contentsArray = Array.isArray(request.contents) ? request.contents : [request.contents];

    for (const content of contentsArray) {
      if (content && typeof content === 'object' && 'role' in content) {
        // This is a Content object
        const role = content.role === 'model' ? 'assistant' : 'user';

        // Handle tool calls and function responses
        const messageContent: Array<any> = [];
        let hasText = false;

        for (const part of content.parts || []) {
          if (part.text) {
            messageContent.push({ type: 'text', text: part.text });
            hasText = true;
          } else if (part.functionCall) {
            // Convert function call to Alibaba format
            // Generate a stable ID based on the function name
            const callId = `${part.functionCall.name}_${Math.random().toString(36).substr(2, 9)}`;
            messageContent.push({
              type: 'tool_use',
              id: callId,
              name: part.functionCall.name,
              input: part.functionCall.args || {}
            });
          } else if (part.functionResponse) {
            // Convert function response to Alibaba format
            // Note: We need to get the call ID from somewhere, but functionResponse doesn't have it
            // This is a limitation - we'll need to track call IDs separately
            messageContent.push({
              type: 'tool_result',
              // Use the function name as a fallback for matching
              tool_use_id: part.functionResponse.name,
              content: JSON.stringify(part.functionResponse.response)
            });
          }
        }

        if (messageContent.length > 0) {
          messages.push({
            role,
            content: messageContent.length === 1 && hasText && messageContent[0].type === 'text'
              ? messageContent[0].text
              : messageContent
          });
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
      input: { messages },
      parameters: {
        result_format: 'message',
        max_tokens: request.config?.maxOutputTokens || 2000,
        temperature: request.config?.temperature || 0.7,
        top_p: request.config?.topP || 0.8,
      },
    };

    // Add tools if present in the request using correct DashScope format
    if (request.config?.tools && request.config.tools.length > 0) {
      const tools = request.config.tools.flatMap((tool: any) => {
        return (
          tool.functionDeclarations?.map((func: any) => {
            const parameters =
              func.parameters ??
              func.parametersJsonSchema ??
              { type: 'object', properties: {} };

            return {
              type: 'function',
              function: {
                name: func.name,
                description: func.description,
                parameters,
              },
            };
          }) ?? []
        );
      });

      if (tools.length > 0) {
        // Try using the root level tools property instead of parameters.tools
        requestBody.tools = tools;
        requestBody.parameters = {
          ...requestBody.parameters,
          tools,
        };
        // Don't set tool_choice initially to see if that helps
      }
    }

    return requestBody;
  }

  convertFromAlibabaResponse(data: any): GenerateContentResponse {

    const choice = data.output?.choices?.[0];
    if (!choice || !choice.message) {
      throw new Error('Invalid response from Alibaba API');
    }

    const response = new GenerateContentResponse();
    const parts: Array<any> = [];

    // Handle different content types
    const messageContent = choice.message.content;


    if (typeof messageContent === 'string') {
      // Simple text response - don't try to parse tool call patterns
      // The DashScope API should return tool calls in a structured format
      parts.push({ text: messageContent });
    } else if (Array.isArray(messageContent)) {
      // Structured content with possible tool calls
      for (const item of messageContent) {
        if (item.type === 'text') {
          parts.push({ text: item.text });
        } else if (item.type === 'tool_use') {
          // Convert Alibaba tool use to Gemini function call format
          parts.push({
            functionCall: {
              name: item.name,
              args: item.input || {}
            }
          });
        }
      }
    } else if (messageContent && typeof messageContent === 'object') {
      // Handle object-based content
      if (messageContent.tool_calls) {
        // Handle tool calls in message content
        for (const toolCall of messageContent.tool_calls) {
          parts.push({
            functionCall: {
              name: toolCall.function?.name,
              args: toolCall.function?.arguments ? JSON.parse(toolCall.function.arguments) : {}
            }
          });
        }
      } else {
        // Fallback to text if no recognizable structure
        parts.push({ text: JSON.stringify(messageContent) });
      }
    }

    // Handle tool_calls at the message level (DashScope format)
    if (choice.message.tool_calls && choice.message.tool_calls.length > 0) {
      for (const toolCall of choice.message.tool_calls) {
        parts.push({
          functionCall: {
            name: toolCall.function?.name || toolCall.name,
            args: toolCall.function?.arguments ? JSON.parse(toolCall.function.arguments) : (toolCall.arguments || {})
          }
        });
      }
    }

    // Ensure all parts are valid
    const validParts = this.ensureValidParts(parts);

    // Check if any part contains a function call to set proper finish reason
    const hasFunctionCall = validParts.some(part => part.functionCall);
    const finishReason = hasFunctionCall ? 'STOP' : this.mapFinishReason(choice.finish_reason);


    response.candidates = [
      {
        content: {
          parts: validParts,
          role: 'model',
        },
        finishReason: finishReason || 'STOP', // Ensure we always have a finish reason
        index: 0,
      },
    ];

    if (data.usage) {
      response.usageMetadata = {
        promptTokenCount: data.usage.input_tokens || 0,
        candidatesTokenCount: data.usage.output_tokens || 0,
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
        return part.parts.map((p: any) => this.extractTextFromPart(p)).filter(Boolean).join('\n');
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
          text += content.parts.map((part: any) => this.extractTextFromPart(part)).filter(Boolean).join('\n') + '\n';
        }
      }
    }

    return text;
  }

  private ensureValidParts(parts: any[]): any[] {
    if (!parts || parts.length === 0) {
      return [{ text: ' ' }]; // Minimal valid part
    }

    const validParts = parts.filter(part => {
      // Remove empty or invalid parts
      if (!part || Object.keys(part).length === 0) {
        return false;
      }
      // Remove parts with empty text (but keep functionCall and functionResponse)
      if (part.text !== undefined && part.text === '' && !part.functionCall && !part.functionResponse) {
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
        return 'MAX_TOKENS';
      case 'content_filter':
        return 'SAFETY';
      case null:
      case undefined:
      case '':
        return 'STOP';
      default:
        return 'STOP'; // 默认返回STOP而不是OTHER，确保流处理正常
    }
  }

}
