/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  ModelProvider,
  ModelInfo,
  ProviderConfig,
  LLMClient,
  GenerateRequest,
  GenerateResponse,
  GenerateStreamChunk,
} from './base.js';

/**
 * Google Gemini model provider
 */
export class GoogleProvider implements ModelProvider {
  readonly id = 'google';
  readonly name = 'google';

  readonly models: ModelInfo[] = [
    {
      id: 'gemini-2.5-pro',
      name: 'Gemini 2.5 Pro',
      description: 'Google\'s most capable multimodal model',
      contextWindow: 2097152,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 1.25,
        outputTokensPer1M: 2.50,
        currency: 'USD',
      },
    },
    {
      id: 'gemini-2.5-flash',
      name: 'Gemini 2.5 Flash',
      description: 'Fast and versatile multimodal model',
      contextWindow: 1048576,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.075,
        outputTokensPer1M: 0.30,
        currency: 'USD',
      },
    },
    {
      id: 'gemini-2.5-flash-lite',
      name: 'Gemini 2.5 Flash Lite',
      description: 'Ultra-fast and cost-effective model',
      contextWindow: 1048576,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.0375,
        outputTokensPer1M: 0.15,
        currency: 'USD',
      },
    },
    {
      id: 'gemini-1.5-pro',
      name: 'Gemini 1.5 Pro',
      description: 'Previous generation professional model',
      contextWindow: 2097152,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 1.25,
        outputTokensPer1M: 5.00,
        currency: 'USD',
      },
    },
    {
      id: 'gemini-1.5-flash',
      name: 'Gemini 1.5 Flash',
      description: 'Previous generation fast model',
      contextWindow: 1048576,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.075,
        outputTokensPer1M: 0.30,
        currency: 'USD',
      },
    },
  ];

  readonly defaultModel = 'gemini-2.5-pro';

  isConfigured(): boolean {
    return !!(process.env['GEMINI_API_KEY'] || process.env['GOOGLE_API_KEY']);
  }

  async initialize(config: ProviderConfig): Promise<void> {
    if (!config.apiKey && !this.isConfigured()) {
      throw new Error(
        'Google API key not found. Please set GEMINI_API_KEY or GOOGLE_API_KEY environment variable.'
      );
    }
  }

  async createClient(config: ProviderConfig): Promise<LLMClient> {
    // Import the existing Gemini client from the core package
    const genaiModule = await import('@google/genai');
    // @ts-ignore - GoogleGenerativeAI exists but TypeScript can't see it
    const GoogleGenerativeAI = genaiModule.GoogleGenerativeAI || genaiModule.default?.GoogleGenerativeAI;

    const apiKey = config.apiKey || process.env['GEMINI_API_KEY'] || process.env['GOOGLE_API_KEY'];

    if (!apiKey) {
      throw new Error('Google API key is required');
    }

    const genAI = new GoogleGenerativeAI(apiKey);

    return new GoogleClient(genAI, config);
  }

  validateModel(modelName: string): boolean {
    return this.models.some(model => model.id === modelName);
  }

  getModelInfo(modelName: string): ModelInfo | undefined {
    return this.models.find(model => model.id === modelName);
  }
}

/**
 * Google Gemini client wrapper
 */
export class GoogleClient implements LLMClient {
  readonly provider = 'google';
  private genAI: any;

  constructor(genAI: any, _config: ProviderConfig) {
    this.genAI = genAI;
  }

  async generate(request: GenerateRequest): Promise<GenerateResponse> {
    const model = this.genAI.getGenerativeModel({
      model: request.model,
      systemInstruction: request.systemPrompt,
    });

    // Convert messages to Gemini format
    const history = this.convertMessagesToHistory(request.messages);
    const chat = model.startChat({ history });

    // Get the last user message
    const lastMessage = request.messages[request.messages.length - 1];
    const prompt = typeof lastMessage.content === 'string'
      ? lastMessage.content
      : this.convertContentToText(lastMessage.content);

    const result = await chat.sendMessage(prompt);
    const response = await result.response;

    return {
      text: response.text(),
      usage: {
        inputTokens: response.usageMetadata?.promptTokenCount || 0,
        outputTokens: response.usageMetadata?.candidatesTokenCount || 0,
        totalTokens: response.usageMetadata?.totalTokenCount || 0,
      },
      finishReason: this.convertFinishReason(response.candidates?.[0]?.finishReason),
    };
  }

  async *generateStream(request: GenerateRequest): AsyncIterable<GenerateStreamChunk> {
    const model = this.genAI.getGenerativeModel({
      model: request.model,
      systemInstruction: request.systemPrompt,
    });

    const history = this.convertMessagesToHistory(request.messages);
    const chat = model.startChat({ history });

    const lastMessage = request.messages[request.messages.length - 1];
    const prompt = typeof lastMessage.content === 'string'
      ? lastMessage.content
      : this.convertContentToText(lastMessage.content);

    const result = await chat.sendMessageStream(prompt);

    for await (const chunk of result.stream) {
      const chunkText = chunk.text();
      if (chunkText) {
        yield { delta: chunkText };
      }
    }

    // Final response with usage info
    const finalResponse = await result.response;
    yield {
      usage: {
        inputTokens: finalResponse.usageMetadata?.promptTokenCount || 0,
        outputTokens: finalResponse.usageMetadata?.candidatesTokenCount || 0,
        totalTokens: finalResponse.usageMetadata?.totalTokenCount || 0,
      },
      finishReason: this.convertFinishReason(finalResponse.candidates?.[0]?.finishReason),
    };
  }

  supportsCapability(capability: keyof import('./base.js').ModelCapabilities): boolean {
    // Gemini models support all major capabilities
    return true;
  }

  private convertMessagesToHistory(messages: import('./base.js').Message[]): any[] {
    const history: any[] = [];

    for (const message of messages) {
      if (message.role === 'system') {
        // System messages are handled via systemInstruction
        continue;
      }

      const role = message.role === 'assistant' ? 'model' : 'user';
      const parts = typeof message.content === 'string'
        ? [{ text: message.content }]
        : this.convertContentToParts(message.content);

      history.push({
        role,
        parts,
      });
    }

    return history;
  }

  private convertContentToParts(content: any[]): any[] {
    return content.map(item => {
      if (item.type === 'text') {
        return { text: item.text };
      } else if (item.type === 'image') {
        return {
          inlineData: {
            mimeType: 'image/jpeg',
            data: item.imageUrl,
          },
        };
      }
      return { text: String(item) };
    });
  }

  private convertContentToText(content: any[]): string {
    return content
      .filter(item => item.type === 'text')
      .map(item => item.text)
      .join('\n');
  }

  private convertFinishReason(reason: string | undefined): GenerateResponse['finishReason'] {
    switch (reason) {
      case 'STOP':
        return 'stop';
      case 'MAX_TOKENS':
        return 'length';
      case 'SAFETY':
      case 'RECITATION':
        return 'error';
      default:
        return 'stop';
    }
  }
}