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
  ToolCall,
} from './base.js';

/**
 * 阿里云通义千问模型提供者
 */
export class AlibabaProvider implements ModelProvider {
  readonly id = 'alibaba';
  readonly name = 'alibaba';

  readonly models: ModelInfo[] = [
    {
      id: 'qwen-max',
      name: '通义千问-Max',
      description: '最强大的通义千问模型，具备最强的理解和生成能力',
      contextWindow: 32768,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 40.00,
        outputTokensPer1M: 120.00,
        currency: 'CNY',
      },
    },
    {
      id: 'qwen-plus',
      name: '通义千问-Plus',
      description: '平衡性能与成本的通义千问模型，适合大多数应用场景',
      contextWindow: 131072,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 4.00,
        outputTokensPer1M: 12.00,
        currency: 'CNY',
      },
    },
    {
      id: 'qwen-turbo',
      name: '通义千问-Turbo',
      description: '高速响应的通义千问模型，适合实时对话和快速任务',
      contextWindow: 131072,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 2.00,
        outputTokensPer1M: 6.00,
        currency: 'CNY',
      },
    },
    {
      id: 'qwen-long',
      name: '通义千问-Long',
      description: '超长文本处理的通义千问模型，支持超大上下文',
      contextWindow: 1000000,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: false,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 0.50,
        outputTokensPer1M: 2.00,
        currency: 'CNY',
      },
    },
    {
      id: 'qwen-vl-plus',
      name: '通义千问-VL-Plus',
      description: '专门的视觉理解模型，具备强大的图像识别和分析能力',
      contextWindow: 32768,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: true,
        tools: false,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 8.00,
        outputTokensPer1M: 24.00,
        currency: 'CNY',
      },
    },
    {
      id: 'qwen-coder-plus',
      name: '通义千问-Coder-Plus',
      description: '专业的代码生成和理解模型，优化编程任务',
      contextWindow: 131072,
      maxOutputTokens: 8192,
      capabilities: {
        textGeneration: true,
        vision: false,
        tools: true,
        streaming: true,
        systemMessages: true,
      },
      pricing: {
        inputTokensPer1M: 4.00,
        outputTokensPer1M: 12.00,
        currency: 'CNY',
      },
    },
  ];

  readonly defaultModel = 'qwen-plus';

  isConfigured(): boolean {
    return !!(
      process.env['ALIBABA_DASHSCOPE_API_KEY'] ||
      process.env['DASHSCOPE_API_KEY'] ||
      process.env['QWEN_API_KEY']
    );
  }

  async initialize(config: ProviderConfig): Promise<void> {
    if (!config.apiKey && !this.isConfigured()) {
      throw new Error(
        '阿里云DashScope API密钥未找到。请设置 ALIBABA_DASHSCOPE_API_KEY、DASHSCOPE_API_KEY 或 QWEN_API_KEY 环境变量。'
      );
    }
  }

  async createClient(config: ProviderConfig): Promise<LLMClient> {
    const apiKey = config.apiKey ||
      process.env['ALIBABA_DASHSCOPE_API_KEY'] ||
      process.env['DASHSCOPE_API_KEY'] ||
      process.env['QWEN_API_KEY'];

    if (!apiKey) {
      throw new Error('阿里云DashScope API密钥是必需的');
    }

    return new AlibabaClient({
      apiKey,
      endpoint: config.endpoint || 'https://dashscope.aliyuncs.com',
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
 * 阿里云DashScope API客户端
 */
export class AlibabaClient implements LLMClient {
  readonly provider = 'alibaba';
  private apiKey: string;
  private endpoint: string;

  constructor(config: { apiKey: string; endpoint: string }) {
    this.apiKey = config.apiKey;
    this.endpoint = config.endpoint;
  }

  async generate(request: GenerateRequest): Promise<GenerateResponse> {
    const alibabaRequest = this.convertRequest(request);

    const response = await fetch(`${this.endpoint}/api/v1/services/aigc/text-generation/generation`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`,
        'X-DashScope-SSE': 'disable',
      },
      body: JSON.stringify(alibabaRequest),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`阿里云DashScope API错误: ${response.status} - ${error}`);
    }

    const data = await response.json();
    return this.convertResponse(data);
  }

  async *generateStream(request: GenerateRequest): AsyncIterable<GenerateStreamChunk> {
    const alibabaRequest = {
      ...this.convertRequest(request),
      parameters: {
        ...this.convertRequest(request).parameters,
        incremental_output: true,
      },
    };

    const response = await fetch(`${this.endpoint}/api/v1/services/aigc/text-generation/generation`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.apiKey}`,
        'X-DashScope-SSE': 'enable',
        'Accept': 'text/event-stream',
      },
      body: JSON.stringify(alibabaRequest),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`阿里云DashScope流式API错误: ${response.status} - ${error}`);
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('无法获取响应流读取器');
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
              // 忽略无效的JSON块
            }
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  supportsCapability(capability: keyof import('./base.js').ModelCapabilities): boolean {
    // 根据能力返回支持状态
    switch (capability) {
      case 'textGeneration':
      case 'streaming':
      case 'systemMessages':
        return true;
      case 'vision':
      case 'tools':
        // 部分模型支持，需要根据具体模型判断
        return true;
      default:
        return false;
    }
  }

  private convertRequest(request: GenerateRequest): any {
    const messages = request.messages.filter(msg => msg.role !== 'system');
    const systemMessage = request.messages.find(msg => msg.role === 'system')?.content || request.systemPrompt;

    // 构建消息格式
    const formattedMessages = messages.map(msg => {
      // 处理工具响应消息
      if (msg.role === 'tool' && msg.toolCallId) {
        return {
          role: 'tool',
          tool_call_id: msg.toolCallId,
          content: typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content),
        };
      } else if (msg.role === 'assistant') {
        // 检查是否有工具调用
        if (msg.toolCalls && msg.toolCalls.length > 0) {
          return {
            role: 'assistant',
            content: msg.content || '',
            tool_calls: msg.toolCalls.map(tc => ({
              id: tc.id,
              type: 'function',
              function: {
                name: tc.name,
                arguments: typeof tc.arguments === 'string' ? tc.arguments : JSON.stringify(tc.arguments),
              }
            }))
          };
        }
        return {
          role: 'assistant',
          content: typeof msg.content === 'string' ? msg.content : this.convertMultiModalContent(msg.content),
        };
      } else {
        return {
          role: 'user',
          content: typeof msg.content === 'string' ? msg.content : this.convertMultiModalContent(msg.content),
        };
      }
    });

    // 如果有系统消息，添加到开头
    if (systemMessage) {
      formattedMessages.unshift({
        role: 'system',
        content: systemMessage,
      });
    }

    const alibabaRequest: any = {
      model: request.model,
      input: {
        messages: formattedMessages,
      },
      parameters: {
        result_format: 'message',
        max_tokens: request.maxTokens || 4096,
      },
    };

    if (request.temperature !== undefined) {
      alibabaRequest.parameters.temperature = request.temperature;
    }

    if (request.topP !== undefined) {
      alibabaRequest.parameters.top_p = request.topP;
    }

    // 工具调用支持
    if (request.tools && request.tools.length > 0) {
      alibabaRequest.parameters.tools = request.tools.map(tool => ({
        type: 'function',
        function: {
          name: tool.name,
          description: tool.description,
          parameters: tool.parameters,
        },
      }));
    }

    return alibabaRequest;
  }

  private convertMultiModalContent(content: any[]): any {
    // 处理多模态内容（文本 + 图像）
    const textParts = content.filter(item => item.type === 'text');
    const imageParts = content.filter(item => item.type === 'image');

    if (imageParts.length > 0) {
      // 对于支持图像的模型，返回多模态格式
      return content.map(item => {
        if (item.type === 'text') {
          return { text: item.text };
        } else if (item.type === 'image') {
          return {
            image: item.imageUrl.startsWith('data:') ?
              item.imageUrl :
              `data:image/jpeg;base64,${item.imageUrl}`,
          };
        }
        return item;
      });
    }

    // 只有文本内容
    return textParts.map(item => item.text).join('\n');
  }

  private convertResponse(data: any): GenerateResponse {
    if (data.code && data.code !== '200') {
      throw new Error(`阿里云API错误: ${data.code} - ${data.message}`);
    }

    const output = data.output;
    const choice = output?.choices?.[0];
    const message = choice?.message;

    const response: GenerateResponse = {
      text: message?.content || '',
      finishReason: this.convertFinishReason(choice?.finish_reason),
    };

    if (data.usage) {
      response.usage = {
        inputTokens: data.usage.input_tokens || 0,
        outputTokens: data.usage.output_tokens || 0,
        totalTokens: (data.usage.input_tokens || 0) + (data.usage.output_tokens || 0),
      };
    }

    // 处理工具调用
    if (message?.tool_calls && message.tool_calls.length > 0) {
      const toolCalls: ToolCall[] = message.tool_calls.map((call: any) => ({
        id: call.id || `call_${Date.now()}`,
        name: call.function?.name || call.name,
        arguments: call.function?.arguments || call.arguments,
      }));
      response.toolCalls = toolCalls;
    }

    return response;
  }

  private convertStreamChunk(event: any): GenerateStreamChunk | null {
    if (event.code && event.code !== '200') {
      throw new Error(`流式响应错误: ${event.code} - ${event.message}`);
    }

    const output = event.output;
    const choice = output?.choices?.[0];

    if (choice?.delta?.content) {
      return { delta: choice.delta.content };
    }

    if (choice?.finish_reason) {
      const result: GenerateStreamChunk = {
        finishReason: this.convertFinishReason(choice.finish_reason),
      };

      if (event.usage) {
        result.usage = {
          inputTokens: event.usage.input_tokens || 0,
          outputTokens: event.usage.output_tokens || 0,
          totalTokens: (event.usage.input_tokens || 0) + (event.usage.output_tokens || 0),
        };
      }

      return result;
    }

    return null;
  }

  private convertFinishReason(finishReason: string): GenerateResponse['finishReason'] {
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