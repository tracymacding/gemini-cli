/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Base interface for LLM model providers
 */
export interface ModelProvider {
  /** The provider ID (e.g., 'google', 'alibaba') */
  readonly id: string;

  /** The provider name (e.g., 'google', 'alibaba') */
  readonly name: string;

  /** Available models for this provider */
  readonly models: ModelInfo[];

  /** Default model for this provider */
  readonly defaultModel: string;

  /** Check if the provider is properly configured */
  isConfigured(): boolean;

  /** Initialize the provider with configuration */
  initialize(config: ProviderConfig): Promise<void>;

  /** Create a client instance for making requests */
  createClient(config: ProviderConfig): Promise<LLMClient>;

  /** Validate model name for this provider */
  validateModel(modelName: string): boolean;

  /** Get model information */
  getModelInfo(modelName: string): ModelInfo | undefined;
}

/**
 * Model information
 */
export interface ModelInfo {
  /** Model identifier */
  id: string;

  /** Human-readable model name */
  name: string;

  /** Model description */
  description?: string;

  /** Context window size */
  contextWindow?: number;

  /** Maximum output tokens */
  maxOutputTokens?: number;

  /** Model capabilities */
  capabilities: ModelCapabilities;

  /** Cost information */
  pricing?: ModelPricing;
}

/**
 * Model capabilities
 */
export interface ModelCapabilities {
  /** Supports text generation */
  textGeneration: boolean;

  /** Supports vision/image understanding */
  vision: boolean;

  /** Supports function calling/tools */
  tools: boolean;

  /** Supports streaming responses */
  streaming: boolean;

  /** Supports system messages */
  systemMessages: boolean;
}

/**
 * Model pricing information
 */
export interface ModelPricing {
  /** Input tokens cost per 1M tokens */
  inputTokensPer1M?: number;

  /** Output tokens cost per 1M tokens */
  outputTokensPer1M?: number;

  /** Currency (e.g., 'USD') */
  currency: string;
}

/**
 * Provider configuration
 */
export interface ProviderConfig {
  /** API key or authentication token */
  apiKey?: string;

  /** API endpoint URL */
  endpoint?: string;

  /** Additional configuration options */
  options?: Record<string, unknown>;
}

/**
 * Generic LLM client interface
 */
export interface LLMClient {
  /** Provider this client belongs to */
  readonly provider: string;

  /** Generate text completion */
  generate(request: GenerateRequest): Promise<GenerateResponse>;

  /** Generate streaming text completion */
  generateStream(request: GenerateRequest): AsyncIterable<GenerateStreamChunk>;

  /** Check if model supports specific capability */
  supportsCapability(capability: keyof ModelCapabilities): boolean;
}

/**
 * Generate request
 */
export interface GenerateRequest {
  /** Model to use */
  model: string;

  /** Messages/conversation history */
  messages: Message[];

  /** System prompt */
  systemPrompt?: string;

  /** Maximum tokens to generate */
  maxTokens?: number;

  /** Temperature for randomness */
  temperature?: number;

  /** Top-p for nucleus sampling */
  topP?: number;

  /** Tools/functions available */
  tools?: Tool[];

  /** Additional parameters */
  parameters?: Record<string, unknown>;
}

/**
 * Message in conversation
 */
export interface Message {
  /** Message role */
  role: 'user' | 'assistant' | 'system' | 'tool';

  /** Message content */
  content: string | MessageContent[];

  /** Tool calls (for assistant messages) */
  toolCalls?: ToolCall[];

  /** Tool call ID (for tool messages) */
  toolCallId?: string;
}

/**
 * Multi-modal message content
 */
export interface MessageContent {
  type: 'text' | 'image';
  text?: string;
  imageUrl?: string;
}

/**
 * Tool definition
 */
export interface Tool {
  /** Tool name */
  name: string;

  /** Tool description */
  description: string;

  /** Tool parameters schema */
  parameters: Record<string, unknown>;
}

/**
 * Tool call
 */
export interface ToolCall {
  /** Call ID */
  id: string;

  /** Tool name */
  name: string;

  /** Tool arguments */
  arguments: Record<string, unknown>;
}

/**
 * Generate response
 */
export interface GenerateResponse {
  /** Generated text */
  text: string;

  /** Tool calls made */
  toolCalls?: ToolCall[];

  /** Usage statistics */
  usage?: TokenUsage;

  /** Finish reason */
  finishReason?: 'stop' | 'length' | 'tool_calls' | 'error' | 'content_filter';
}

/**
 * Streaming chunk
 */
export interface GenerateStreamChunk {
  /** Delta text */
  delta?: string;

  /** Tool call delta */
  toolCallDelta?: Partial<ToolCall>;

  /** Usage statistics (final chunk) */
  usage?: TokenUsage;

  /** Finish reason (final chunk) */
  finishReason?: 'stop' | 'length' | 'tool_calls' | 'error' | 'content_filter';
}

/**
 * Token usage statistics
 */
export interface TokenUsage {
  /** Input tokens used */
  inputTokens: number;

  /** Output tokens generated */
  outputTokens: number;

  /** Total tokens */
  totalTokens: number;
}