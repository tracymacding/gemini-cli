/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type { ModelProvider, ProviderConfig, LLMClient } from './base.js';
import { GoogleProvider } from './google.js';
import { AlibabaProvider } from './alibaba.js';
import { DeepSeekProvider } from './deepseek.js';

/**
 * Registry for managing model providers
 */
export class ModelProviderRegistry {
  private providers = new Map<string, ModelProvider>();
  private clients = new Map<string, LLMClient>();

  constructor() {
    // Register built-in providers
    this.registerProvider(new GoogleProvider());
    this.registerProvider(new AlibabaProvider());
    this.registerProvider(new DeepSeekProvider());
  }

  /**
   * Register a new model provider
   */
  registerProvider(provider: ModelProvider): void {
    this.providers.set(provider.id, provider);
  }

  /**
   * Get a provider by name
   */
  getProvider(name: string): ModelProvider | undefined {
    return this.providers.get(name);
  }

  /**
   * Get all registered providers
   */
  getAllProviders(): ModelProvider[] {
    return Array.from(this.providers.values());
  }

  /**
   * Get all available models across all providers
   */
  getAllModels(): { provider: string; models: import('./base.js').ModelInfo[] }[] {
    return Array.from(this.providers.values()).map(provider => ({
      provider: provider.name,
      models: provider.models,
    }));
  }

  /**
   * Find provider and model info by model name
   */
  resolveModel(modelName: string): {
    provider: ModelProvider;
    modelInfo: import('./base.js').ModelInfo;
  } | null {
    // First, try to find exact match
    for (const provider of this.providers.values()) {
      const modelInfo = provider.getModelInfo(modelName);
      if (modelInfo) {
        return { provider, modelInfo };
      }
    }

    // If no exact match, try provider:model format
    if (modelName.includes(':')) {
      const [providerName, modelId] = modelName.split(':', 2);
      const provider = this.getProvider(providerName);
      if (provider) {
        const modelInfo = provider.getModelInfo(modelId);
        if (modelInfo) {
          return { provider, modelInfo };
        }
      }
    }

    return null;
  }

  /**
   * Get or create a client for a specific provider
   */
  async getClient(providerName: string, config: ProviderConfig): Promise<LLMClient> {
    const cacheKey = `${providerName}:${JSON.stringify(config)}`;

    // Return cached client if available
    if (this.clients.has(cacheKey)) {
      return this.clients.get(cacheKey)!;
    }

    const provider = this.getProvider(providerName);
    if (!provider) {
      throw new Error(`Unknown provider: ${providerName}`);
    }

    await provider.initialize(config);
    const client = await provider.createClient(config);

    // Cache the client
    this.clients.set(cacheKey, client);

    return client;
  }

  /**
   * Get configured providers (those with valid configuration)
   */
  getConfiguredProviders(): ModelProvider[] {
    return Array.from(this.providers.values()).filter(provider =>
      provider.isConfigured()
    );
  }

  /**
   * Get default model for each configured provider
   */
  getDefaultModels(): { provider: string; model: string }[] {
    return this.getConfiguredProviders().map(provider => ({
      provider: provider.name,
      model: provider.defaultModel,
    }));
  }

  /**
   * Validate if a model is available
   */
  isModelAvailable(modelName: string): boolean {
    return this.resolveModel(modelName) !== null;
  }

  /**
   * Get provider name from model name
   */
  getProviderForModel(modelName: string): string | null {
    const resolved = this.resolveModel(modelName);
    return resolved?.provider.name || null;
  }

  /**
   * Generate a model to use for a request based on preferences
   */
  selectModel(preferences: ModelSelectionPreferences): string {
    const configuredProviders = this.getConfiguredProviders();

    if (configuredProviders.length === 0) {
      throw new Error('No model providers are configured');
    }

    // If specific model requested, validate and return
    if (preferences.model && this.isModelAvailable(preferences.model)) {
      return preferences.model;
    }

    // If provider preference specified
    if (preferences.provider) {
      const provider = configuredProviders.find(p => p.name === preferences.provider);
      if (provider) {
        return provider.defaultModel;
      }
    }

    // If capability required
    if (preferences.capability) {
      for (const provider of configuredProviders) {
        for (const model of provider.models) {
          if (model.capabilities[preferences.capability]) {
            return model.id;
          }
        }
      }
    }

    // Default: return first configured provider's default model
    return configuredProviders[0].defaultModel;
  }

  /**
   * Clear cached clients
   */
  clearCache(): void {
    this.clients.clear();
  }
}

/**
 * Model selection preferences
 */
export interface ModelSelectionPreferences {
  /** Specific model name */
  model?: string;

  /** Preferred provider */
  provider?: string;

  /** Required capability */
  capability?: keyof import('./base.js').ModelCapabilities;

  /** Cost preference */
  costPreference?: 'low' | 'medium' | 'high';

  /** Performance preference */
  performancePreference?: 'fast' | 'balanced' | 'quality';
}

// Global registry instance
export const modelRegistry = new ModelProviderRegistry();