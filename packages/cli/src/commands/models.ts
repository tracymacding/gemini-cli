/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { modelRegistry } from '@google/gemini-cli-core';

/**
 * Handle the --list-models command
 */
export function handleListModels(): void {
  console.log('🤖 Available Models\n');

  const allModels = modelRegistry.getAllModels();

  if (allModels.length === 0) {
    console.log('❌ No model providers are configured.');
    console.log('\nTo configure providers:');
    console.log('• Google Gemini: Set GEMINI_API_KEY environment variable');
    console.log('• Anthropic Claude: Set ANTHROPIC_API_KEY environment variable');
    console.log('• Alibaba Qwen: Set ALIBABA_DASHSCOPE_API_KEY environment variable');
    return;
  }

  const configuredProviders = modelRegistry.getConfiguredProviders();
  const unconfiguredProviders = allModels.filter(
    pm => !configuredProviders.some(cp => cp.name === pm.provider)
  );

  // Show configured providers
  for (const provider of configuredProviders) {
    const providerModels = allModels.find(pm => pm.provider === provider.name);
    if (!providerModels) continue;

    console.log(`\n📡 ${provider.name.toUpperCase()} (✅ Configured)`);
    console.log('─'.repeat(50));

    for (const model of providerModels.models) {
      const isDefault = model.id === provider.defaultModel;
      const defaultMark = isDefault ? ' (default)' : '';

      console.log(`\n🔹 ${model.id}${defaultMark}`);
      console.log(`   ${model.name}`);

      if (model.description) {
        console.log(`   ${model.description}`);
      }

      // Show capabilities
      const capabilities = [];
      if (model.capabilities.vision) capabilities.push('Vision');
      if (model.capabilities.tools) capabilities.push('Tools');
      if (model.capabilities.streaming) capabilities.push('Streaming');

      if (capabilities.length > 0) {
        console.log(`   Capabilities: ${capabilities.join(', ')}`);
      }

      // Show context window
      if (model.contextWindow) {
        const contextK = Math.floor(model.contextWindow / 1000);
        console.log(`   Context: ${contextK}K tokens`);
      }

      // Show pricing
      if (model.pricing) {
        const input = model.pricing.inputTokensPer1M;
        const output = model.pricing.outputTokensPer1M;
        console.log(`   Pricing: $${input}/$${output} per 1M tokens (in/out)`);
      }
    }
  }

  // Show unconfigured providers
  if (unconfiguredProviders.length > 0) {
    console.log('\n\n❌ Unconfigured Providers');
    console.log('─'.repeat(50));

    for (const providerModel of unconfiguredProviders) {
      console.log(`\n📡 ${providerModel.provider.toUpperCase()} (⚠️  Not Configured)`);

      const setup = getProviderSetupInstructions(providerModel.provider);
      if (setup) {
        console.log(`   Setup: ${setup}`);
      }

      console.log(`   Available models: ${providerModel.models.length}`);

      // Show just the model names for unconfigured providers
      const modelNames = providerModel.models.map(m => m.id).join(', ');
      console.log(`   Models: ${modelNames}`);
    }
  }

  console.log('\n\n💡 Usage Examples:');
  console.log('─'.repeat(50));

  if (configuredProviders.length > 0) {
    const firstProvider = configuredProviders[0];
    const firstModel = allModels.find(pm => pm.provider === firstProvider.name)?.models[0];

    if (firstModel) {
      console.log(`# Use specific model:`);
      console.log(`gemini -m ${firstModel.id} -p "Hello!"`);

      console.log(`\n# Use provider:model format:`);
      console.log(`gemini -m ${firstProvider.name}:${firstModel.id} -p "Hello!"`);

      console.log(`\n# Use provider preference:`);
      console.log(`gemini --provider ${firstProvider.name} -p "Hello!"`);
    }
  }

  // Show example for each configured provider
  for (const provider of configuredProviders) {
    const providerModels = allModels.find(pm => pm.provider === provider.name);
    if (providerModels && providerModels.models.length > 0) {
      const defaultModel = providerModels.models.find(m => m.id === provider.defaultModel);
      if (defaultModel) {
        console.log(`\n# Use ${provider.name} default (${defaultModel.id}):`);
        console.log(`gemini --provider ${provider.name} -p "Hello!"`);
      }
    }
  }

  console.log('\n');
}

/**
 * Get setup instructions for a provider
 */
function getProviderSetupInstructions(providerName: string): string | null {
  switch (providerName) {
    case 'google':
      return 'Set GEMINI_API_KEY environment variable';
    case 'anthropic':
      return 'Set ANTHROPIC_API_KEY environment variable';
    case 'alibaba':
      return 'Set ALIBABA_DASHSCOPE_API_KEY environment variable';
    case 'deepseek':
      return 'Set DEEPSEEK_API_KEY environment variable';
    default:
      return null;
  }
}

/**
 * Validate and resolve model name
 */
export function validateAndResolveModel(modelName: string): {
  provider: string;
  model: string;
  fullName: string;
} {
  const resolved = modelRegistry.resolveModel(modelName);

  if (!resolved) {
    console.error(`❌ Model '${modelName}' not found.`);
    console.error('\nRun `gemini --list-models` to see available models.');
    process.exit(1);
  }

  return {
    provider: resolved.provider.name,
    model: resolved.modelInfo.id,
    fullName: `${resolved.provider.name}:${resolved.modelInfo.id}`,
  };
}

/**
 * Get model selection based on CLI arguments
 */
export function getModelSelection(args: {
  model?: string;
  provider?: string;
}): string {
  // If both model and provider specified, validate compatibility
  if (args.model && args.provider) {
    const provider = modelRegistry.getProvider(args.provider);
    if (!provider) {
      console.error(`❌ Unknown provider: ${args.provider}`);
      const availableProviders = modelRegistry.getAllProviders().map(p => p.id).join(', ');
      console.error(`Available providers: ${availableProviders}`);
      process.exit(1);
    }

    if (!provider.isConfigured()) {
      const setup = getProviderSetupInstructions(provider.name);
      console.error(`❌ Provider '${provider.name}' is not configured.`);
      if (setup) {
        console.error(`Setup: ${setup}`);
      }
      process.exit(1);
    }

    // Check if the model belongs to the specified provider
    const modelInfo = provider.getModelInfo(args.model);
    if (!modelInfo) {
      console.error(`❌ Model '${args.model}' is not available for provider '${args.provider}'.`);
      console.error(`Available models for ${args.provider}: ${provider.models.map(m => m.id).join(', ')}`);
      process.exit(1);
    }

    // For non-Google providers, return provider:model format for multi-provider support
    // For Google providers, return just the model name for current compatibility
    if (args.provider === 'google') {
      return args.model;
    } else {
      return `${args.provider}:${args.model}`;
    }
  }

  // If specific model provided without provider, use it
  if (args.model) {
    const validation = validateAndResolveModel(args.model);

    // For Google models, return just the model name for current compatibility
    if (validation.provider === 'google') {
      return validation.model;
    } else {
      return validation.fullName; // Returns provider:model format
    }
  }

  // If provider specified without model, use its default model
  if (args.provider) {
    const provider = modelRegistry.getProvider(args.provider);
    if (!provider) {
      console.error(`❌ Unknown provider: ${args.provider}`);
      const availableProviders = modelRegistry.getAllProviders().map(p => p.id).join(', ');
      console.error(`Available providers: ${availableProviders}`);
      process.exit(1);
    }

    if (!provider.isConfigured()) {
      const setup = getProviderSetupInstructions(provider.name);
      console.error(`❌ Provider '${provider.name}' is not configured.`);
      if (setup) {
        console.error(`Setup: ${setup}`);
      }
      process.exit(1);
    }

    // For Google providers, return just the model name for current compatibility
    if (args.provider === 'google') {
      return provider.defaultModel;
    } else {
      return `${args.provider}:${provider.defaultModel}`;
    }
  }

  // Use first configured provider's default model
  const defaultModels = modelRegistry.getDefaultModels();

  if (defaultModels.length === 0) {
    console.error('❌ No model providers are configured.');
    console.error('\nTo configure providers:');
    console.error('• Google Gemini: Set GEMINI_API_KEY environment variable');
    console.error('• Anthropic Claude: Set ANTHROPIC_API_KEY environment variable');
    console.error('• Alibaba Qwen: Set ALIBABA_DASHSCOPE_API_KEY environment variable');
    process.exit(1);
  }

  // Use first configured provider's default model
  // For Google providers, return just the model name for current compatibility
  if (defaultModels[0].provider === 'google') {
    return defaultModels[0].model;
  } else {
    return `${defaultModels[0].provider}:${defaultModels[0].model}`;
  }
}