/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import { getErrorMessage } from '@google/gemini-cli-core';

/**
 * DeepSeek authentication status
 */
export interface DeepSeekAuthStatus {
  isConfigured: boolean;
  keySource?: 'env' | 'file';
  keyPreview?: string;
  error?: string;
}

/**
 * Check the current DeepSeek authentication status
 */
export function checkDeepSeekAuth(): DeepSeekAuthStatus {
  try {
    // Check for DeepSeek API key in environment variables
    const apiKey = process.env['DEEPSEEK_API_KEY'] || process.env['DEEPSEEK_KEY'];

    if (!apiKey) {
      return {
        isConfigured: false,
        error: 'No DeepSeek API key found in environment variables',
      };
    }

    // Validate API key format (DeepSeek keys typically start with 'sk-')
    if (!apiKey.startsWith('sk-')) {
      return {
        isConfigured: false,
        error: 'Invalid DeepSeek API key format (should start with "sk-")',
      };
    }

    return {
      isConfigured: true,
      keySource: 'env',
      keyPreview: `${apiKey.substring(0, 8)}...${apiKey.substring(apiKey.length - 4)}`,
    };
  } catch (error) {
    return {
      isConfigured: false,
      error: getErrorMessage(error),
    };
  }
}

/**
 * Display DeepSeek authentication status and setup instructions
 */
export function displayDeepSeekAuthStatus(): void {
  console.log('🔍 DeepSeek Authentication Status\n');

  const status = checkDeepSeekAuth();

  if (status.isConfigured) {
    console.log('✅ DeepSeek is configured and ready to use!');
    console.log(`🔑 API Key source: ${status.keySource}`);
    console.log(`👀 Key preview: ${status.keyPreview}`);

    console.log('\n💡 Available models:');
    console.log('   • deepseek-chat - Advanced conversational AI (default)');
    console.log('   • deepseek-coder - Specialized for code generation');
    console.log('   • deepseek-reasoner - Advanced reasoning with chain-of-thought');

    console.log('\n🚀 Usage examples:');
    console.log('   gemini -m deepseek:deepseek-chat "Explain quantum computing"');
    console.log('   gemini -m deepseek-chat "Write a Python function to sort a list"');
    console.log('   gemini -m deepseek-coder "Review this code for bugs"');
    console.log('   gemini -m deepseek-reasoner "Solve this logic puzzle step by step"');

    console.log('\n💰 Pricing (per 1M tokens):');
    console.log('   • Chat/Coder: $0.14 input / $0.28 output');
    console.log('   • Reasoner: $0.55 input / $2.19 output');
  } else {
    console.log('❌ DeepSeek is not configured');
    if (status.error) {
      console.log(`   Error: ${status.error}`);
    }

    console.log('\n📋 Setup Instructions:');
    console.log('1. Get your DeepSeek API key:');
    console.log('   • Visit: https://platform.deepseek.com/');
    console.log('   • Sign up or log in to your account');
    console.log('   • Navigate to API Keys section');
    console.log('   • Create a new API key');

    console.log('\n2. Set your API key as an environment variable:');
    console.log('   export DEEPSEEK_API_KEY="sk-your-api-key-here"');
    console.log('   # Or alternatively:');
    console.log('   export DEEPSEEK_KEY="sk-your-api-key-here"');

    console.log('\n3. Add to your shell profile for persistence:');
    console.log('   echo \'export DEEPSEEK_API_KEY="sk-your-api-key-here"\' >> ~/.bashrc');
    console.log('   # Or for zsh users:');
    console.log('   echo \'export DEEPSEEK_API_KEY="sk-your-api-key-here"\' >> ~/.zshrc');

    console.log('\n4. Reload your shell or run:');
    console.log('   source ~/.bashrc  # or ~/.zshrc');

    console.log('\n5. Verify the setup:');
    console.log('   gemini auth deepseek');

    console.log('\n💡 You can also use a .env file in your project root:');
    console.log('   DEEPSEEK_API_KEY=sk-your-api-key-here');
  }

  console.log('\n📚 Documentation:');
  console.log('   • DeepSeek API: https://platform.deepseek.com/api-docs');
  console.log('   • Model capabilities: https://platform.deepseek.com/');
  console.log('   • Pricing: https://platform.deepseek.com/pricing');
}

/**
 * Validate a DeepSeek API key format
 */
export function validateDeepSeekApiKey(apiKey: string): boolean {
  if (!apiKey || typeof apiKey !== 'string') {
    return false;
  }

  // DeepSeek API keys typically follow the pattern sk-...
  if (!apiKey.startsWith('sk-')) {
    return false;
  }

  // Check minimum length (DeepSeek keys are usually at least 32 characters)
  if (apiKey.length < 32) {
    return false;
  }

  return true;
}

/**
 * Test DeepSeek API connectivity
 */
export async function testDeepSeekConnection(): Promise<{
  success: boolean;
  error?: string;
  models?: string[];
}> {
  try {
    const apiKey = process.env['DEEPSEEK_API_KEY'] || process.env['DEEPSEEK_KEY'];

    if (!apiKey) {
      return {
        success: false,
        error: 'No API key found',
      };
    }

    // Test with a simple API call
    const response = await fetch('https://api.deepseek.com/v1/models', {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      return {
        success: false,
        error: `API Error: ${response.status} - ${errorText}`,
      };
    }

    const data = await response.json();
    const models = data.data?.map((model: any) => model.id) || [];

    return {
      success: true,
      models,
    };
  } catch (error) {
    return {
      success: false,
      error: getErrorMessage(error),
    };
  }
}