/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  handleAlibabaLogin,
  handleAlibabaLogout,
  displayAlibabaAuthStatus,
  checkAlibabaAuthStatus
} from './auth-alibaba.js';
import {
  displayDeepSeekAuthStatus,
  checkDeepSeekAuth
} from './auth-deepseek.js';

/**
 * Main authentication command handler
 */
export async function authCommand(action: string): Promise<void> {
  switch (action) {
    case 'status':
      await handleOverallAuthStatus();
      break;

    case 'login-google':
      console.log('🔐 Google OAuth login...');
      console.log('💡 This will use the existing Google OAuth flow.');
      console.log('⚠️  Google OAuth is handled through the main authentication dialog.');
      console.log('   Please run: gemini (without parameters) and select Google OAuth from the auth menu.');
      break;


    case 'login-alibaba':
      await handleAlibabaLogin();
      break;

    case 'logout-alibaba':
      await handleAlibabaLogout();
      break;

    case 'status-alibaba':
      await displayAlibabaAuthStatus();
      break;

    case 'status-deepseek':
      displayDeepSeekAuthStatus();
      break;

    case 'deepseek':
      displayDeepSeekAuthStatus();
      break;

    default:
      console.error(`❌ Unknown auth action: ${action}`);
      console.log('\n💡 Available actions:');
      console.log('   • gemini auth status - Show overall authentication status');
      console.log('   • gemini auth login-google - Login with Google OAuth');
      console.log('   • gemini auth login-alibaba - Setup Alibaba DashScope API key');
      console.log('   • gemini auth logout-alibaba - Clear Alibaba configuration');
      console.log('   • gemini auth status-alibaba - Check Alibaba authentication status');
      console.log('   • gemini auth deepseek - Setup and check DeepSeek API key');
      console.log('   • gemini auth status-deepseek - Check DeepSeek authentication status');
      process.exit(1);
  }
}

/**
 * Display overall authentication status for all providers
 */
async function handleOverallAuthStatus(): Promise<void> {
  console.log('🔍 Overall Authentication Status\n');
  console.log('=' .repeat(50));

  // Check Google/Gemini authentication
  console.log('\n🔵 Google / Gemini:');
  const hasGeminiApiKey = !!(process.env['GEMINI_API_KEY'] || process.env['GOOGLE_API_KEY']);
  const hasVertexConfig = !!(process.env['GOOGLE_CLOUD_PROJECT'] && process.env['GOOGLE_CLOUD_LOCATION']);

  if (hasGeminiApiKey) {
    const keySource = process.env['GEMINI_API_KEY'] ? 'GEMINI_API_KEY' : 'GOOGLE_API_KEY';
    console.log(`   ✅ API Key configured (${keySource})`);
  } else {
    console.log('   ❌ No API key found');
  }

  if (hasVertexConfig) {
    console.log(`   ✅ Vertex AI configured (Project: ${process.env['GOOGLE_CLOUD_PROJECT']});`);
  } else {
    console.log('   ❌ Vertex AI not configured');
  }


  // Check Alibaba authentication
  console.log('\n🟠 Alibaba / 通义千问:');
  const alibabaStatus = await checkAlibabaAuthStatus();

  if (alibabaStatus.isAuthenticated && alibabaStatus.details) {
    console.log(`   ✅ Authenticated via ${alibabaStatus.method}`);
    console.log(`   🔑 API Key source: ${alibabaStatus.details.source}`);
    console.log(`   📏 Key length: ${alibabaStatus.details.keyLength} characters`);
  } else {
    console.log('   ❌ Not authenticated');
  }

  // Check DeepSeek authentication
  console.log('\n🔵 DeepSeek:');
  const deepseekStatus = checkDeepSeekAuth();

  if (deepseekStatus.isConfigured) {
    console.log(`   ✅ Configured`);
    console.log(`   🔑 API Key source: ${deepseekStatus.keySource}`);
    console.log(`   👀 Key preview: ${deepseekStatus.keyPreview}`);
  } else {
    console.log('   ❌ Not configured');
    if (deepseekStatus.error) {
      console.log(`   💬 ${deepseekStatus.error}`);
    }
  }

  // Summary and recommendations
  console.log('\n📋 Summary:');
  const googleAuth = hasGeminiApiKey || hasVertexConfig;
  const alibabaAuth = alibabaStatus.isAuthenticated;
  const deepseekAuth = deepseekStatus.isConfigured;

  const configuredCount = [googleAuth, alibabaAuth, deepseekAuth].filter(Boolean).length;

  if (configuredCount === 3) {
    console.log('   🎉 All providers are configured! (Google, Alibaba, DeepSeek)');
    console.log('   💡 You can use all available models from all providers.');
  } else if (configuredCount >= 2) {
    const configured = [];
    if (googleAuth) configured.push('Google');
    if (alibabaAuth) configured.push('Alibaba');
    if (deepseekAuth) configured.push('DeepSeek');

    console.log(`   ✅ ${configured.join(' and ')} are configured.`);

    if (!googleAuth) {
      console.log('   💡 Add Google: export GEMINI_API_KEY="your-key"');
    }
    if (!alibabaAuth) {
      console.log('   💡 Add Alibaba: gemini auth login-alibaba');
    }
    if (!deepseekAuth) {
      console.log('   💡 Add DeepSeek: export DEEPSEEK_API_KEY="your-key"');
    }
  } else if (configuredCount === 1) {
    if (googleAuth) {
      console.log('   ⚠️  Only Google is configured.');
      console.log('   💡 Add more providers:');
      console.log('      gemini auth login-alibaba');
      console.log('      export DEEPSEEK_API_KEY="your-key"');
    } else if (alibabaAuth) {
      console.log('   ⚠️  Only Alibaba is configured.');
      console.log('   💡 Add more providers:');
      console.log('      export GEMINI_API_KEY="your-key"');
      console.log('      export DEEPSEEK_API_KEY="your-key"');
    } else if (deepseekAuth) {
      console.log('   ⚠️  Only DeepSeek is configured.');
      console.log('   💡 Add more providers:');
      console.log('      export GEMINI_API_KEY="your-key"');
      console.log('      gemini auth login-alibaba');
    }
  } else {
    console.log('   ❌ No providers are configured.');
    console.log('   💡 Get started (choose one or more):');
    console.log('      export GEMINI_API_KEY="your-gemini-key"');
    console.log('      export ALIBABA_DASHSCOPE_API_KEY="your-alibaba-key"');
    console.log('      export DEEPSEEK_API_KEY="your-deepseek-key"');
    console.log('      # OR use OAuth:');
    console.log('      gemini auth login-alibaba');
  }

  console.log('\n🚀 Quick Start:');
  if (googleAuth) {
    console.log('   # Test Google Gemini:');
    console.log('   gemini -m gemini-2.5-flash -p "Hello Gemini!"');
  }
  if (alibabaAuth) {
    console.log('   # Test Alibaba Qwen:');
    console.log('   gemini -m qwen-plus -p "你好，通义千问！"');
  }

  console.log('\n🔧 Management Commands:');
  console.log('   gemini auth status-alibaba     # Detailed Alibaba status');
  console.log('   gemini auth login-alibaba      # Setup Alibaba API key');
  console.log('   gemini auth logout-alibaba     # Clear Alibaba configuration');
  console.log('   gemini auth status-deepseek    # Detailed DeepSeek status');
  console.log('   gemini --list-models           # Show all available models');
}