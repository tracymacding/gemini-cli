/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Check Alibaba authentication status
 */
export async function checkAlibabaAuthStatus(): Promise<{
  isAuthenticated: boolean;
  method: 'api-key' | 'none';
  details?: any;
}> {
  // Check for API key in environment variables
  const apiKey =
    process.env['ALIBABA_DASHSCOPE_API_KEY'] ||
    process.env['DASHSCOPE_API_KEY'] ||
    process.env['QWEN_API_KEY'];

  if (apiKey) {
    let source = 'ALIBABA_DASHSCOPE_API_KEY';
    if (process.env['DASHSCOPE_API_KEY']) source = 'DASHSCOPE_API_KEY';
    if (process.env['QWEN_API_KEY']) source = 'QWEN_API_KEY';

    return {
      isAuthenticated: true,
      method: 'api-key',
      details: {
        source,
        keyLength: apiKey.length,
        keyPreview: `${apiKey.substring(0, 8)}...${apiKey.substring(apiKey.length - 4)}`,
      },
    };
  }

  return {
    isAuthenticated: false,
    method: 'none',
  };
}

/**
 * Display Alibaba authentication status
 */
export async function displayAlibabaAuthStatus(): Promise<void> {
  console.log('🔍 Alibaba / 通义千问 Authentication Status\n');
  console.log('=' .repeat(50));

  const status = await checkAlibabaAuthStatus();

  if (status.isAuthenticated && status.details) {
    console.log('✅ Authenticated via API Key');
    console.log(`🔑 Key source: ${status.details.source}`);
    console.log(`📏 Key length: ${status.details.keyLength} characters`);
    console.log(`👀 Key preview: ${status.details.keyPreview}`);

    console.log('\n💡 Available models:');
    console.log('   • qwen-max - 最强大的通义千问模型');
    console.log('   • qwen-plus - 平衡性能与成本 (默认)');
    console.log('   • qwen-turbo - 高速响应模型');
    console.log('   • qwen-long - 超长文本处理');
    console.log('   • qwen-vl-plus - 视觉理解模型');
    console.log('   • qwen-coder-plus - 代码生成模型');

    console.log('\n🚀 Quick Test:');
    console.log('   gemini -m qwen-plus -p "你好，通义千问！"');
    console.log('   gemini --provider alibaba -p "Hello Qwen!"');
  } else {
    console.log('❌ Not authenticated');

    console.log('\n📋 Setup Instructions:');
    console.log('1. 🌐 Get your DashScope API key from:');
    console.log('   https://dashscope.console.aliyun.com/');

    console.log('\n2. 🔧 Set environment variable (choose one):');
    console.log('   export ALIBABA_DASHSCOPE_API_KEY="sk-xxx"');
    console.log('   export DASHSCOPE_API_KEY="sk-xxx"');
    console.log('   export QWEN_API_KEY="sk-xxx"');

    console.log('\n3. ✅ Verify setup:');
    console.log('   gemini auth status-alibaba');

    console.log('\n💡 Note: Alibaba Qwen uses API key authentication');
    console.log('   No OAuth setup required, just set the environment variable.');
  }

  console.log('\n🔗 Helpful Links:');
  console.log('   • DashScope Console: https://dashscope.console.aliyun.com/');
  console.log('   • API Documentation: https://help.aliyun.com/zh/dashscope/');
  console.log('   • Model Pricing: https://help.aliyun.com/zh/dashscope/product-overview/billing-methods');
}

/**
 * Handle Alibaba API key setup guidance
 */
export async function handleAlibabaLogin(): Promise<void> {
  console.log('🔐 阿里云通义千问 API Key 设置指南\n');
  console.log('=' .repeat(50));

  const status = await checkAlibabaAuthStatus();

  if (status.isAuthenticated) {
    console.log('✅ 您已经配置了阿里云 API Key！');
    console.log(`🔑 当前使用: ${status.details?.source}`);
    console.log('\n💡 您可以直接使用通义千问模型:');
    console.log('   gemini -m qwen-plus -p "你好，通义千问！"');
    return;
  }

  console.log('📝 请按照以下步骤设置 API Key:');
  console.log('\n1. 🌐 访问阿里云DashScope控制台:');
  console.log('   https://dashscope.console.aliyun.com/');

  console.log('\n2. 🔑 获取 API Key:');
  console.log('   • 登录您的阿里云账号');
  console.log('   • 在控制台中找到 "API-KEY管理"');
  console.log('   • 创建新的 API Key 或复制现有的');

  console.log('\n3. 💻 设置环境变量 (选择其中一种):');
  console.log('   # 推荐方式');
  console.log('   export ALIBABA_DASHSCOPE_API_KEY="sk-your-api-key-here"');
  console.log('   ');
  console.log('   # 或者使用简短形式');
  console.log('   export DASHSCOPE_API_KEY="sk-your-api-key-here"');
  console.log('   export QWEN_API_KEY="sk-your-api-key-here"');

  console.log('\n4. ✅ 验证设置:');
  console.log('   gemini auth status-alibaba');

  console.log('\n5. 🚀 开始使用:');
  console.log('   gemini -m qwen-plus -p "你好，通义千问！"');

  console.log('\n💡 提示:');
  console.log('   • API Key 通常以 "sk-" 开头');
  console.log('   • 保护好您的 API Key，不要在代码中明文写入');
  console.log('   • 可以在 ~/.bashrc 或 ~/.zshrc 中设置环境变量以持久化');

  console.log('\n⚠️  重要提醒:');
  console.log('   使用通义千问会产生费用，请查看阿里云的计费说明：');
  console.log('   https://help.aliyun.com/zh/dashscope/product-overview/billing-methods');
}

/**
 * Handle Alibaba logout (clear environment guidance)
 */
export async function handleAlibabaLogout(): Promise<void> {
  console.log('🔐 清除阿里云通义千问配置\n');
  console.log('=' .repeat(50));

  const status = await checkAlibabaAuthStatus();

  if (!status.isAuthenticated) {
    console.log('ℹ️  当前没有配置阿里云 API Key');
    return;
  }

  console.log(`🔑 当前配置的 API Key 来源: ${status.details?.source}`);
  console.log('\n📝 要清除配置，请执行以下操作:');

  console.log('\n1. 🗑️  取消设置环境变量:');
  if (status.details?.source === 'ALIBABA_DASHSCOPE_API_KEY') {
    console.log('   unset ALIBABA_DASHSCOPE_API_KEY');
  } else if (status.details?.source === 'DASHSCOPE_API_KEY') {
    console.log('   unset DASHSCOPE_API_KEY');
  } else if (status.details?.source === 'QWEN_API_KEY') {
    console.log('   unset QWEN_API_KEY');
  }

  console.log('\n2. 📝 如果在配置文件中设置过，请编辑:');
  console.log('   # 检查并编辑这些文件，删除相关的 export 语句');
  console.log('   ~/.bashrc');
  console.log('   ~/.zshrc');
  console.log('   ~/.bash_profile');

  console.log('\n3. ✅ 重新加载配置:');
  console.log('   source ~/.bashrc  # 或相应的配置文件');

  console.log('\n4. 🔍 验证清除:');
  console.log('   gemini auth status-alibaba');

  console.log('\n💡 注意: 这只是配置清除指南，实际的环境变量需要您手动清除');
}