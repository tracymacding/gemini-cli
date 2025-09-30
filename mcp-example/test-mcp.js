#!/usr/bin/env node

/**
 * 简单的 MCP 客户端测试脚本
 * 用于验证 MCP server 的工具是否能被正确发现
 */

import { spawn } from 'child_process';

async function testMCPServer() {
  console.log('🔧 正在测试 MCP Server...\n');

  // 启动 MCP server
  const mcpProcess = spawn('node', ['index-expert-enhanced.js'], {
    stdio: ['pipe', 'pipe', 'pipe']
  });

  // 发送 list_tools 请求
  const listToolsRequest = {
    jsonrpc: '2.0',
    id: 1,
    method: 'tools/list'
  };

  console.log('📤 发送 list_tools 请求...');
  mcpProcess.stdin.write(JSON.stringify(listToolsRequest) + '\n');

  // 监听响应
  mcpProcess.stdout.on('data', (data) => {
    const response = data.toString().trim();
    if (response) {
      try {
        const parsed = JSON.parse(response);
        if (parsed.result && parsed.result.tools) {
          console.log('✅ 工具发现成功！');
          console.log(`📋 发现 ${parsed.result.tools.length} 个工具：\n`);

          parsed.result.tools.forEach((tool, index) => {
            console.log(`${index + 1}. 🛠️  ${tool.name}`);
            console.log(`   📝 ${tool.description}`);
            console.log('');
          });
        }
      } catch (e) {
        console.log('📤 MCP Server 输出:', response);
      }
    }
  });

  mcpProcess.stderr.on('data', (data) => {
    console.log('🔍 MCP Server 调试信息:', data.toString());
  });

  // 清理
  setTimeout(() => {
    mcpProcess.kill();
    console.log('🏁 测试完成');
    process.exit(0);
  }, 3000);
}

testMCPServer().catch(console.error);