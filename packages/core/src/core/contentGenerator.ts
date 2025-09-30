/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type {
  CountTokensResponse,
  GenerateContentResponse,
  GenerateContentParameters,
  CountTokensParameters,
  EmbedContentResponse,
  EmbedContentParameters,
} from '@google/genai';
import { GoogleGenAI } from '@google/genai';
import { createCodeAssistContentGenerator } from '../code_assist/codeAssist.js';
import type { Config } from '../config/config.js';

import type { UserTierId } from '../code_assist/types.js';
import { LoggingContentGenerator } from './loggingContentGenerator.js';
import { InstallationManager } from '../utils/installationManager.js';
import { AlibabaContentGenerator } from './alibabaContentGenerator.js';
import { DeepSeekContentGenerator } from './deepseekContentGenerator.js';

/**
 * Interface abstracting the core functionalities for generating content and counting tokens.
 */
export interface ContentGenerator {
  generateContent(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<GenerateContentResponse>;

  generateContentStream(
    request: GenerateContentParameters,
    userPromptId: string,
  ): Promise<AsyncGenerator<GenerateContentResponse>>;

  countTokens(request: CountTokensParameters): Promise<CountTokensResponse>;

  embedContent(request: EmbedContentParameters): Promise<EmbedContentResponse>;

  userTier?: UserTierId;
}

export enum AuthType {
  LOGIN_WITH_GOOGLE = 'oauth-personal',
  USE_GEMINI = 'gemini-api-key',
  USE_VERTEX_AI = 'vertex-ai',
  CLOUD_SHELL = 'cloud-shell',
  LOGIN_WITH_ANTHROPIC = 'oauth-anthropic',
  USE_ANTHROPIC_API_KEY = 'anthropic-api-key',
  MULTI_PROVIDER = 'multi-provider',
}

export type ContentGeneratorConfig = {
  apiKey?: string;
  vertexai?: boolean;
  authType?: AuthType;
  proxy?: string;
};

export function createContentGeneratorConfig(
  config: Config,
  authType: AuthType | undefined,
): ContentGeneratorConfig {
  const geminiApiKey = process.env['GEMINI_API_KEY'] || undefined;
  const googleApiKey = process.env['GOOGLE_API_KEY'] || undefined;
  const googleCloudProject = process.env['GOOGLE_CLOUD_PROJECT'] || undefined;
  const googleCloudLocation = process.env['GOOGLE_CLOUD_LOCATION'] || undefined;

  // Check if we're using a multi-provider model format (provider:model)
  const modelName = config?.getModel();
  const isMultiProvider = modelName?.includes(':');
  const effectiveAuthType = isMultiProvider ? AuthType.MULTI_PROVIDER : authType;


  const contentGeneratorConfig: ContentGeneratorConfig = {
    authType: effectiveAuthType,
    proxy: config?.getProxy(),
  };

  // If we are using Google auth, Cloud Shell, or multi-provider, there is nothing else to validate for now
  if (
    effectiveAuthType === AuthType.LOGIN_WITH_GOOGLE ||
    effectiveAuthType === AuthType.CLOUD_SHELL ||
    effectiveAuthType === AuthType.MULTI_PROVIDER
  ) {
    return contentGeneratorConfig;
  }

  if (authType === AuthType.USE_GEMINI && geminiApiKey) {
    contentGeneratorConfig.apiKey = geminiApiKey;
    contentGeneratorConfig.vertexai = false;

    return contentGeneratorConfig;
  }

  if (
    authType === AuthType.USE_VERTEX_AI &&
    (googleApiKey || (googleCloudProject && googleCloudLocation))
  ) {
    contentGeneratorConfig.apiKey = googleApiKey;
    contentGeneratorConfig.vertexai = true;

    return contentGeneratorConfig;
  }

  return contentGeneratorConfig;
}

export async function createContentGenerator(
  config: ContentGeneratorConfig,
  gcConfig: Config,
  sessionId?: string,
): Promise<ContentGenerator> {
  const version = process.env['CLI_VERSION'] || process.version;
  const userAgent = `GeminiCLI/${version} (${process.platform}; ${process.arch})`;
  const baseHeaders: Record<string, string> = {
    'User-Agent': userAgent,
  };

  // Check for multi-provider model format (provider:model) or MULTI_PROVIDER auth type
  const modelName = gcConfig.getModel();
  const modelParts = modelName.split(':');

  // If auth type is MULTI_PROVIDER, force check for provider:model format
  const isMultiProvider = config.authType === AuthType.MULTI_PROVIDER || modelParts.length === 2;

  if (isMultiProvider && modelParts.length === 2) {
    const [provider, model] = modelParts;

    if (provider === 'alibaba') {
      // Create Alibaba ContentGenerator
      const alibabaApiKey =
        process.env['ALIBABA_DASHSCOPE_API_KEY'] ||
        process.env['DASHSCOPE_API_KEY'] ||
        process.env['QWEN_API_KEY'];

      if (!alibabaApiKey) {
        throw new Error('Alibaba API key not found. Please set ALIBABA_DASHSCOPE_API_KEY environment variable.');
      }

      return new LoggingContentGenerator(
        new AlibabaContentGenerator({
          apiKey: alibabaApiKey,
          model: model,
        }),
        gcConfig,
      );
    }


    if (provider === 'deepseek') {
      // Create DeepSeek ContentGenerator
      const deepseekApiKey =
        process.env['DEEPSEEK_API_KEY'] ||
        process.env['DEEPSEEK_KEY'];

      if (!deepseekApiKey) {
        throw new Error('DeepSeek API key not found. Please set DEEPSEEK_API_KEY environment variable.');
      }

      return new LoggingContentGenerator(
        new DeepSeekContentGenerator({
          apiKey: deepseekApiKey,
          model: model,
        }),
        gcConfig,
      );
    }

    // For unknown providers, fall back to error
    throw new Error(`Unknown provider: ${provider}. Supported providers: google, alibaba, deepseek`);
  }

  // Default Google provider handling for models without provider prefix
  if (
    config.authType === AuthType.LOGIN_WITH_GOOGLE ||
    config.authType === AuthType.CLOUD_SHELL
  ) {
    const httpOptions = { headers: baseHeaders };
    return new LoggingContentGenerator(
      await createCodeAssistContentGenerator(
        httpOptions,
        config.authType,
        gcConfig,
        sessionId,
      ),
      gcConfig,
    );
  }

  if (
    config.authType === AuthType.USE_GEMINI ||
    config.authType === AuthType.USE_VERTEX_AI
  ) {
    let headers: Record<string, string> = { ...baseHeaders };
    if (gcConfig?.getUsageStatisticsEnabled()) {
      const installationManager = new InstallationManager();
      const installationId = installationManager.getInstallationId();
      headers = {
        ...headers,
        'x-gemini-api-privileged-user-id': `${installationId}`,
      };
    }
    const httpOptions = { headers };

    const googleGenAI = new GoogleGenAI({
      apiKey: config.apiKey === '' ? undefined : config.apiKey,
      vertexai: config.vertexai,
      httpOptions,
    });
    return new LoggingContentGenerator(googleGenAI.models, gcConfig);
  }
  throw new Error(
    `Error creating contentGenerator: Unsupported authType: ${config.authType}`,
  );
}
