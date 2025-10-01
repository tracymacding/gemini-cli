/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import type { Config } from '@google/gemini-cli-core';
import { AuthType, OutputFormat } from '@google/gemini-cli-core';
import { USER_SETTINGS_PATH } from './config/settings.js';
import { validateAuthMethod } from './config/auth.js';
import { type LoadedSettings } from './config/settings.js';
import { handleError } from './utils/errors.js';

function getAuthTypeFromEnv(modelName?: string): AuthType | undefined {
  if (process.env['GOOGLE_GENAI_USE_GCA'] === 'true') {
    return AuthType.LOGIN_WITH_GOOGLE;
  }
  if (process.env['GOOGLE_GENAI_USE_VERTEXAI'] === 'true') {
    return AuthType.USE_VERTEX_AI;
  }

  // Check if we're using a multi-provider model (provider:model format)
  if (modelName?.includes(':')) {
    const [provider] = modelName.split(':');
    if (
      provider === 'alibaba' ||
      provider === 'deepseek' ||
      provider === 'anthropic'
    ) {
      return AuthType.MULTI_PROVIDER;
    }
  }

  if (process.env['GEMINI_API_KEY']) {
    return AuthType.USE_GEMINI;
  }
  return undefined;
}

export async function validateNonInteractiveAuth(
  configuredAuthType: AuthType | undefined,
  useExternalAuth: boolean | undefined,
  nonInteractiveConfig: Config,
  settings: LoadedSettings,
) {
  try {
    const enforcedType = settings.merged.security?.auth?.enforcedType;

    // Check if we're using a multi-provider model format (provider:model) FIRST
    // Multi-provider mode should OVERRIDE any configured auth type (including selectedType)
    const configModelName = nonInteractiveConfig.getModel();
    const isMultiProvider = configModelName?.includes(':');

    // If multi-provider mode is detected, use it immediately and ignore other auth settings
    if (isMultiProvider) {
      const authType = AuthType.MULTI_PROVIDER;
      if (!useExternalAuth) {
        const err = validateAuthMethod(String(authType));
        if (err != null) {
          throw new Error(err);
        }
      }
      await nonInteractiveConfig.refreshAuth(authType);
      return nonInteractiveConfig;
    }

    // Only check enforcedType if not in multi-provider mode
    if (enforcedType) {
      const currentAuthType = getAuthTypeFromEnv(configModelName);
      if (currentAuthType !== enforcedType) {
        const message = `The configured auth type is ${enforcedType}, but the current auth type is ${currentAuthType}. Please re-authenticate with the correct type.`;
        throw new Error(message);
      }
    }

    const effectiveAuthType =
      enforcedType || getAuthTypeFromEnv(configModelName) || configuredAuthType;

    if (!effectiveAuthType) {
      const message = `Please set an Auth method in your ${USER_SETTINGS_PATH} or specify one of the following environment variables before running: GEMINI_API_KEY, GOOGLE_GENAI_USE_VERTEXAI, GOOGLE_GENAI_USE_GCA`;
      throw new Error(message);
    }

    const authType: AuthType = effectiveAuthType as AuthType;

    if (!useExternalAuth) {
      const err = validateAuthMethod(String(authType));
      if (err != null) {
        throw new Error(err);
      }
    }

    await nonInteractiveConfig.refreshAuth(authType);
    return nonInteractiveConfig;
  } catch (error) {
    if (nonInteractiveConfig.getOutputFormat() === OutputFormat.JSON) {
      handleError(
        error instanceof Error ? error : new Error(String(error)),
        nonInteractiveConfig,
        1,
      );
    } else {
      console.error(error instanceof Error ? error.message : String(error));
      process.exit(1);
    }
  }
}
