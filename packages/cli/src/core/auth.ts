/**
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

import {
  type AuthType,
  type Config,
  getErrorMessage,
  AuthType as AuthTypeEnum,
} from '@google/gemini-cli-core';

/**
 * Handles the initial authentication flow.
 * @param config The application config.
 * @param authType The selected auth type.
 * @returns An error message if authentication fails, otherwise null.
 */
export async function performInitialAuth(
  config: Config,
  authType: AuthType | undefined,
): Promise<string | null> {
  if (!authType) {
    return null;
  }

  // Check if we're using a multi-provider model format (provider:model)
  const modelName = config.getModel();
  const isMultiProvider = modelName?.includes(':');
  const effectiveAuthType = isMultiProvider ? AuthTypeEnum.MULTI_PROVIDER : authType;


  try {
    await config.refreshAuth(effectiveAuthType);
    // The console.log is intentionally left out here.
    // We can add a dedicated startup message later if needed.
  } catch (e) {
    return `Failed to login. Message: ${getErrorMessage(e)}`;
  }

  return null;
}
