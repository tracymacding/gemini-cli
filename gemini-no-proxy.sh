#!/bin/bash
# Wrapper script to run Gemini CLI without proxy for localhost connections

# Save current proxy settings
SAVED_HTTP_PROXY="${http_proxy}"
SAVED_HTTPS_PROXY="${https_proxy}"
SAVED_ALL_PROXY="${all_proxy}"

# Unset proxy for this session
unset http_proxy
unset https_proxy
unset HTTP_PROXY
unset HTTPS_PROXY
unset all_proxy
unset ALL_PROXY

# Run Gemini CLI with all arguments
./bundle/gemini.js "$@"

# Restore proxy settings (optional, as this script exits anyway)
export http_proxy="${SAVED_HTTP_PROXY}"
export https_proxy="${SAVED_HTTPS_PROXY}"
export all_proxy="${SAVED_ALL_PROXY}"
