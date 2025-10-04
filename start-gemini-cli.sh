#!/bin/bash

# Gemini CLI 启动脚本
# 支持 Google Gemini、阿里云通义千问、DeepSeek
# 自动加载环境变量并启动 CLI

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_header() {
    echo -e "${PURPLE}🤖 $1${NC}"
}

# 显示帮助信息
show_help() {
    echo -e "${CYAN}Gemini CLI 启动脚本 - 多模型支持${NC}"
    echo ""
    echo "用法: $0 [选项] [Gemini CLI 参数]"
    echo ""
    echo "选项:"
    echo "  -h, --help              显示此帮助信息"
    echo "  -m, --model MODEL       指定模型 (gemini-2.5-flash, qwen-plus, deepseek-chat 等)"
    echo "  --provider PROVIDER     指定提供者 (google, alibaba, deepseek)"
    echo "  -p, --prompt PROMPT     指定提示词"
    echo "  -c, --check             检查所有提供者的认证状态"
    echo "  -s, --setup             显示设置指南"
    echo "  --qwen                  快速使用通义千问 (qwen-plus)"
    echo "  --gemini                快速使用 Gemini (gemini-2.5-flash)"
    echo "  --deepseek              快速使用 DeepSeek (deepseek-chat)"
    echo ""
    echo "示例:"
    echo "  $0                                    # 启动交互模式"
    echo "  $0 --check                           # 检查认证状态"
    echo "  $0 --qwen -p \"你好，通义千问！\"      # 使用通义千问"
    echo "  $0 -m qwen-max -p \"复杂的问题\"      # 使用 qwen-max 模型"
    echo "  $0 --deepseek -p \"Hello DeepSeek!\"  # 使用 DeepSeek"
    echo ""
}

# 检查环境变量
check_env() {
    local env_name="$1"
    local description="$2"

    if [ -n "${!env_name}" ]; then
        print_success "$description 已配置 ($env_name)"
        return 0
    else
        print_warning "$description 未配置 ($env_name)"
        return 1
    fi
}

# 检查认证状态
check_auth_status() {
    print_header "检查多提供者认证状态"
    echo ""

    # 检查 Google/Gemini
    echo -e "${BLUE}🔵 Google / Gemini:${NC}"
    google_configured=false
    if check_env "GEMINI_API_KEY" "Gemini API Key"; then
        google_configured=true
    elif check_env "GOOGLE_API_KEY" "Google API Key"; then
        google_configured=true
    fi

    # 检查 Vertex AI
    if [ -n "$GOOGLE_CLOUD_PROJECT" ] && [ -n "$GOOGLE_CLOUD_LOCATION" ]; then
        print_success "Vertex AI 已配置"
        google_configured=true
    fi

    if [ "$google_configured" = false ]; then
        print_warning "Google 认证未配置"
    fi

    echo ""


    # 检查阿里云通义千问
    echo -e "${CYAN}🟠 Alibaba / 通义千问:${NC}"
    alibaba_configured=false
    if check_env "ALIBABA_DASHSCOPE_API_KEY" "阿里云 DashScope API Key"; then
        alibaba_configured=true
    elif check_env "DASHSCOPE_API_KEY" "DashScope API Key"; then
        alibaba_configured=true
    elif check_env "QWEN_API_KEY" "Qwen API Key"; then
        alibaba_configured=true
    fi

    echo ""

    # 检查 DeepSeek
    echo -e "${PURPLE}🔵 DeepSeek:${NC}"
    deepseek_configured=false
    if check_env "DEEPSEEK_API_KEY" "DeepSeek API Key"; then
        deepseek_configured=true
    elif check_env "DEEPSEEK_KEY" "DeepSeek Key"; then
        deepseek_configured=true
    fi

    echo ""

    # 汇总状态
    configured_count=0
    [ "$google_configured" = true ] && ((configured_count++))
    [ "$alibaba_configured" = true ] && ((configured_count++))
    [ "$deepseek_configured" = true ] && ((configured_count++))

    print_header "配置汇总:"
    if [ $configured_count -eq 3 ]; then
        print_success "所有提供者都已配置！🎉"
    elif [ $configured_count -eq 2 ]; then
        print_success "$configured_count/3 个提供者已配置"
    elif [ $configured_count -eq 1 ]; then
        print_warning "$configured_count/3 个提供者已配置"
    else
        print_error "没有提供者配置完成"
    fi

    echo ""
}

# 显示设置指南
show_setup_guide() {
    print_header "多提供者设置指南"
    echo ""

    echo -e "${BLUE}🔵 Google Gemini 设置:${NC}"
    echo "  export GEMINI_API_KEY=\"your-gemini-api-key\""
    echo "  # 获取 API Key: https://aistudio.google.com/apikey"
    echo ""


    echo -e "${CYAN}🟠 阿里云通义千问设置:${NC}"
    echo "  export ALIBABA_DASHSCOPE_API_KEY=\"sk-your-dashscope-key\""
    echo "  # 或使用其他变量名:"
    echo "  # export DASHSCOPE_API_KEY=\"sk-your-dashscope-key\""
    echo "  # export QWEN_API_KEY=\"sk-your-dashscope-key\""
    echo "  # 获取 API Key: https://dashscope.console.aliyun.com/"
    echo ""

    echo -e "${PURPLE}🔵 DeepSeek 设置:${NC}"
    echo "  export DEEPSEEK_API_KEY=\"sk-your-deepseek-key\""
    echo "  # 或使用其他变量名:"
    echo "  # export DEEPSEEK_KEY=\"sk-your-deepseek-key\""
    echo "  # 获取 API Key: https://platform.deepseek.com/"
    echo "  # 模型: deepseek-chat, deepseek-coder, deepseek-reasoner"
    echo ""

    echo -e "${PURPLE}💡 设置完成后运行:${NC}"
    echo "  $0 --check  # 验证配置"
    echo ""
}

# 加载环境配置
load_env() {
    # 加载 .env 文件
    if [ -f .env ]; then
        set -a
        source .env
        set +a
        print_success "已加载 .env 配置"
    fi

    # 设置代理（如果需要）
    if [ -n "$HTTP_PROXY" ]; then
        export https_proxy="$HTTP_PROXY"
        export http_proxy="$HTTP_PROXY"
        print_info "已设置代理: $HTTP_PROXY"
    fi
}

# 解析命令行参数
parse_args() {
    POSITIONAL=()
    MODEL="deepseek-chat"
    PROVIDER="deepseek"
    PROMPT=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--check)
                check_auth_status
                exit 0
                ;;
            -s|--setup)
                show_setup_guide
                exit 0
                ;;
            -m|--model)
                MODEL="$2"
                shift 2
                ;;
            --provider)
                PROVIDER="$2"
                shift 2
                ;;
            --qwen)
                MODEL="qwen-plus"
                PROVIDER="alibaba"
                shift
                ;;
            --gemini)
                MODEL="gemini-2.5-flash"
                PROVIDER="google"
                shift
                ;;
            --deepseek)
                MODEL="deepseek-chat"
                PROVIDER="deepseek"
                shift
                ;;
            -p|--prompt)
                PROMPT="$2"
                shift 2
                ;;
            *)
                POSITIONAL+=("$1")
                shift
                ;;
        esac
    done

    # 恢复位置参数
    set -- "${POSITIONAL[@]}"
}

# 构建 CLI 参数
build_cli_args() {
    CLI_ARGS=()

    # 添加模型参数
    if [ -n "$MODEL" ]; then
        CLI_ARGS+=("-m" "$MODEL")
        print_info "使用模型: $MODEL"
    fi

    # 添加提供者参数
    if [ -n "$PROVIDER" ]; then
        CLI_ARGS+=("--provider" "$PROVIDER")
        print_info "使用提供者: $PROVIDER"
    fi

    # 添加提示词参数
    if [ -n "$PROMPT" ]; then
        CLI_ARGS+=("-p" "$PROMPT")
        print_info "使用提示词: $PROMPT"
    fi

    # 添加其他参数
    CLI_ARGS+=("$@")
}

# 检查非Google模型的特殊处理
check_non_google_models() {
    if [ "$PROVIDER" = "alibaba" ] || [[ "$MODEL" == qwen-* ]]; then
        print_warning "检测到阿里云通义千问模型使用"
        echo ""
        print_info "当前状态: 阿里云提供者已集成但运行时支持正在开发中"
        print_info "您可以使用以下功能:"
        echo "  ✅ 认证状态检查: $0 --check"
        echo "  ✅ 模型列表查看: npx gemini --list-models"
        echo "  ✅ 配置管理: npx gemini auth status-alibaba"
        echo ""
        print_warning "运行时支持即将推出，当前建议使用:"
        echo "  • Google Gemini: $0 --gemini -p \"你的问题\""
        echo ""
        read -p "是否继续尝试运行? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "已取消运行"
            exit 0
        fi
    fi


    if [ "$PROVIDER" = "deepseek" ] || [[ "$MODEL" == deepseek-* ]]; then
        if [ -z "$DEEPSEEK_API_KEY" ] && [ -z "$DEEPSEEK_KEY" ]; then
            print_error "DeepSeek API Key 未配置"
            print_info "请设置: export DEEPSEEK_API_KEY=\"sk-your-key\""
            print_info "或运行: npx gemini auth deepseek"
            print_info "获取 API Key: https://platform.deepseek.com/"
            exit 1
        fi
        print_success "检测到 DeepSeek 模型，配置正常"
    fi
}

# 主函数
main() {
    print_header "Gemini CLI 多模型启动器"
    echo ""

    # 加载环境变量
    load_env

    # 解析参数
    parse_args "$@"

    # 检查非Google模型的特殊处理
    check_non_google_models

    # 构建 CLI 参数
    build_cli_args "${POSITIONAL[@]}"

    # 检查 CLI 是否存在
    # 优先使用 bundle 版本（包含最新功能）
    CLI_PATH="./bundle/gemini.js"
    if [ ! -f "$CLI_PATH" ]; then
        # 回退到开发版本
        CLI_PATH="./packages/cli/dist/index.js"
        if [ ! -f "$CLI_PATH" ]; then
            print_error "CLI 文件不存在"
            print_info "请先运行: npm run build && npm run bundle"
            print_info "或者: npm run build （使用开发版本）"
            exit 1
        else
            print_warning "使用开发版本: $CLI_PATH"
            print_info "建议运行 'npm run bundle' 获取最新功能"
        fi
    else
        print_success "使用 bundle 版本: $CLI_PATH"
    fi

    # 启动 CLI
    if [ ${#CLI_ARGS[@]} -eq 0 ]; then
        print_info "启动交互模式..."
        echo ""
    else
        print_info "执行命令: node $CLI_PATH ${CLI_ARGS[*]}"
        echo ""
    fi

    # 执行
    node "$CLI_PATH" "${CLI_ARGS[@]}"
}

# 运行主函数
main "$@"
