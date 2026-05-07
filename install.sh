#!/bin/bash
set -e

detect_shell() {
    if [ -n "${SHELL:-}" ]; then
        basename "$SHELL"
        return
    fi

    ps -p "$$" -o comm= 2>/dev/null | awk '{print $1}'
}

ask_yes_no() {
    if [ "${BYTEORM_INSTALL_COMPLETIONS:-}" = "1" ]; then
        return 0
    fi
    if [ "${BYTEORM_INSTALL_COMPLETIONS:-}" = "0" ]; then
        return 1
    fi

    printf "%s [Y/n] " "$1"
    read -r answer
    [ -z "$answer" ] || printf "%s" "$answer" | grep -Eq '^[Yy]'
}

append_once() {
    local file="$1"
    local marker="$2"
    local content="$3"

    mkdir -p "$(dirname "$file")"
    touch "$file"
    if ! grep -Fq "$marker" "$file"; then
        {
            echo ""
            echo "$marker"
            printf "%s\n" "$content"
        } >> "$file"
    fi
}

install_completions() {
    local shell_name
    shell_name="$(detect_shell)"

    case "$shell_name" in
        bash|zsh|fish|elvish)
            ;;
        *)
            echo "Detected shell: ${shell_name:-unknown}"
            echo "Autocomplete skipped: unsupported shell."
            echo "Generate manually with: byteorm completions <bash|zsh|fish|powershell|elvish>"
            return
            ;;
    esac

    echo "Detected shell: $shell_name"
    if ! ask_yes_no "Install ByteORM autocomplete for $shell_name?"; then
        echo "Autocomplete skipped. You can install it later with:"
        echo "  byteorm completions $shell_name"
        return
    fi

    case "$shell_name" in
        bash)
            local completion_dir="${XDG_DATA_HOME:-$HOME/.local/share}/bash-completion/completions"
            mkdir -p "$completion_dir"
            byteorm completions bash > "$completion_dir/byteorm"
            echo "Autocomplete installed for bash: $completion_dir/byteorm"
            ;;
        zsh)
            local completion_dir="$HOME/.byteorm/completions"
            mkdir -p "$completion_dir"
            byteorm completions zsh > "$completion_dir/_byteorm"
            append_once "${ZDOTDIR:-$HOME}/.zshrc" "# ByteORM autocomplete" "fpath=(\"$completion_dir\" \$fpath)
autoload -Uz compinit
compinit"
            echo "Autocomplete installed for zsh. Restart the shell if Tab completion is not active."
            ;;
        fish)
            local completion_dir="${XDG_CONFIG_HOME:-$HOME/.config}/fish/completions"
            mkdir -p "$completion_dir"
            byteorm completions fish > "$completion_dir/byteorm.fish"
            echo "Autocomplete installed for fish: $completion_dir/byteorm.fish"
            ;;
        elvish)
            local completion_dir="${XDG_CONFIG_HOME:-$HOME/.config}/elvish/lib"
            mkdir -p "$completion_dir"
            byteorm completions elvish > "$completion_dir/byteorm-completions.elv"
            append_once "${XDG_CONFIG_HOME:-$HOME/.config}/elvish/rc.elv" "# ByteORM autocomplete" "use byteorm-completions"
            echo "Autocomplete installed for elvish. Restart the shell if Tab completion is not active."
            ;;
    esac
}

echo "Installing ByteORM..."

if ! command -v cargo > /dev/null 2>&1; then
    echo "Cargo is not installed. Please install Rust first:"
    echo "   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

echo "Installing ByteORM package from GitHub..."
cargo install --git https://github.com/bytematebot/byteorm --package byteorm --bin byteorm --force

echo ""
echo "ByteORM installed successfully!"
install_completions

echo ""
echo "Usage:"
echo "  byteorm init        - Initialize byteorm.toml and starter schema"
echo "  byteorm generate    - Generate client from schema"
echo "  byteorm push        - Push schema and generate client"
echo "  byteorm doctor      - Show resolved config and paths"
echo "  byteorm completions - Generate shell autocomplete scripts"
echo "  byteorm reset       - Reset database"
echo "  byteorm self-update - Update to latest version"
