#!/bin/bash
set -e

echo "🚀 Installing ByteORM..."

if ! command -v cargo &> /dev/null; then
    echo "❌ Cargo is not installed. Please install Rust first:"
    echo "   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

echo "📦 Installing from GitHub..."
cargo install --git https://github.com/bytematebot/byteorm --force

echo ""
echo "✅ ByteORM installed successfully!"
echo ""
echo "Usage:"
echo "  byteorm push        - Push schema and generate client"
echo "  byteorm reset       - Reset database"
echo "  byteorm studio      - Launch ByteORM Studio"
echo "  byteorm self-update - Update to latest version"
