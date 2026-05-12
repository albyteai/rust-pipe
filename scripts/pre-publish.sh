#!/bin/bash
set -e

echo "=== Pre-publish checklist ==="
echo ""

echo "1. Format check..."
cargo fmt --check || { echo "FAIL: Run cargo fmt"; exit 1; }
echo "   PASS"

echo "2. Clippy (library)..."
cargo clippy --lib -- -D warnings 2>&1 | grep -q "error" && { echo "FAIL: Clippy errors"; exit 1; } || echo "   PASS"

echo "3. Clippy (tests)..."
cargo clippy --tests -- -D warnings -A deprecated 2>&1 | grep -q "error" && { echo "FAIL: Clippy test errors"; exit 1; } || echo "   PASS"

echo "4. Tests..."
cargo test 2>&1 | grep -q "FAILED" && { echo "FAIL: Tests failed"; exit 1; } || echo "   PASS"

echo "5. Doc build..."
cargo doc --no-deps 2>&1 | grep -qi "^error" && { echo "FAIL: Doc errors"; exit 1; } || echo "   PASS"

echo "6. Package..."
cargo package --allow-dirty > /dev/null 2>&1 || { echo "FAIL: Package failed"; exit 1; }
echo "   PASS"

echo "7. README mermaid (no emojis, no :: in labels)..."
MERMAID_BLOCKS=$(sed -n '/```mermaid/,/```/p' README.md)
if echo "$MERMAID_BLOCKS" | grep -P '[\x{1F300}-\x{1F9FF}\x{2600}-\x{27BF}]' > /dev/null 2>&1; then
    echo "FAIL: Emojis in mermaid blocks break crates.io renderer"
    exit 1
fi
echo "   PASS"

echo "8. No leaked references..."
if grep -rn 'amazon\|albinkm\|@gmail\|havocray' src/ sdks/ README.md Cargo.toml LICENSE* CHANGELOG.md 2>/dev/null; then
    echo "FAIL: Leaked internal references"
    exit 1
fi
echo "   PASS"

echo "9. Version in CHANGELOG..."
VERSION=$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)
if ! grep -q "\[$VERSION\]" CHANGELOG.md; then
    echo "FAIL: Version $VERSION not in CHANGELOG.md"
    exit 1
fi
echo "   PASS"

echo "10. Git clean..."
if [ -n "$(git status --porcelain)" ]; then
    echo "FAIL: Uncommitted changes"
    exit 1
fi
echo "   PASS"

echo ""
echo "=== ALL CHECKS PASSED ==="
echo "Safe to run: cargo publish"
