#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST_DIR="$(cd "$ROOT_DIR/.." && pwd)"

case "$(go env GOOS)" in
  darwin) ext="dylib" ;;
  windows) ext="dll" ;;
  *) ext="so" ;;
esac

mkdir -p "$HOST_DIR/plugins"
cd "$ROOT_DIR"
go build -buildmode=c-shared -o "$HOST_DIR/plugins/cpa-freemodel.$ext" ./cmd/plugin
rm -f "$HOST_DIR/plugins/cpa-freemodel.h"
echo "$HOST_DIR/plugins/cpa-freemodel.$ext"
