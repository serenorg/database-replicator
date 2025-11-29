#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <tag-version>" >&2
  exit 1
fi

TAG_VERSION="$1"
VERSION="${TAG_VERSION#v}"

if [[ -z "${VERSION}" ]]; then
  echo "Unable to derive version from tag '${TAG_VERSION}'" >&2
  exit 1
fi

if [[ ! -f "CHANGELOG.md" ]]; then
  echo "CHANGELOG.md not found in $(pwd)" >&2
  exit 1
fi

CHANGELOG_SECTION="$(
  python3 - "${VERSION}" <<'PY' || exit 1
import pathlib
import re
import sys

version = sys.argv[1]
text = pathlib.Path("CHANGELOG.md").read_text(encoding="utf-8")
pattern = re.compile(rf"^## \[{re.escape(version)}\].*?(?=^## \[|\Z)", re.M | re.S)
match = pattern.search(text)

if not match:
    raise SystemExit(f"No changelog entry found for version {version}")

print(match.group(0).strip())
PY
)"

cat <<EOF
## 🚀 database-replicator ${TAG_VERSION}

${CHANGELOG_SECTION}

### 📦 Installation

**From crates.io:**
\`\`\`bash
cargo install database-replicator
\`\`\`

**Download binaries:**
- **Linux (x64)**: \`database-replicator-linux-x64-binary\`
- **macOS (Intel)**: \`database-replicator-macos-x64-binary\`
- **macOS (Apple Silicon)**: \`database-replicator-macos-arm64-binary\`

\`\`\`bash
chmod +x database-replicator-*-binary
./database-replicator-*-binary --version
\`\`\`

### 🔗 Links

- **Documentation**: https://github.com/serenorg/database-replicator#readme
- **Crates.io**: https://crates.io/crates/database-replicator
- **Issues**: https://github.com/serenorg/database-replicator/issues

### License

Apache License 2.0
EOF
