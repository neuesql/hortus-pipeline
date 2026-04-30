#!/bin/bash
#
# Switch the extension to build against a different DuckDB version.
#
# Usage:
#   ./scripts/switch-duckdb-version.sh v1.5.2
#
# This will:
#   1. Create a branch duckdb-v1.5.2 (if not already on it)
#   2. Update the duckdb submodule to the target version
#   3. Update the extension-ci-tools submodule to the target version
#   4. Update the CI workflow to reference the correct CI tools version
#   5. Commit the changes
#

set -euo pipefail

VERSION="${1:?Usage: $0 <version> (e.g. v1.5.2)}"

# Validate version format
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format vX.Y.Z (e.g. v1.5.2)"
    exit 1
fi

BRANCH="duckdb-${VERSION}"

echo "==> Switching to DuckDB ${VERSION}"

# Create branch if not already on it
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "$BRANCH" ]; then
    echo "==> Creating branch ${BRANCH}"
    git checkout -b "$BRANCH"
fi

# Update duckdb submodule
echo "==> Updating duckdb submodule to ${VERSION}"
cd duckdb
git fetch --tags
git checkout "$VERSION"
cd ..

# Update extension-ci-tools submodule
echo "==> Updating extension-ci-tools to ${VERSION}"
cd extension-ci-tools
git fetch --tags
git checkout "$VERSION"
cd ..

# Update CI workflow
echo "==> Updating CI workflow"
sed -i.bak "s|extension-ci-tools/\.github/workflows/_extension_distribution\.yml@v[0-9.]*|extension-ci-tools/.github/workflows/_extension_distribution.yml@${VERSION}|g" .github/workflows/MainDistributionPipeline.yml
sed -i.bak "s|extension-ci-tools/\.github/workflows/_extension_code_quality\.yml@v[0-9.]*|extension-ci-tools/.github/workflows/_extension_code_quality.yml@${VERSION}|g" .github/workflows/MainDistributionPipeline.yml
sed -i.bak "s|duckdb_version: v[0-9.]*|duckdb_version: ${VERSION}|g" .github/workflows/MainDistributionPipeline.yml
sed -i.bak "s|ci_tools_version: v[0-9.]*|ci_tools_version: ${VERSION}|g" .github/workflows/MainDistributionPipeline.yml
rm -f .github/workflows/MainDistributionPipeline.yml.bak

echo "==> Done. Changes ready to commit."
echo ""
echo "Next steps:"
echo "  git add -A"
echo "  git commit -m 'chore: switch to DuckDB ${VERSION}'"
echo "  make clean && make release"
echo "  make test"
