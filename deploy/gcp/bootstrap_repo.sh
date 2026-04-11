#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_DIR"

"$REPO_DIR/deploy/gcp/install_docker_host.sh"

mkdir -p "$REPO_DIR/runtime" "$REPO_DIR/static_data"

if [ ! -f "$REPO_DIR/config/runtime.env" ]; then
  cp "$REPO_DIR/config/runtime.container.env.example" "$REPO_DIR/config/runtime.env"
  echo "Created config/runtime.env from container example."
else
  echo "config/runtime.env already exists. Leaving it unchanged."
fi

cat <<'EOF'

Bootstrap complete.

Next steps:
1. Get a free OpenTopography API key at https://opentopography.org/
   (Create account > Dashboard > Request API Key)

2. Add your key to config/runtime.env:
   nano config/runtime.env
   Set: CUSTOM_SRTM_API_KEY=your_key_here

3. Build the Docker image (~30 min first time, needs 50 GB disk):
   ./deploy/gcp/mwn.sh build

4. Download terrain for your area (use your lat/lon bounding box):
   ./deploy/gcp/mwn.sh fetch-dem <north> <east> <south> <west> static_data/my_area.tif

5. Update config/domains.json so elevation_file matches your filename.

6. Verify and run:
   ./deploy/gcp/mwn.sh check
   ./deploy/gcp/mwn.sh run --hours 6

See docs/gcp_setup.md for the full walkthrough.

EOF
