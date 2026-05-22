#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_DIR"

"$REPO_DIR/deploy/gcp/install_docker_host.sh"

mkdir -p "$REPO_DIR/runtime" "$REPO_DIR/static_data"

if [ ! -f "$REPO_DIR/config/runtime.env" ]; then
  cp "$REPO_DIR/config/runtime.env.example" "$REPO_DIR/config/runtime.env"
  echo "Created config/runtime.env from example."
else
  echo "config/runtime.env already exists. Leaving it unchanged."
fi

cat <<'EOF'

Bootstrap complete.

Next steps:
1. Apply Docker group permissions in this shell:
   newgrp docker

2. Prove Docker works without sudo:
   docker run hello-world

3. Initialize local config and pull the default image:
   ./deploy/gcp/mwn.sh init --image pull

   If the image pull fails, build locally (~30 min first time, needs 50 GB disk):
   ./deploy/gcp/mwn.sh build-local

4. Download and register DEM + LCP terrain for your area (use your center point):
   ./deploy/gcp/mwn.sh fetch-terrain --center <lat> <lon> --size-km 10 --domain my_area --label "My Area"

5. Verify and run:
   ./deploy/gcp/mwn.sh check
   ./deploy/gcp/mwn.sh smoke
   ./deploy/gcp/mwn.sh run --hours 6

For non-US SRTM terrain, get a free OpenTopography API key and set
CUSTOM_SRTM_API_KEY in config/runtime.env. USGS 3DEP terrain does not need it.

See docs/gcp_setup.md for the full walkthrough.

EOF
