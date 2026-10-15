#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
# Existing credentials are retained. Rotate explicitly if they have expired.
if [ ! -f data/local-auth.json ]; then
    python3 -m backend.api.setup_auth
fi
bash backend/worker/demo.sh
bash backend/knowledge/demo.sh
docker compose up -d --build --wait api web
python3 -m backend.api.verify "$@"
