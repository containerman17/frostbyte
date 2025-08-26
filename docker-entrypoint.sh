#!/bin/sh
set -e

# Create node_modules symlink in plugins directory at runtime
# This is needed because /plugins is a mounted volume
if [ -d "/plugins" ]; then
    mkdir -p /plugins/node_modules
    ln -sfn /app /plugins/node_modules/frostbyte-sdk
    echo "Created frostbyte-sdk symlink in /plugins/node_modules"
fi

# Execute the main command
exec "$@"
