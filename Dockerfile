FROM node:24

# Install build dependencies for native modules
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    zstd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./

# Install dependencies with caching
RUN --mount=type=cache,id=npm-cache,target=/root/.npm \
    npm ci --prefer-offline --no-audit

COPY . .

# Copy and make entrypoint script executable
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Create a global npm link for frostbyte-sdk
# This makes it available system-wide for any module
RUN npm link

# Set NODE_PATH to include global npm modules and plugins
ENV NODE_PATH=/usr/local/lib/node_modules:/app/node_modules:/plugins/node_modules

# Increase Node.js heap size to 16GB
ENV NODE_OPTIONS="--max-old-space-size=16384"

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["node", "cli.ts", "run", "--plugins-dir=/plugins", "--data-dir=/data"]
