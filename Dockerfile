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

# Create a symlink for the SDK so plugins can import from 'frostbyte-sdk'
# This mimics what npm link would do in development
RUN cd /usr/local/lib && \
    mkdir -p node_modules && \
    ln -s /app node_modules/frostbyte-sdk

# Set NODE_PATH so Node.js can find the linked module
ENV NODE_PATH=/usr/local/lib/node_modules:/app/node_modules

# Increase Node.js heap size to 16GB
ENV NODE_OPTIONS="--max-old-space-size=16384"

CMD ["node", "cli.ts", "run", "--plugins-dir=/plugins", "--data-dir=/data"]
