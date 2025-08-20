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

# Create a tarball of the package and install it globally
# This makes frostbyte-sdk available for external plugins
RUN npm pack && \
    npm install -g frostbyte-sdk-*.tgz && \
    rm frostbyte-sdk-*.tgz

# Set NODE_PATH to include global modules so external plugins can resolve imports
ENV NODE_PATH=/usr/local/lib/node_modules

# Increase Node.js heap size to 16GB and enable TypeScript stripping
ENV NODE_OPTIONS="--max-old-space-size=16384 --experimental-strip-types"

CMD ["frostbyte", "run", "--plugins-dir=/plugins", "--data-dir=/data"]
