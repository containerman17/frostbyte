# Migration from Hono to Fastify

## Overview
This document summarizes the migration from Hono framework to Fastify for the blockchain indexer API server.

## Key Changes

### 1. Dependencies
- **Removed**: `@hono/node-server`, `@hono/zod-openapi`, `hono`
- **Added**: `fastify`, `@fastify/swagger`, `@fastify/swagger-ui`

### 2. Type Definitions
Updated `lib/types.ts`:
- Changed `OpenAPIHono` to `FastifyInstance`
- Updated `registerRoutes` function signature

### 3. Server Implementation
Updated `server.ts`:
- Replaced Hono server with Fastify instance
- Integrated `@fastify/swagger` for OpenAPI documentation
- Added `@fastify/swagger-ui` for interactive API documentation
- Changed route registration to use Fastify's approach

### 4. Plugin Updates
All plugins in `pluginExamples/` were updated:
- Replaced Zod schemas with JSON Schema format
- Changed route registration from `createRoute` to Fastify's `app.get`/`app.post` methods
- Updated request/response handling to use Fastify's API
- Changed error responses to use `reply.code().send()`

### 5. Route Schema Format
Migrated from Zod schemas to JSON Schema:
```javascript
// Before (Zod)
const QuerySchema = z.object({
    startTimestamp: z.coerce.number().optional(),
    timeInterval: z.enum(['hour', 'day']).optional().default('hour')
});

// After (JSON Schema)
const querySchema = {
    type: 'object',
    properties: {
        startTimestamp: { type: 'number' },
        timeInterval: { 
            type: 'string',
            enum: ['hour', 'day'],
            default: 'hour'
        }
    }
};
```

### 6. API Documentation
- OpenAPI spec available at: `/api/openapi.json`
- Interactive documentation at: `/docs` (using Stoplight Elements)
- Swagger UI available at: `/documentation`

## Benefits of Migration
1. **Better Performance**: Fastify is known for its high performance
2. **Standard JSON Schema**: More widely adopted than Zod schemas
3. **Native TypeScript Support**: Better type inference
4. **Extensive Plugin Ecosystem**: Large collection of official and community plugins
5. **Built-in Validation**: JSON Schema validation built into the framework

## Testing
Run the test script to verify the migration:
```bash
npx tsx test-fastify-api.ts
```

Visit http://localhost:3001/docs to see the API documentation.