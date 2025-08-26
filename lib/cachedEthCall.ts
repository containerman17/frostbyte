import { ChainConfig } from "../config";
import { createPublicClient, http, PublicClient } from 'viem';
import crypto from 'crypto';
import sqlite3 from 'better-sqlite3';
import { getRateLimitClient, RateLimitClient } from './ipcQueue.js';

// Map to store viem clients and their domains per chain
const viemClients = new Map<number, { client: PublicClient; domain: string }>();


function getViemClient(evmChainId: number, chainConfigs: ChainConfig[]): { client: PublicClient; domain: string } {
    if (!viemClients.has(evmChainId)) {
        const chainConfig = chainConfigs.find(c => c.evmChainId === evmChainId);
        if (!chainConfig) {
            throw new Error(`Chain config not found for evmChainId ${evmChainId}`);
        }

        // Extract domain from RPC URL for rate limiting
        let domain: string;
        try {
            const url = new URL(chainConfig.rpcConfig.rpcUrl);
            domain = url.hostname;
        } catch (error) {
            throw new Error(`Failed to parse RPC URL: ${chainConfig.rpcConfig.rpcUrl}`);
        }

        const client = createPublicClient({
            transport: http(chainConfig.rpcConfig.rpcUrl),
        });

        viemClients.set(evmChainId, { client, domain });
    }

    return viemClients.get(evmChainId)!;
}

// Initialize eth_calls table in the API cache database
export function initializeEthCallsTable(db: sqlite3.Database, chainId: string): void {
    const tableName = `eth_calls_${chainId.replace(/[^a-zA-Z0-9_]/g, '_')}`;
    db.exec(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
            call_hash TEXT PRIMARY KEY,
            to_address TEXT NOT NULL,
            data TEXT NOT NULL,
            result TEXT NOT NULL
        );
    `);
}

export async function cachedEthCall(
    evmChainId: number,
    to: `0x${string}`,
    data: `0x${string}`,
    chainConfigs: ChainConfig[],
    cacheDb: sqlite3.Database
): Promise<`0x${string}`> {
    // Get the chain config
    const chainConfig = chainConfigs.find(c => c.evmChainId === evmChainId);
    if (!chainConfig) {
        throw new Error(`Chain config not found for evmChainId ${evmChainId}`);
    }

    const tableName = `eth_calls_${chainConfig.blockchainId.replace(/[^a-zA-Z0-9_]/g, '_')}`;

    // Generate cache key by hashing to + data
    const callHash = crypto.createHash('sha256')
        .update(to.toLowerCase())
        .update(data.toLowerCase())
        .digest('hex');

    // Check cache
    const cached = cacheDb.prepare(`
        SELECT result FROM ${tableName} WHERE call_hash = ?
    `).get(callHash) as { result: string } | undefined;

    if (cached) {
        return cached.result as `0x${string}`;
    }

    // Make the actual call with rate limiting
    const { client, domain } = getViemClient(evmChainId, chainConfigs);
    const result = await getRateLimitClient().execute(domain, async () => {
        return client.call({
            to,
            data
        });
    });

    // Ensure result is properly formatted
    const resultHex = (result?.data || '0x') as `0x${string}`;

    // Store in cache
    cacheDb.prepare(`
        INSERT INTO ${tableName} (call_hash, to_address, data, result)
        VALUES (?, ?, ?, ?)
    `).run(
        callHash,
        to.toLowerCase(),
        data.toLowerCase(),
        resultHex
    );

    return resultHex;
}
