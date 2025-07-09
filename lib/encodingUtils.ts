import { utils } from "@avalabs/avalanchejs";

// Simple LRU cache implementation
class LRUCache<K, V> {
    private cache = new Map<K, V>();
    private readonly maxSize: number;

    constructor(maxSize: number) {
        this.maxSize = maxSize;
    }

    get(key: K): V | undefined {
        const value = this.cache.get(key);
        if (value !== undefined) {
            // Move to end (most recently used)
            this.cache.delete(key);
            this.cache.set(key, value);
        }
        return value;
    }

    set(key: K, value: V): void {
        // Remove if exists to update position
        this.cache.delete(key);

        // Add to end
        this.cache.set(key, value);

        // Remove oldest if over capacity
        if (this.cache.size > this.maxSize) {
            const firstKey = this.cache.keys().next().value!;
            this.cache.delete(firstKey);
        }
    }
}

// Create caches with reasonable size (adjust based on your usage patterns)
const hexToCB58Cache = new LRUCache<string, string>(1000);
const cb58ToHexCache = new LRUCache<string, string>(1000);

export function hexToCB58(hex: string): string {
    const cached = hexToCB58Cache.get(hex);
    if (cached !== undefined) {
        return cached;
    }

    const bytes = utils.hexToBuffer(hex);
    const result = utils.base58check.encode(bytes);
    hexToCB58Cache.set(hex, result);
    return result;
}

export function CB58ToHex(cb58: string): string {
    const cached = cb58ToHexCache.get(cb58);
    if (cached !== undefined) {
        return cached;
    }

    const bytes = utils.base58check.decode(cb58);
    const result = utils.bufferToHex(bytes);
    cb58ToHexCache.set(cb58, result);
    return result;
}
