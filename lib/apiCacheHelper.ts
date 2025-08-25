import crypto from 'node:crypto';

/**
 * Calculate a unique version hash for API plugin cache database
 * based on the plugin's own version and all its dependency versions
 */
export function calculateApiCacheVersion(
    apiVersion: number,
    apiName: string,
    requiredIndexers: string[],
    indexerVersions: Map<string, number>
): number {
    // Sort indexers to ensure consistent hash regardless of order
    const sortedIndexers = [...requiredIndexers].sort();

    // Build a string containing all version info
    const versionString = [
        `api:${apiName}:${apiVersion}`,
        ...sortedIndexers.map(name => `${name}:${indexerVersions.get(name)}`)
    ].join('|');

    // Create SHA256 hash and take first 8 bytes as BigInt
    const hash = crypto.createHash('sha256').update(versionString).digest();
    const version = hash.readBigInt64BE(0);

    // Convert to positive number (JS number is safe up to 2^53-1)
    return Number(version & 0x7FFFFFFFFFFFFFFFn);
}
