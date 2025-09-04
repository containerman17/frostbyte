import Database from 'better-sqlite3';

/**
 * Register all custom SQLite functions on a database instance
 */
export function registerAllCustomFunctions(db: Database.Database): void {
    registerUint256Functions(db);
    // Add more custom function registrations here as needed
}

/**
 * Register uint256 aggregate and scalar functions
 */
function registerUint256Functions(db: Database.Database): void {
    // CUSTOM_SUM_UINT256 - sums uint256 values stored as blobs
    db.aggregate('CUSTOM_SUM_UINT256', {
        start: () => BigInt(0),
        step: (acc: any, val: any) => {
            if (!val) return acc;
            return acc + blobToUint256(val as Buffer);
        },
        result: (acc: any) => uint256ToBlob(acc as bigint)
    });

    // Scalar functions for convenience
    db.function('TO_UINT256', (value: string | number | null) => {
        if (value === null) return null;
        return uint256ToBlob(BigInt(value));
    });

    db.function('FROM_UINT256', (blob: Buffer | null) => {
        if (!blob) return null;
        return blobToUint256(blob).toString();
    });

    // Add two uint256 blobs
    db.function('UINT256_ADD', (a: Buffer | null, b: Buffer | null) => {
        if (!a && !b) return null;
        if (!a) return b;
        if (!b) return a;
        const sum = blobToUint256(a) + blobToUint256(b);
        return uint256ToBlob(sum);
    });
}

/**
 * Convert a string/number/bigint to a variable-length blob (up to uint256)
 * Stores only the necessary bytes, similar to Ethereum's compact encoding
 */
export function uint256ToBlob(value: string | number | bigint): Buffer {
    const num = BigInt(value);
    if (num < 0n) {
        throw new Error('uint256 cannot be negative');
    }

    // Handle zero case
    if (num === 0n) {
        return Buffer.alloc(1); // Single zero byte
    }

    // Calculate how many bytes we need
    let temp = num;
    let byteCount = 0;
    while (temp > 0n) {
        byteCount++;
        temp = temp >> 8n;
    }

    // Allocate only the bytes we need (up to 32 for uint256)
    if (byteCount > 32) {
        throw new Error('Value exceeds uint256 max');
    }

    const buf = Buffer.alloc(byteCount);
    temp = num;
    for (let i = byteCount - 1; i >= 0; i--) {
        buf[i] = Number(temp & 0xFFn);
        temp = temp >> 8n;
    }

    return buf;
}

/**
 * Convert a variable-length blob to a bigint
 * Accepts any length up to 32 bytes (uint256 max)
 */
export function blobToUint256(blob: Buffer): bigint {
    if (blob.length > 32) {
        throw new Error(`Blob too large for uint256: ${blob.length} bytes`);
    }

    if (blob.length === 0) {
        return BigInt(0);
    }

    let num = BigInt(0);
    for (let i = 0; i < blob.length; i++) {
        num = (num << 8n) | BigInt(blob[i]!);
    }
    return num;
}

/**
 * Convert a variable-length blob to a string
 */
export function blobToUint256String(blob: Buffer): string {
    return blobToUint256(blob).toString();
}
