import { describe, it } from 'node:test';
import assert from 'node:assert';
import Database from 'better-sqlite3';
import { uint256ToBlob, blobToUint256, blobToUint256String, registerAllCustomFunctions } from './db-functions.ts';

describe('uint256 variable-length encoding', () => {
    describe('uint256ToBlob', () => {
        it('should encode zero as 1 byte', () => {
            const blob = uint256ToBlob(0);
            assert.strictEqual(blob.length, 1);
            assert.strictEqual(blob[0], 0);
        });

        it('should encode small numbers efficiently', () => {
            const tests = [
                { value: 1, expectedLength: 1, expectedHex: '01' },
                { value: 255, expectedLength: 1, expectedHex: 'ff' },
                { value: 256, expectedLength: 2, expectedHex: '0100' },
                { value: 65535, expectedLength: 2, expectedHex: 'ffff' },
                { value: 65536, expectedLength: 3, expectedHex: '010000' },
                { value: 16777215, expectedLength: 3, expectedHex: 'ffffff' },
                { value: 16777216, expectedLength: 4, expectedHex: '01000000' },
            ];

            for (const test of tests) {
                const blob = uint256ToBlob(test.value);
                assert.strictEqual(blob.length, test.expectedLength, `Value ${test.value} should be ${test.expectedLength} bytes`);
                assert.strictEqual(blob.toString('hex'), test.expectedHex, `Value ${test.value} hex mismatch`);
            }
        });

        it('should encode typical Ethereum values correctly', () => {
            // 1 ETH in wei
            const oneEth = '1000000000000000000';
            const blob = uint256ToBlob(oneEth);
            assert.strictEqual(blob.length, 8);
            assert.strictEqual(blob.toString('hex'), '0de0b6b3a7640000');
        });

        it('should handle max uint256', () => {
            const maxUint256 = '115792089237316195423570985008687907853269984665640564039457584007913129639935';
            const blob = uint256ToBlob(maxUint256);
            assert.strictEqual(blob.length, 32);
            assert.strictEqual(blob.toString('hex'), 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
        });

        it('should throw on negative numbers', () => {
            assert.throws(() => uint256ToBlob(-1), /cannot be negative/);
            assert.throws(() => uint256ToBlob('-100'), /cannot be negative/);
        });

        it('should throw on values exceeding uint256', () => {
            const tooLarge = '115792089237316195423570985008687907853269984665640564039457584007913129639936'; // max + 1
            assert.throws(() => uint256ToBlob(tooLarge), /exceeds uint256/);
        });
    });

    describe('blobToUint256', () => {
        it('should decode zero correctly', () => {
            assert.strictEqual(blobToUint256(Buffer.from([0])), 0n);
            assert.strictEqual(blobToUint256(Buffer.alloc(0)), 0n); // empty buffer
        });

        it('should decode various byte lengths', () => {
            const tests = [
                { hex: '01', expected: 1n },
                { hex: 'ff', expected: 255n },
                { hex: '0100', expected: 256n },
                { hex: 'ffff', expected: 65535n },
                { hex: '010000', expected: 65536n },
                { hex: 'ffffff', expected: 16777215n },
                { hex: '01000000', expected: 16777216n },
                { hex: '0de0b6b3a7640000', expected: 1000000000000000000n },
            ];

            for (const test of tests) {
                const result = blobToUint256(Buffer.from(test.hex, 'hex'));
                assert.strictEqual(result, test.expected, `Hex ${test.hex} should decode to ${test.expected}`);
            }
        });

        it('should handle max uint256', () => {
            const maxBlob = Buffer.from('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 'hex');
            const result = blobToUint256(maxBlob);
            assert.strictEqual(result.toString(), '115792089237316195423570985008687907853269984665640564039457584007913129639935');
        });

        it('should throw on blobs larger than 32 bytes', () => {
            const tooLarge = Buffer.alloc(33);
            assert.throws(() => blobToUint256(tooLarge), /too large for uint256/);
        });
    });

    describe('round-trip encoding/decoding', () => {
        it('should preserve values through encode/decode cycle', () => {
            const testValues = [
                '0',
                '1',
                '127',
                '128',
                '255',
                '256',
                '32767',
                '32768',
                '65535',
                '65536',
                '8388607',
                '8388608',
                '16777215',
                '16777216',
                '2147483647',
                '2147483648',
                '4294967295',
                '4294967296',
                '1000000000000000000', // 1 ETH
                '79228162514264337593543950335', // ~2^96
                '115792089237316195423570985008687907853269984665640564039457584007913129639935', // max uint256
            ];

            for (const value of testValues) {
                const blob = uint256ToBlob(value);
                const recovered = blobToUint256String(blob);
                assert.strictEqual(recovered, value, `Failed round-trip for ${value}`);
            }
        });
    });

    describe('SQLite custom functions', () => {
        it('should sum uint256 values correctly', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            db.exec(`
                CREATE TABLE test (
                    value BLOB
                )
            `);

            const insert = db.prepare('INSERT INTO test VALUES (?)');

            // Insert values of different byte sizes
            insert.run(uint256ToBlob('100'));        // 1 byte
            insert.run(uint256ToBlob('1000'));       // 2 bytes
            insert.run(uint256ToBlob('100000'));     // 3 bytes
            insert.run(uint256ToBlob('10000000'));   // 4 bytes

            const result = db.prepare('SELECT CUSTOM_SUM_UINT256(value) as total FROM test').get() as { total: Buffer };
            const sum = blobToUint256String(result.total);

            assert.strictEqual(sum, '10101100'); // 100 + 1000 + 100000 + 10000000

            db.close();
        });

        it('should handle TO_UINT256 and FROM_UINT256 functions', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            // Test TO_UINT256
            const toResult = db.prepare("SELECT TO_UINT256(?) as blob").get('123456789') as { blob: Buffer };
            assert.strictEqual(blobToUint256String(toResult.blob), '123456789');
            assert.strictEqual(toResult.blob.length, 4); // Should be 4 bytes for this value

            // Test FROM_UINT256
            const blob = uint256ToBlob('987654321');
            const fromResult = db.prepare("SELECT FROM_UINT256(?) as value").get(blob) as { value: string };
            assert.strictEqual(fromResult.value, '987654321');

            // Test NULL handling
            const nullTo = db.prepare("SELECT TO_UINT256(NULL) as blob").get() as { blob: Buffer | null };
            assert.strictEqual(nullTo.blob, null);

            const nullFrom = db.prepare("SELECT FROM_UINT256(NULL) as value").get() as { value: string | null };
            assert.strictEqual(nullFrom.value, null);

            db.close();
        });

        it('should handle empty tables and NULL values in aggregation', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            db.exec(`CREATE TABLE test (value BLOB)`);

            // Empty table
            const emptyResult = db.prepare('SELECT CUSTOM_SUM_UINT256(value) as total FROM test').get() as { total: Buffer };
            assert.strictEqual(blobToUint256String(emptyResult.total), '0');

            // Table with NULLs - need to use prepared statement for parameters
            const insertNulls = db.prepare('INSERT INTO test VALUES (?)');
            insertNulls.run(null);
            insertNulls.run(uint256ToBlob('500'));
            insertNulls.run(null);
            insertNulls.run(uint256ToBlob('500'));

            const withNulls = db.prepare('SELECT CUSTOM_SUM_UINT256(value) as total FROM test').get() as { total: Buffer };
            assert.strictEqual(blobToUint256String(withNulls.total), '1000');

            db.close();
        });

        it('should work with GROUP BY', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            db.exec(`
                CREATE TABLE balances (
                    category TEXT,
                    amount BLOB
                )
            `);

            const insert = db.prepare('INSERT INTO balances VALUES (?, ?)');
            insert.run('A', uint256ToBlob('100'));
            insert.run('A', uint256ToBlob('200'));
            insert.run('B', uint256ToBlob('1000'));
            insert.run('B', uint256ToBlob('2000'));
            insert.run('B', uint256ToBlob('3000'));

            const results = db.prepare(`
                SELECT category, CUSTOM_SUM_UINT256(amount) as total 
                FROM balances 
                GROUP BY category 
                ORDER BY category
            `).all() as Array<{ category: string, total: Buffer }>;

            assert.strictEqual(results.length, 2);
            assert.strictEqual(blobToUint256String(results[0]!.total), '300');  // A: 100 + 200
            assert.strictEqual(blobToUint256String(results[1]!.total), '6000'); // B: 1000 + 2000 + 3000

            db.close();
        });

        it('should handle UINT256_ADD function correctly', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            // Test adding two normal values
            const blob1 = uint256ToBlob('123456789');
            const blob2 = uint256ToBlob('987654321');
            const result1 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(blob1, blob2) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result1.sum), '1111111110');

            // Test adding with zero
            const zeroBlob = uint256ToBlob('0');
            const result2 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(blob1, zeroBlob) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result2.sum), '123456789');

            // Test adding zero to zero
            const result3 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(zeroBlob, zeroBlob) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result3.sum), '0');

            // Test large values that exceed uint64 (max uint64 = ~18.4 * 10^18)
            const thousandEth = uint256ToBlob('1000000000000000000000'); // 1000 ETH = 10^21 wei (exceeds uint64)
            const twoThousandEth = uint256ToBlob('2000000000000000000000'); // 2000 ETH = 2 * 10^21 wei
            const result4 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(thousandEth, twoThousandEth) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result4.sum), '3000000000000000000000');

            // Test with values of different byte lengths
            const small = uint256ToBlob('255'); // 1 byte
            const medium = uint256ToBlob('65536'); // 3 bytes
            const result5 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(small, medium) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result5.sum), '65791');

            db.close();
        });

        it('should handle NULL values in UINT256_ADD', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            const blob = uint256ToBlob('500');

            // Test both NULL
            const result1 = db.prepare('SELECT UINT256_ADD(NULL, NULL) as sum').get() as { sum: Buffer | null };
            assert.strictEqual(result1.sum, null);

            // Test first NULL
            const result2 = db.prepare('SELECT UINT256_ADD(NULL, ?) as sum').get(blob) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result2.sum), '500');

            // Test second NULL
            const result3 = db.prepare('SELECT UINT256_ADD(?, NULL) as sum').get(blob) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result3.sum), '500');

            db.close();
        });

        it('should use UINT256_ADD in complex queries', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            db.exec(`
                CREATE TABLE transactions (
                    id INTEGER PRIMARY KEY,
                    value BLOB,
                    fee BLOB
                )
            `);

            const insert = db.prepare('INSERT INTO transactions (value, fee) VALUES (?, ?)');
            insert.run(uint256ToBlob('1000'), uint256ToBlob('10'));
            insert.run(uint256ToBlob('2000'), uint256ToBlob('20'));
            insert.run(uint256ToBlob('3000'), uint256ToBlob('30'));

            // Test adding value and fee columns
            const results = db.prepare(`
                SELECT id, UINT256_ADD(value, fee) as total
                FROM transactions
                ORDER BY id
            `).all() as Array<{ id: number, total: Buffer }>;

            assert.strictEqual(results.length, 3);
            assert.strictEqual(blobToUint256String(results[0]!.total), '1010');
            assert.strictEqual(blobToUint256String(results[1]!.total), '2020');
            assert.strictEqual(blobToUint256String(results[2]!.total), '3030');

            // Test chaining additions
            const chainResult = db.prepare(`
                SELECT UINT256_ADD(UINT256_ADD(?, ?), ?) as total
            `).get(
                uint256ToBlob('100'),
                uint256ToBlob('200'),
                uint256ToBlob('300')
            ) as { total: Buffer };
            assert.strictEqual(blobToUint256String(chainResult.total), '600');

            db.close();
        });

        it('should handle edge cases in UINT256_ADD', () => {
            const db = new Database(':memory:');
            registerAllCustomFunctions(db);

            // Test adding near max values (should work as long as result fits in uint256)
            const halfMax = '57896044618658097711785492504343953926634992332820282019728792003956564819967';
            const blob1 = uint256ToBlob(halfMax);
            const blob2 = uint256ToBlob(halfMax);
            const result = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(blob1, blob2) as { sum: Buffer };
            // halfMax + halfMax = max uint256 - 1
            assert.strictEqual(blobToUint256String(result.sum), '115792089237316195423570985008687907853269984665640564039457584007913129639934');

            // Test adding 1 to get exactly max uint256
            const almostMax = '115792089237316195423570985008687907853269984665640564039457584007913129639934';
            const one = '1';
            const blob3 = uint256ToBlob(almostMax);
            const blob4 = uint256ToBlob(one);
            const result2 = db.prepare('SELECT UINT256_ADD(?, ?) as sum').get(blob3, blob4) as { sum: Buffer };
            assert.strictEqual(blobToUint256String(result2.sum), '115792089237316195423570985008687907853269984665640564039457584007913129639935');

            db.close();
        });
    });
});
