import cluster from 'node:cluster';
import { getRateLimitClient, startRateLimitServer } from './ipcQueue.ts';

// Define rate limits for different services
const limits = {
    'slow-api': { rps: 100, concurrentRequests: 3 }, // Test concurrency limit
    'fast-api': { rps: 2, concurrentRequests: 10 },  // Test RPS limit  
    'other-api': { rps: 5, concurrentRequests: 2 }   // Different domain
};

if (cluster.isPrimary) {
    console.log(`Primary ${process.pid} starting rate limit server`);

    await startRateLimitServer(limits);

    // Fork 2 worker processes
    cluster.fork();
    cluster.fork();

    // Handle worker exit - kill all and exit when any worker dies
    cluster.on('exit', (worker, code) => {
        console.log(`Worker ${worker.process.pid} died with code ${code}`);
        for (const id in cluster.workers) {
            cluster.workers[id]?.kill();
        }
        process.exit(0);
    });

    // Graceful shutdown
    process.on('SIGINT', () => {
        console.log('\nShutting down cluster...');
        for (const id in cluster.workers) {
            cluster.workers[id]?.kill();
        }
        process.exit(0);
    });

} else {
    const client = getRateLimitClient();
    const workerId = process.pid;

    // Test 1: Concurrency limit (should only run 3 at once despite 6 requests)
    async function testConcurrencyLimit() {
        console.log(`\n[Worker ${workerId}] === TEST 1: Concurrency Limit (max 3 concurrent) ===`);

        const startTime = Date.now();
        const promises = [];

        for (let i = 0; i < 6; i++) {
            promises.push(
                client.execute('slow-api', async () => {
                    const reqStart = Date.now();
                    console.log(`[${workerId}] Slow request ${i} started at +${reqStart - startTime}ms`);

                    await new Promise(resolve => setTimeout(resolve, 1000));

                    const reqEnd = Date.now();
                    console.log(`[${workerId}] Slow request ${i} completed at +${reqEnd - startTime}ms`);
                    return i;
                })
            );
        }

        await Promise.all(promises);
        console.log(`[${workerId}] All slow requests done in ${Date.now() - startTime}ms (should be ~2000ms)`);
    }

    // Test 2: RPS limit (should allow only 2 per second)
    async function testRpsLimit() {
        console.log(`\n[Worker ${workerId}] === TEST 2: RPS Limit (max 2 per second) ===`);

        const startTime = Date.now();
        const promises = [];

        for (let i = 0; i < 6; i++) {
            promises.push(
                client.execute('fast-api', async () => {
                    const reqTime = Date.now();
                    console.log(`[${workerId}] Fast request ${i} executed at +${reqTime - startTime}ms`);
                    return i;
                })
            );
        }

        await Promise.all(promises);
        console.log(`[${workerId}] All fast requests done in ${Date.now() - startTime}ms (should be ~3000ms)`);
    }

    // Test 3: Different domains don't share limits
    async function testSeparateDomains() {
        console.log(`\n[Worker ${workerId}] === TEST 3: Separate Domain Limits ===`);

        const startTime = Date.now();

        // Mix requests to different APIs
        const promises = [
            // 2 to slow-api (3 concurrent limit)
            client.execute('slow-api', async () => {
                console.log(`[${workerId}] Mixed slow-api request at +${Date.now() - startTime}ms`);
                await new Promise(resolve => setTimeout(resolve, 500));
                return 'slow';
            }),
            client.execute('slow-api', async () => {
                console.log(`[${workerId}] Mixed slow-api request at +${Date.now() - startTime}ms`);
                await new Promise(resolve => setTimeout(resolve, 500));
                return 'slow';
            }),

            // 2 to other-api (2 concurrent limit, 5 RPS)
            client.execute('other-api', async () => {
                console.log(`[${workerId}] Mixed other-api request at +${Date.now() - startTime}ms`);
                await new Promise(resolve => setTimeout(resolve, 300));
                return 'other';
            }),
            client.execute('other-api', async () => {
                console.log(`[${workerId}] Mixed other-api request at +${Date.now() - startTime}ms`);
                await new Promise(resolve => setTimeout(resolve, 300));
                return 'other';
            })
        ];

        await Promise.all(promises);
        console.log(`[${workerId}] Mixed domain requests done in ${Date.now() - startTime}ms`);
    }

    // Run tests sequentially
    async function runTests() {
        try {
            await new Promise(resolve => setTimeout(resolve, 200)); // Let server start

            if (workerId % 2 === 0) { // Even worker IDs run first set of tests
                await testConcurrencyLimit();
                await testRpsLimit();
            } else { // Odd worker IDs run different test
                await testSeparateDomains();
            }

            console.log(`[${workerId}] All tests completed`);
            process.exit(0);

        } catch (error) {
            console.error(`[${workerId}] Test error:`, error);
            process.exit(1);
        }
    }

    runTests();
}
