import cluster from 'node:cluster';

let isServerStarted = false;

interface QueueLimits {
    rps: number;
    concurrentRequests: number;
}

interface PendingRequest {
    workerId: number;
    requestId: string;
    queueName: string;
}

class RateLimitServer {
    private queues: Map<string, {
        limits: QueueLimits;
        currentRequests: number;
        requestsThisSecond: number;
        pendingRequests: PendingRequest[];
    }>;

    private limits: Record<string, QueueLimits>;
    private defaultLimit: QueueLimits;

    constructor(limits: Record<string, QueueLimits>, defaultLimit: QueueLimits) {
        this.limits = limits;
        this.defaultLimit = defaultLimit;
        this.queues = new Map();
        for (const [queueName, queueLimits] of Object.entries(limits)) {
            this.queues.set(queueName, {
                limits: queueLimits,
                currentRequests: 0,
                requestsThisSecond: 0,
                pendingRequests: []
            });
        }
    }

    public async start() {
        if (!cluster.isPrimary) {
            throw new Error('RateLimitServer must be started from primary process');
        }

        setInterval(() => {
            for (const queue of this.queues.values()) {
                queue.requestsThisSecond = 0;
                this.processPendingRequests();
            }
        }, 1000);

        cluster.on('message', (worker, message) => {
            if (message.type === 'acquire') {
                this.handleAcquire(worker.id!, message.queueName, message.requestId);
            } else if (message.type === 'release') {
                this.handleRelease(worker.id!, message.queueName);
            }
        });

        console.log('RateLimitServer started');
    }

    private handleAcquire(workerId: number, queueName: string, requestId: string) {
        let queue = this.queues.get(queueName);
        if (!queue) {
            // Create new queue with default limits for unknown domains
            queue = {
                limits: this.defaultLimit,
                currentRequests: 0,
                requestsThisSecond: 0,
                pendingRequests: []
            };
            this.queues.set(queueName, queue);
            console.log(`Created new queue '${queueName}' with default limits: RPS=${this.defaultLimit.rps}, Concurrent=${this.defaultLimit.concurrentRequests}`);
        }

        if (queue.currentRequests < queue.limits.concurrentRequests &&
            queue.requestsThisSecond < queue.limits.rps) {
            queue.currentRequests++;
            queue.requestsThisSecond++;

            const worker = cluster.workers?.[workerId];
            if (worker) {
                worker.send({
                    type: 'acquired',
                    requestId,
                    queueName
                });
            }
        } else {
            queue.pendingRequests.push({ workerId, requestId, queueName });
        }
    }

    private handleRelease(workerId: number, queueName: string) {
        const queue = this.queues.get(queueName);
        if (!queue) {
            return;
        }

        queue.currentRequests = Math.max(0, queue.currentRequests - 1);
        this.processPendingRequests();
    }

    private processPendingRequests() {
        for (const [queueName, queue] of this.queues) {
            const toProcess: PendingRequest[] = [];

            while (queue.pendingRequests.length > 0 &&
                queue.currentRequests < queue.limits.concurrentRequests &&
                queue.requestsThisSecond < queue.limits.rps) {
                const request = queue.pendingRequests.shift()!;
                toProcess.push(request);
                queue.currentRequests++;
                queue.requestsThisSecond++;
            }

            for (const request of toProcess) {
                const worker = cluster.workers?.[request.workerId];
                if (worker) {
                    worker.send({
                        type: 'acquired',
                        requestId: request.requestId,
                        queueName: request.queueName
                    });
                } else {
                    queue.currentRequests--;
                }
            }
        }
    }
}

export function startRateLimitServer(
    limits: Record<string, QueueLimits>,
    defaultLimit: QueueLimits = { rps: 10, concurrentRequests: 5 }
) {
    if (isServerStarted) {
        throw new Error('RateLimitServer already started');
    }
    isServerStarted = true;
    return (new RateLimitServer(limits, defaultLimit)).start();
}

interface QueuedTask<T> {
    execute: () => Promise<T>;
    resolve: (value: T) => void;
    reject: (error: Error) => void;
}

export class RateLimitClient {
    private pendingRequests: Map<string, {
        resolve: () => void;
        reject: (error: Error) => void;
    }> = new Map();

    // Local queue for each domain to manage backpressure
    private localQueues: Map<string, QueuedTask<any>[]> = new Map();
    private activeRequests: Map<string, number> = new Map();

    constructor() {
        if (cluster.isPrimary) {
            throw new Error('RateLimitClient must be created in worker process');
        }

        // Listen for responses from primary
        process.on('message', (message: any) => {
            if (message.type === 'acquired') {
                const pending = this.pendingRequests.get(message.requestId);
                if (pending) {
                    this.pendingRequests.delete(message.requestId);
                    pending.resolve();
                }
            } else if (message.type === 'error') {
                const pending = this.pendingRequests.get(message.requestId);
                if (pending) {
                    this.pendingRequests.delete(message.requestId);
                    pending.reject(new Error(message.error));
                }
            }
        });
    }

    /**
     * Execute a function with rate limiting. Automatically handles acquire/release.
     * Queues requests locally and processes them as slots become available.
     */
    public async execute<T>(queueName: string, fn: () => Promise<T>): Promise<T> {
        return new Promise((resolve, reject) => {
            // Add to local queue
            if (!this.localQueues.has(queueName)) {
                this.localQueues.set(queueName, []);
            }

            this.localQueues.get(queueName)!.push({
                execute: fn,
                resolve,
                reject
            });

            // Process queue
            this.processQueue(queueName);
        });
    }

    private async processQueue(queueName: string) {
        const queue = this.localQueues.get(queueName);
        if (!queue || queue.length === 0) return;

        // Check if we're already processing maximum for this queue
        const activeCount = this.activeRequests.get(queueName) || 0;

        // Process tasks one by one as slots become available
        while (queue.length > 0) {
            const task = queue.shift()!;

            try {
                // Wait for slot from server
                await this.acquire(queueName);

                // Track active request
                this.activeRequests.set(queueName, activeCount + 1);

                // Execute task asynchronously (don't await here to allow parallel processing)
                this.executeTask(queueName, task);
            } catch (error) {
                task.reject(error as Error);
            }
        }
    }

    private async executeTask<T>(queueName: string, task: QueuedTask<T>) {
        try {
            const result = await task.execute();
            task.resolve(result);
        } catch (error) {
            task.reject(error as Error);
        } finally {
            // Release slot
            await this.release(queueName);

            // Update active count
            const activeCount = this.activeRequests.get(queueName) || 1;
            this.activeRequests.set(queueName, activeCount - 1);

            // Process next in queue if any
            this.processQueue(queueName);
        }
    }

    private async acquire(queueName: string): Promise<void> {
        const requestId = `${process.pid}-${Date.now()}-${Math.random()}`;

        return new Promise((resolve, reject) => {
            this.pendingRequests.set(requestId, { resolve, reject });

            process.send?.({
                type: 'acquire',
                queueName,
                requestId
            });
        });
    }

    private async release(queueName: string): Promise<void> {
        process.send?.({
            type: 'release',
            queueName
        });
    }
}

let client: RateLimitClient | null = null;

export function getRateLimitClient(): RateLimitClient {
    if (!client) {
        client = new RateLimitClient();
    }
    return client;
}
