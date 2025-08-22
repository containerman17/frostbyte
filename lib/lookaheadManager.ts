import { currentLoad, mem } from "systeminformation";

// Lookahead management class
class LookaheadManager {
    private currentLookahead = 0; // Start more aggressively
    private readonly maxLookahead = 10;
    private readonly minLookahead = 0;
    private readonly cpuThreshold = 0.9;
    private readonly memoryThreshold = 0.9;
    private lastLoggedLookahead = -1;

    constructor() {
        this.startMonitoring();
    }

    private startMonitoring(): void {
        setInterval(async () => {
            try {
                const [cpuData, memData] = await Promise.all([
                    currentLoad(),
                    mem()
                ]);

                const cpuUsage = cpuData.currentLoad / 100;
                const memUsage = (memData.used / memData.total);

                const prevLookahead = this.currentLookahead;

                if (cpuUsage > this.cpuThreshold || memUsage > this.memoryThreshold) {
                    this.currentLookahead = Math.max(this.minLookahead, this.currentLookahead - 1);
                } else {
                    this.currentLookahead = Math.min(this.maxLookahead, this.currentLookahead + 1);
                }

                // Log only when lookahead changes
                if (this.currentLookahead !== prevLookahead || this.lastLoggedLookahead !== this.currentLookahead) {
                    console.log(`[LookaheadManager] Lookahead: ${prevLookahead} -> ${this.currentLookahead} (CPU: ${(cpuUsage * 100).toFixed(1)}%, Mem: ${(memUsage * 100).toFixed(1)}%)`);
                    this.lastLoggedLookahead = this.currentLookahead;
                }
            } catch (error) {
                console.error('[LookaheadManager] Error monitoring system:', error);
            }
        }, 1000 + Math.random() * 100); // Slightly randomize to prevent synchronized checks
    }

    getCurrentLookahead(): number {
        return this.currentLookahead;
    }
}
export type { LookaheadManager };
export const lookaheadManager = new LookaheadManager();
