import { LazyTx } from "../../../blockFetcher/lazy/LazyTx";
import { LazyTraces } from "../../../blockFetcher/lazy/LazyTrace";
import { countCreateCallsInTrace } from "../utils";

export function countContractDeployments(txs: LazyTx[], traces: LazyTraces | undefined): number {
    if (!traces) {
        // Fallback to current method when traces are unavailable
        return txs.filter(tx => tx.contractAddress).length;
    }

    // Count CREATE, CREATE2, and CREATE3 calls from traces
    let contractCount = 0;
    for (const trace of traces.traces) {
        contractCount += countCreateCallsInTrace(trace.result);
    }
    return contractCount;
} 
