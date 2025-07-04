import { LazyTx } from "../../../blockFetcher/lazy/LazyTx";

export function countTransactions(txs: LazyTx[]): number {
    return txs.length;
} 
