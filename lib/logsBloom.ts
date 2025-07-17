import { keccak256, toBytes } from "viem";
import type { RpcReceiptLog, RpcTxReceipt, RpcBlock } from "../blockFetcher/evmTypes";

function addToBloom(bloom: Uint8Array, value: string) {
    const hash = keccak256(toBytes(value), 'bytes');
    for (let i = 0; i < 6; i += 2) {
        const bit = ((hash[i]! << 8) | hash[i + 1]!) & 2047;
        const index = 256 - 1 - Math.floor(bit / 8);
        bloom[index] = bloom[index]! | (1 << (bit % 8));
    }
}

export function computeLogsBloom(logs: RpcReceiptLog[]): string {
    const bloom = new Uint8Array(256);
    for (const log of logs) {
        addToBloom(bloom, log.address);
        for (const topic of log.topics) {
            addToBloom(bloom, topic);
        }
    }
    return '0x' + Buffer.from(bloom).toString('hex');
}

export function addLogsBloomToReceipt(receipt: Omit<RpcTxReceipt, 'logsBloom'>): RpcTxReceipt {
    const logsBloom = computeLogsBloom(receipt.logs);
    return { ...receipt, logsBloom };
}

export function addLogsBloomToBlock(block: Omit<RpcBlock, 'logsBloom'>, receipts: RpcTxReceipt[]): RpcBlock {
    const allLogs: RpcReceiptLog[] = [];
    for (const receipt of receipts) {
        allLogs.push(...receipt.logs);
    }
    const logsBloom = computeLogsBloom(allLogs);
    return { ...block, logsBloom };
}
