// EVM utilities for blockchain data processing

// Standard ERC20 Transfer event signature
export const TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export interface AddressSet {
    senders: Set<string>;
    addresses: Set<string>;
}

// Extract addresses from Transfer events in transaction logs
export function extractTransferAddresses(logs: Array<{ topics: string[]; }>): AddressSet {
    const senders = new Set<string>();
    const addresses = new Set<string>();

    for (const log of logs) {
        // Check if this is a Transfer event
        if (log.topics.length >= 3 && log.topics[0] === TRANSFER_EVENT_SIGNATURE) {
            // Extract from address (topics[1])
            const fromTopic = log.topics[1];
            if (fromTopic && fromTopic.length >= 42) {
                const fromAddress = "0x" + fromTopic.slice(-40);
                senders.add(fromAddress);
                addresses.add(fromAddress);
            }

            // Extract to address (topics[2])
            const toTopic = log.topics[2];
            if (toTopic && toTopic.length >= 42) {
                const toAddress = "0x" + toTopic.slice(-40);
                addresses.add(toAddress);
            }
        }
    }

    return { senders, addresses };
}

// Count CREATE calls in a trace recursively
export function countCreateCallsInTrace(trace: any): number {
    let count = 0;

    // Check if this trace is a contract creation
    if (trace.type === 'CREATE' || trace.type === 'CREATE2' || trace.type === 'CREATE3') {
        count = 1;
    }

    // Recursively check nested calls
    if (trace.calls && Array.isArray(trace.calls)) {
        for (const nestedCall of trace.calls) {
            count += countCreateCallsInTrace(nestedCall);
        }
    }

    return count;
}