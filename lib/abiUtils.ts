import { toEventSignature, AbiItem as ViemAbiItem, keccak256, toHex } from 'viem';

export type AbiItem = ViemAbiItem;

export function getEventHashesMap(abi: AbiItem[]): Map<string, string> {
    const events = abi.filter(x => x.type === "event");
    const eventHashes: Map<string, string> = new Map();
    for (const event of events) {
        const signature = toEventSignature(event);
        const hash = keccak256(toHex(signature));
        eventHashes.set(hash, event.name);
    }
    return eventHashes;
}
