import { BlockDB } from "./blockFetcher/BlockDB"
import { LazyTx, lazyTxToReceipt } from "./blockFetcher/lazy/LazyTx";
import { LazyBlock, lazyBlockToBlock } from "./blockFetcher/lazy/LazyBlock";
import { LazyTraces } from "./blockFetcher/lazy/LazyTrace";
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt } from "./blockFetcher/evmTypes";
import { pack, unpack } from 'msgpackr';
import { compressSync as lz4CompressSync, uncompressSync as lz4UncompressSync } from 'lz4-napi';
import * as fs from 'fs';
import * as path from 'path';
import { tmpdir } from 'os';
import { execSync } from 'child_process';
import { zstdCompressSync, zstdDecompressSync, constants as zlibConstants } from 'node:zlib';
import * as zstd from 'zstd-napi';
import { Table } from 'console-table-printer';
const BENCHMARK_BLOCKS = 10000;
const DICT_PERCENTAGE = 0.50
const dbPath = "./database/C-Chain/blocks_no_dbg.db"
const HAS_DEBUG = false;
const HEX_RE = /^0x[0-9a-f]+$/i;

// Smart hex conversion - only convert to buffer if it saves space
function smartHex2Bin(hex: string): string | Buffer {
    // For hex strings, the break-even point is around 6-8 characters
    // "0x1234" (6 chars) vs Buffer (2 bytes + msgpack overhead)
    if (hex.length <= 10) { // "0x" + 8 hex chars = 4 bytes
        return hex; // Keep as string for small values
    }
    return Buffer.from(hex.slice(2), 'hex');
}

function smartBin2Hex(value: string | Buffer): string {
    if (typeof value === 'string') {
        return value;
    }
    return '0x' + value.toString('hex');
}

// Ultra-optimized hex conversion functions
function ultraOptimizedHex2BinBlock(block: RpcBlock): any {
    return {
        // Hashes are always 32 bytes - always convert
        hash: Buffer.from(block.hash.slice(2), 'hex'),
        parentHash: Buffer.from(block.parentHash.slice(2), 'hex'),
        stateRoot: Buffer.from(block.stateRoot.slice(2), 'hex'),
        transactionsRoot: Buffer.from(block.transactionsRoot.slice(2), 'hex'),
        receiptsRoot: Buffer.from(block.receiptsRoot.slice(2), 'hex'),
        mixHash: Buffer.from(block.mixHash.slice(2), 'hex'),
        sha3Uncles: Buffer.from(block.sha3Uncles.slice(2), 'hex'),

        // Addresses are always 20 bytes - always convert
        miner: Buffer.from(block.miner.slice(2), 'hex'),

        // Large data fields - always convert
        logsBloom: Buffer.from(block.logsBloom.slice(2), 'hex'), // 256 bytes
        extraData: Buffer.from(block.extraData.slice(2), 'hex'),

        // Numeric fields - use smart conversion
        number: smartHex2Bin(block.number),
        timestamp: smartHex2Bin(block.timestamp),
        gasLimit: smartHex2Bin(block.gasLimit),
        gasUsed: smartHex2Bin(block.gasUsed),
        baseFeePerGas: block.baseFeePerGas ? smartHex2Bin(block.baseFeePerGas) : undefined,
        difficulty: smartHex2Bin(block.difficulty),
        totalDifficulty: smartHex2Bin(block.totalDifficulty),
        size: smartHex2Bin(block.size),
        nonce: smartHex2Bin(block.nonce), // Usually 8 bytes, borderline

        // Arrays
        uncles: block.uncles.map(u => Buffer.from(u.slice(2), 'hex')), // Hashes
        transactions: block.transactions.map(ultraOptimizedHex2BinTx),

        // Optional fields
        blobGasUsed: block.blobGasUsed ? smartHex2Bin(block.blobGasUsed) : undefined,
        excessBlobGas: block.excessBlobGas ? smartHex2Bin(block.excessBlobGas) : undefined,
        parentBeaconBlockRoot: block.parentBeaconBlockRoot ? Buffer.from(block.parentBeaconBlockRoot.slice(2), 'hex') : undefined,
        blockGasCost: block.blockGasCost ? smartHex2Bin(block.blockGasCost) : undefined,
        blockExtraData: block.blockExtraData ? smartHex2Bin(block.blockExtraData) : undefined,
        extDataHash: block.extDataHash ? Buffer.from(block.extDataHash.slice(2), 'hex') : undefined,
    };
}

function ultraOptimizedHex2BinTx(tx: RpcBlockTransaction): any {
    return {
        // Hashes - always convert
        hash: Buffer.from(tx.hash.slice(2), 'hex'),
        blockHash: Buffer.from(tx.blockHash.slice(2), 'hex'),
        r: Buffer.from(tx.r.slice(2), 'hex'), // 32 bytes
        s: Buffer.from(tx.s.slice(2), 'hex'), // 32 bytes

        // Addresses - always convert
        from: Buffer.from(tx.from.slice(2), 'hex'),
        to: tx.to ? Buffer.from(tx.to.slice(2), 'hex') : null,

        // Data fields - usually large, always convert
        input: Buffer.from(tx.input.slice(2), 'hex'),

        // Numeric fields - use smart conversion
        blockNumber: smartHex2Bin(tx.blockNumber),
        transactionIndex: smartHex2Bin(tx.transactionIndex),
        value: smartHex2Bin(tx.value),
        gas: smartHex2Bin(tx.gas),
        gasPrice: smartHex2Bin(tx.gasPrice),
        nonce: smartHex2Bin(tx.nonce),
        type: smartHex2Bin(tx.type), // Usually small (0x0, 0x1, 0x2)
        chainId: smartHex2Bin(tx.chainId), // Usually small
        v: smartHex2Bin(tx.v),

        // Optional EIP-1559 fields
        maxFeePerGas: tx.maxFeePerGas ? smartHex2Bin(tx.maxFeePerGas) : undefined,
        maxPriorityFeePerGas: tx.maxPriorityFeePerGas ? smartHex2Bin(tx.maxPriorityFeePerGas) : undefined,

        // Access list
        accessList: tx.accessList?.map(entry => ({
            address: Buffer.from(entry.address.slice(2), 'hex'),
            storageKeys: entry.storageKeys.map(key => Buffer.from(key.slice(2), 'hex')) // 32 bytes each
        })),

        yParity: tx.yParity ? smartHex2Bin(tx.yParity) : undefined,
    };
}

function ultraOptimizedHex2BinReceipt(receipt: RpcTxReceipt): any {
    return {
        // Hashes - always convert
        blockHash: Buffer.from(receipt.blockHash.slice(2), 'hex'),
        transactionHash: Buffer.from(receipt.transactionHash.slice(2), 'hex'),

        // Addresses - always convert
        from: Buffer.from(receipt.from.slice(2), 'hex'),
        to: receipt.to ? Buffer.from(receipt.to.slice(2), 'hex') : Buffer.from('0000000000000000000000000000000000000000', 'hex'),
        contractAddress: receipt.contractAddress ? Buffer.from(receipt.contractAddress.slice(2), 'hex') : null,

        // Large data - always convert
        logsBloom: Buffer.from(receipt.logsBloom.slice(2), 'hex'), // 256 bytes

        // Numeric fields - use smart conversion
        blockNumber: smartHex2Bin(receipt.blockNumber),
        transactionIndex: smartHex2Bin(receipt.transactionIndex),
        cumulativeGasUsed: smartHex2Bin(receipt.cumulativeGasUsed),
        effectiveGasPrice: smartHex2Bin(receipt.effectiveGasPrice),
        gasUsed: smartHex2Bin(receipt.gasUsed),
        status: smartHex2Bin(receipt.status), // Usually 0x0 or 0x1
        type: smartHex2Bin(receipt.type),

        // Logs
        logs: receipt.logs.map(log => ({
            address: Buffer.from(log.address.slice(2), 'hex'),
            topics: log.topics.map(t => Buffer.from(t.slice(2), 'hex')), // 32 bytes each
            data: Buffer.from(log.data.slice(2), 'hex'),
            blockNumber: smartHex2Bin(log.blockNumber),
            transactionHash: Buffer.from(log.transactionHash.slice(2), 'hex'),
            transactionIndex: smartHex2Bin(log.transactionIndex),
            blockHash: Buffer.from(log.blockHash.slice(2), 'hex'),
            logIndex: smartHex2Bin(log.logIndex),
            removed: log.removed
        })),
    };
}

// Ultra-optimized reverse functions
function ultraOptimizedBin2HexBlock(block: any): RpcBlock {
    const result: RpcBlock = {
        hash: '0x' + block.hash.toString('hex'),
        number: smartBin2Hex(block.number),
        parentHash: '0x' + block.parentHash.toString('hex'),
        timestamp: smartBin2Hex(block.timestamp),
        gasLimit: smartBin2Hex(block.gasLimit),
        gasUsed: smartBin2Hex(block.gasUsed),
        miner: '0x' + block.miner.toString('hex'),
        difficulty: smartBin2Hex(block.difficulty),
        totalDifficulty: smartBin2Hex(block.totalDifficulty),
        size: smartBin2Hex(block.size),
        stateRoot: '0x' + block.stateRoot.toString('hex'),
        transactionsRoot: '0x' + block.transactionsRoot.toString('hex'),
        receiptsRoot: '0x' + block.receiptsRoot.toString('hex'),
        logsBloom: '0x' + block.logsBloom.toString('hex'),
        extraData: '0x' + block.extraData.toString('hex'),
        mixHash: '0x' + block.mixHash.toString('hex'),
        nonce: smartBin2Hex(block.nonce),
        sha3Uncles: '0x' + block.sha3Uncles.toString('hex'),
        uncles: block.uncles.map((u: Buffer) => '0x' + u.toString('hex')),
        transactions: block.transactions.map(ultraOptimizedBin2HexTx),
    };

    if (block.baseFeePerGas !== undefined) result.baseFeePerGas = smartBin2Hex(block.baseFeePerGas);
    if (block.blobGasUsed !== undefined) result.blobGasUsed = smartBin2Hex(block.blobGasUsed);
    if (block.excessBlobGas !== undefined) result.excessBlobGas = smartBin2Hex(block.excessBlobGas);
    if (block.parentBeaconBlockRoot !== undefined) result.parentBeaconBlockRoot = '0x' + block.parentBeaconBlockRoot.toString('hex');
    if (block.blockGasCost !== undefined) result.blockGasCost = smartBin2Hex(block.blockGasCost);
    if (block.blockExtraData !== undefined) result.blockExtraData = smartBin2Hex(block.blockExtraData);
    if (block.extDataHash !== undefined) result.extDataHash = '0x' + block.extDataHash.toString('hex');

    return result;
}

function ultraOptimizedBin2HexTx(tx: any): RpcBlockTransaction {
    const result: RpcBlockTransaction = {
        hash: '0x' + tx.hash.toString('hex'),
        blockHash: '0x' + tx.blockHash.toString('hex'),
        blockNumber: smartBin2Hex(tx.blockNumber),
        transactionIndex: smartBin2Hex(tx.transactionIndex),
        from: '0x' + tx.from.toString('hex'),
        to: tx.to ? '0x' + tx.to.toString('hex') : null,
        value: smartBin2Hex(tx.value),
        gas: smartBin2Hex(tx.gas),
        gasPrice: smartBin2Hex(tx.gasPrice),
        input: '0x' + tx.input.toString('hex'),
        nonce: smartBin2Hex(tx.nonce),
        type: smartBin2Hex(tx.type),
        chainId: smartBin2Hex(tx.chainId),
        v: smartBin2Hex(tx.v),
        r: '0x' + tx.r.toString('hex'),
        s: '0x' + tx.s.toString('hex'),
    };
    if (tx.maxFeePerGas) result.maxFeePerGas = smartBin2Hex(tx.maxFeePerGas);
    if (tx.maxPriorityFeePerGas) result.maxPriorityFeePerGas = smartBin2Hex(tx.maxPriorityFeePerGas);
    if (tx.accessList) {
        result.accessList = tx.accessList.map((entry: any) => ({
            address: '0x' + entry.address.toString('hex'),
            storageKeys: entry.storageKeys.map((key: Buffer) => '0x' + key.toString('hex'))
        }));
    }
    if (tx.yParity) result.yParity = smartBin2Hex(tx.yParity);
    return result;
}

function ultraOptimizedBin2HexReceipt(receipt: any): RpcTxReceipt {
    return {
        blockHash: '0x' + receipt.blockHash.toString('hex'),
        blockNumber: smartBin2Hex(receipt.blockNumber),
        contractAddress: receipt.contractAddress ? '0x' + receipt.contractAddress.toString('hex') : null,
        cumulativeGasUsed: smartBin2Hex(receipt.cumulativeGasUsed),
        effectiveGasPrice: smartBin2Hex(receipt.effectiveGasPrice),
        from: '0x' + receipt.from.toString('hex'),
        gasUsed: smartBin2Hex(receipt.gasUsed),
        logs: receipt.logs.map((log: any) => ({
            address: '0x' + log.address.toString('hex'),
            topics: log.topics.map((t: Buffer) => '0x' + t.toString('hex')),
            data: '0x' + log.data.toString('hex'),
            blockNumber: smartBin2Hex(log.blockNumber),
            transactionHash: '0x' + log.transactionHash.toString('hex'),
            transactionIndex: smartBin2Hex(log.transactionIndex),
            blockHash: '0x' + log.blockHash.toString('hex'),
            logIndex: smartBin2Hex(log.logIndex),
            removed: log.removed
        })),
        logsBloom: '0x' + receipt.logsBloom.toString('hex'),
        status: smartBin2Hex(receipt.status),
        to: receipt.to ? '0x' + receipt.to.toString('hex') : '0x0000000000000000000000000000000000000000',
        transactionHash: '0x' + receipt.transactionHash.toString('hex'),
        transactionIndex: smartBin2Hex(receipt.transactionIndex),
        type: smartBin2Hex(receipt.type),
    };
}

// Optimized hex conversion functions that only process known hex fields
function optimizedHex2BinBlock(block: RpcBlock): any {
    return {
        hash: Buffer.from(block.hash.slice(2), 'hex'),
        number: Buffer.from(block.number.slice(2), 'hex'),
        parentHash: Buffer.from(block.parentHash.slice(2), 'hex'),
        timestamp: Buffer.from(block.timestamp.slice(2), 'hex'),
        gasLimit: Buffer.from(block.gasLimit.slice(2), 'hex'),
        gasUsed: Buffer.from(block.gasUsed.slice(2), 'hex'),
        baseFeePerGas: block.baseFeePerGas ? Buffer.from(block.baseFeePerGas.slice(2), 'hex') : undefined,
        miner: Buffer.from(block.miner.slice(2), 'hex'),
        difficulty: Buffer.from(block.difficulty.slice(2), 'hex'),
        totalDifficulty: Buffer.from(block.totalDifficulty.slice(2), 'hex'),
        size: Buffer.from(block.size.slice(2), 'hex'),
        stateRoot: Buffer.from(block.stateRoot.slice(2), 'hex'),
        transactionsRoot: Buffer.from(block.transactionsRoot.slice(2), 'hex'),
        receiptsRoot: Buffer.from(block.receiptsRoot.slice(2), 'hex'),
        logsBloom: Buffer.from(block.logsBloom.slice(2), 'hex'),
        extraData: Buffer.from(block.extraData.slice(2), 'hex'),
        mixHash: Buffer.from(block.mixHash.slice(2), 'hex'),
        nonce: Buffer.from(block.nonce.slice(2), 'hex'),
        sha3Uncles: Buffer.from(block.sha3Uncles.slice(2), 'hex'),
        uncles: block.uncles.map(u => Buffer.from(u.slice(2), 'hex')),
        transactions: block.transactions.map(optimizedHex2BinTx),
        blobGasUsed: block.blobGasUsed ? Buffer.from(block.blobGasUsed.slice(2), 'hex') : undefined,
        excessBlobGas: block.excessBlobGas ? Buffer.from(block.excessBlobGas.slice(2), 'hex') : undefined,
        parentBeaconBlockRoot: block.parentBeaconBlockRoot ? Buffer.from(block.parentBeaconBlockRoot.slice(2), 'hex') : undefined,
        blockGasCost: block.blockGasCost ? Buffer.from(block.blockGasCost.slice(2), 'hex') : undefined,
        blockExtraData: block.blockExtraData ? Buffer.from(block.blockExtraData.slice(2), 'hex') : undefined,
        extDataHash: block.extDataHash ? Buffer.from(block.extDataHash.slice(2), 'hex') : undefined,
    };
}

function optimizedHex2BinTx(tx: RpcBlockTransaction): any {
    return {
        hash: Buffer.from(tx.hash.slice(2), 'hex'),
        blockHash: Buffer.from(tx.blockHash.slice(2), 'hex'),
        blockNumber: Buffer.from(tx.blockNumber.slice(2), 'hex'),
        transactionIndex: Buffer.from(tx.transactionIndex.slice(2), 'hex'),
        from: Buffer.from(tx.from.slice(2), 'hex'),
        to: tx.to ? Buffer.from(tx.to.slice(2), 'hex') : null,
        value: Buffer.from(tx.value.slice(2), 'hex'),
        gas: Buffer.from(tx.gas.slice(2), 'hex'),
        gasPrice: Buffer.from(tx.gasPrice.slice(2), 'hex'),
        input: Buffer.from(tx.input.slice(2), 'hex'),
        nonce: Buffer.from(tx.nonce.slice(2), 'hex'),
        type: Buffer.from(tx.type.slice(2), 'hex'),
        chainId: Buffer.from(tx.chainId.slice(2), 'hex'),
        v: Buffer.from(tx.v.slice(2), 'hex'),
        r: Buffer.from(tx.r.slice(2), 'hex'),
        s: Buffer.from(tx.s.slice(2), 'hex'),
        maxFeePerGas: tx.maxFeePerGas ? Buffer.from(tx.maxFeePerGas.slice(2), 'hex') : undefined,
        maxPriorityFeePerGas: tx.maxPriorityFeePerGas ? Buffer.from(tx.maxPriorityFeePerGas.slice(2), 'hex') : undefined,
        accessList: tx.accessList?.map(entry => ({
            address: Buffer.from(entry.address.slice(2), 'hex'),
            storageKeys: entry.storageKeys.map(key => Buffer.from(key.slice(2), 'hex'))
        })),
        yParity: tx.yParity ? Buffer.from(tx.yParity.slice(2), 'hex') : undefined,
    };
}

function optimizedHex2BinReceipt(receipt: RpcTxReceipt): any {
    return {
        blockHash: Buffer.from(receipt.blockHash.slice(2), 'hex'),
        blockNumber: Buffer.from(receipt.blockNumber.slice(2), 'hex'),
        contractAddress: receipt.contractAddress ? Buffer.from(receipt.contractAddress.slice(2), 'hex') : null,
        cumulativeGasUsed: Buffer.from(receipt.cumulativeGasUsed.slice(2), 'hex'),
        effectiveGasPrice: Buffer.from(receipt.effectiveGasPrice.slice(2), 'hex'),
        from: Buffer.from(receipt.from.slice(2), 'hex'),
        gasUsed: Buffer.from(receipt.gasUsed.slice(2), 'hex'),
        logs: receipt.logs.map(log => ({
            address: Buffer.from(log.address.slice(2), 'hex'),
            topics: log.topics.map(t => Buffer.from(t.slice(2), 'hex')),
            data: Buffer.from(log.data.slice(2), 'hex'),
            blockNumber: Buffer.from(log.blockNumber.slice(2), 'hex'),
            transactionHash: Buffer.from(log.transactionHash.slice(2), 'hex'),
            transactionIndex: Buffer.from(log.transactionIndex.slice(2), 'hex'),
            blockHash: Buffer.from(log.blockHash.slice(2), 'hex'),
            logIndex: Buffer.from(log.logIndex.slice(2), 'hex'),
            removed: log.removed
        })),
        logsBloom: Buffer.from(receipt.logsBloom.slice(2), 'hex'),
        status: Buffer.from(receipt.status.slice(2), 'hex'),
        to: receipt.to ? Buffer.from(receipt.to.slice(2), 'hex') : Buffer.from('0000000000000000000000000000000000000000', 'hex'),
        transactionHash: Buffer.from(receipt.transactionHash.slice(2), 'hex'),
        transactionIndex: Buffer.from(receipt.transactionIndex.slice(2), 'hex'),
        type: Buffer.from(receipt.type.slice(2), 'hex'),
    };
}

function optimizedBin2HexBlock(block: any): RpcBlock {
    const result: RpcBlock = {
        hash: '0x' + block.hash.toString('hex'),
        number: '0x' + block.number.toString('hex'),
        parentHash: '0x' + block.parentHash.toString('hex'),
        timestamp: '0x' + block.timestamp.toString('hex'),
        gasLimit: '0x' + block.gasLimit.toString('hex'),
        gasUsed: '0x' + block.gasUsed.toString('hex'),
        miner: '0x' + block.miner.toString('hex'),
        difficulty: '0x' + block.difficulty.toString('hex'),
        totalDifficulty: '0x' + block.totalDifficulty.toString('hex'),
        size: '0x' + block.size.toString('hex'),
        stateRoot: '0x' + block.stateRoot.toString('hex'),
        transactionsRoot: '0x' + block.transactionsRoot.toString('hex'),
        receiptsRoot: '0x' + block.receiptsRoot.toString('hex'),
        logsBloom: '0x' + block.logsBloom.toString('hex'),
        extraData: '0x' + block.extraData.toString('hex'),
        mixHash: '0x' + block.mixHash.toString('hex'),
        nonce: '0x' + block.nonce.toString('hex'),
        sha3Uncles: '0x' + block.sha3Uncles.toString('hex'),
        uncles: block.uncles.map((u: Buffer) => '0x' + u.toString('hex')),
        transactions: block.transactions.map(optimizedBin2HexTx),
    };

    if (block.baseFeePerGas !== undefined) result.baseFeePerGas = '0x' + block.baseFeePerGas.toString('hex');
    if (block.blobGasUsed !== undefined) result.blobGasUsed = '0x' + block.blobGasUsed.toString('hex');
    if (block.excessBlobGas !== undefined) result.excessBlobGas = '0x' + block.excessBlobGas.toString('hex');
    if (block.parentBeaconBlockRoot !== undefined) result.parentBeaconBlockRoot = '0x' + block.parentBeaconBlockRoot.toString('hex');
    if (block.blockGasCost !== undefined) result.blockGasCost = '0x' + block.blockGasCost.toString('hex');
    if (block.blockExtraData !== undefined) result.blockExtraData = '0x' + block.blockExtraData.toString('hex');
    if (block.extDataHash !== undefined) result.extDataHash = '0x' + block.extDataHash.toString('hex');

    return result;
}

function optimizedBin2HexTx(tx: any): RpcBlockTransaction {
    const result: RpcBlockTransaction = {
        hash: '0x' + tx.hash.toString('hex'),
        blockHash: '0x' + tx.blockHash.toString('hex'),
        blockNumber: '0x' + tx.blockNumber.toString('hex'),
        transactionIndex: '0x' + tx.transactionIndex.toString('hex'),
        from: '0x' + tx.from.toString('hex'),
        to: tx.to ? '0x' + tx.to.toString('hex') : null,
        value: '0x' + tx.value.toString('hex'),
        gas: '0x' + tx.gas.toString('hex'),
        gasPrice: '0x' + tx.gasPrice.toString('hex'),
        input: '0x' + tx.input.toString('hex'),
        nonce: '0x' + tx.nonce.toString('hex'),
        type: '0x' + tx.type.toString('hex'),
        chainId: '0x' + tx.chainId.toString('hex'),
        v: '0x' + tx.v.toString('hex'),
        r: '0x' + tx.r.toString('hex'),
        s: '0x' + tx.s.toString('hex'),
    };
    if (tx.maxFeePerGas) result.maxFeePerGas = '0x' + tx.maxFeePerGas.toString('hex');
    if (tx.maxPriorityFeePerGas) result.maxPriorityFeePerGas = '0x' + tx.maxPriorityFeePerGas.toString('hex');
    if (tx.accessList) {
        result.accessList = tx.accessList.map((entry: any) => ({
            address: '0x' + entry.address.toString('hex'),
            storageKeys: entry.storageKeys.map((key: Buffer) => '0x' + key.toString('hex'))
        }));
    }
    if (tx.yParity) result.yParity = '0x' + tx.yParity.toString('hex');
    return result;
}

function optimizedBin2HexReceipt(receipt: any): RpcTxReceipt {
    return {
        blockHash: '0x' + receipt.blockHash.toString('hex'),
        blockNumber: '0x' + receipt.blockNumber.toString('hex'),
        contractAddress: receipt.contractAddress ? '0x' + receipt.contractAddress.toString('hex') : null,
        cumulativeGasUsed: '0x' + receipt.cumulativeGasUsed.toString('hex'),
        effectiveGasPrice: '0x' + receipt.effectiveGasPrice.toString('hex'),
        from: '0x' + receipt.from.toString('hex'),
        gasUsed: '0x' + receipt.gasUsed.toString('hex'),
        logs: receipt.logs.map((log: any) => ({
            address: '0x' + log.address.toString('hex'),
            topics: log.topics.map((t: Buffer) => '0x' + t.toString('hex')),
            data: '0x' + log.data.toString('hex'),
            blockNumber: '0x' + log.blockNumber.toString('hex'),
            transactionHash: '0x' + log.transactionHash.toString('hex'),
            transactionIndex: '0x' + log.transactionIndex.toString('hex'),
            blockHash: '0x' + log.blockHash.toString('hex'),
            logIndex: '0x' + log.logIndex.toString('hex'),
            removed: log.removed
        })),
        logsBloom: '0x' + receipt.logsBloom.toString('hex'),
        status: '0x' + receipt.status.toString('hex'),
        to: receipt.to ? '0x' + receipt.to.toString('hex') : '0x0000000000000000000000000000000000000000',
        transactionHash: '0x' + receipt.transactionHash.toString('hex'),
        transactionIndex: '0x' + receipt.transactionIndex.toString('hex'),
        type: '0x' + receipt.type.toString('hex'),
    };
}

const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: HAS_DEBUG })
const lastIndexedBlock = db.getLastStoredBlockNumber();
let lastBlock = 0;


const maxBlocks = Math.min(lastIndexedBlock, BENCHMARK_BLOCKS);

const testBlocks: RpcBlock[] = [];
const testReceipts: RpcTxReceipt[] = [];

const readingStart = performance.now();
while (lastBlock < maxBlocks) {
    const batch: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[] = db.getBlocks(lastBlock, 10010);
    if (batch.length === 0) {
        break;
    }
    lastBlock = batch[batch.length - 1]!.block.number;

    for (const block of batch) {
        const rawBlock = lazyBlockToBlock(block.block, block.txs);
        testBlocks.push(rawBlock);
        const rawReceipts = block.txs.map(tx => lazyTxToReceipt(tx));
        testReceipts.push(...rawReceipts);
    }
}
const readingEnd = performance.now();

console.log(`Got ${testBlocks.length} blocks and ${testReceipts.length} receipts in ${Math.round(readingEnd - readingStart)}ms. Starting benchmark...`);

// Train LZ4 dictionaries from sample data using zstd
console.log('\n=== Training LZ4 Dictionaries ===');
const dictStart = performance.now();

// Dictionary size target: 64KB
const DICT_SIZE = 16 * 1024;
// Assuming ~5KB per sample, we need 100x samples: 6.4MB / 5KB = 1280 samples
const TARGET_SAMPLES = 1280;

// Create temp directory for training samples
const tempDir = path.join(tmpdir(), 'dict-training-' + Date.now());
fs.mkdirSync(tempDir, { recursive: true });

// Helper function to train dictionary using zstd
function trainDictionary(samples: any[], name: string, transformer: (item: any) => string | Buffer): Buffer {
    const sampleDir = path.join(tempDir, name);
    fs.mkdirSync(sampleDir, { recursive: true });

    // Use first 10% of data, but select TARGET_SAMPLES unique items
    const availableSamples = Math.floor(samples.length * DICT_PERCENTAGE);
    const samplesToUse = Math.min(TARGET_SAMPLES, availableSamples);

    // Write samples to individual files
    for (let i = 0; i < samplesToUse; i++) {
        const sampleData = transformer(samples[i]);
        fs.writeFileSync(path.join(sampleDir, `sample_${i}`), sampleData);
    }

    // Train dictionary using zstd CLI
    const dictPath = path.join(tempDir, `${name}.dict`);
    try {
        execSync(`zstd --train ${sampleDir}/* -o ${dictPath} --maxdict=${DICT_SIZE}`, {
            stdio: 'pipe' // Suppress output
        });
        return fs.readFileSync(dictPath);
    } catch (err) {
        console.error(`Failed to train dictionary for ${name}:`, err);
        // Return empty buffer as fallback
        return Buffer.alloc(0);
    }
}

// Train dictionaries for each format
const jsonBlockDict = trainDictionary(testBlocks, 'json-blocks', b => JSON.stringify(b));
const jsonReceiptDict = trainDictionary(testReceipts, 'json-receipts', r => JSON.stringify(r));
const msgpackBlockDict = trainDictionary(testBlocks, 'msgpack-blocks', b => Buffer.from(pack(b)));
const msgpackReceiptDict = trainDictionary(testReceipts, 'msgpack-receipts', r => Buffer.from(pack(r)));

const compactBlockDict = trainDictionary(
    testBlocks,
    'compact-blocks',
    b => Buffer.from(pack(hex2bin(b)))
);
const compactReceiptDict = trainDictionary(
    testReceipts,
    'compact-receipts',
    r => Buffer.from(pack(hex2bin(r)))
);

// Train zstd-napi dictionaries
const zstdJsonBlockDict = trainDictionary(testBlocks, 'zstd-json-blocks', b => JSON.stringify(b));
const zstdJsonReceiptDict = trainDictionary(testReceipts, 'zstd-json-receipts', r => JSON.stringify(r));
const zstdMsgpackBlockDict = trainDictionary(testBlocks, 'zstd-msgpack-blocks', b => Buffer.from(pack(b)));
const zstdMsgpackReceiptDict = trainDictionary(testReceipts, 'zstd-msgpack-receipts', r => Buffer.from(pack(r)));
const zstdCompactBlockDict = trainDictionary(testBlocks, 'zstd-compact-blocks', b => Buffer.from(pack(hex2bin(b))));
const zstdCompactReceiptDict = trainDictionary(testReceipts, 'zstd-compact-receipts', r => Buffer.from(pack(hex2bin(r))));

// Clean up temp directory
fs.rmSync(tempDir, { recursive: true, force: true });

const dictEnd = performance.now();
console.log(`Dictionary training completed in ${Math.round(dictEnd - dictStart)}ms`);
console.log(`  Target samples per dictionary: ${TARGET_SAMPLES}`);
console.log(`  Actual block samples: ${Math.min(TARGET_SAMPLES, Math.floor(testBlocks.length * 0.1))}`);
console.log(`  Actual receipt samples: ${Math.min(TARGET_SAMPLES, Math.floor(testReceipts.length * 0.1))}`);
console.log(`  JSON block dict: ${(jsonBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  JSON receipt dict: ${(jsonReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Msgpack block dict: ${(msgpackBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Msgpack receipt dict: ${(msgpackReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Compact block dict: ${(compactBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Compact receipt dict: ${(compactReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd JSON block dict: ${(zstdJsonBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd JSON receipt dict: ${(zstdJsonReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Msgpack block dict: ${(zstdMsgpackBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Msgpack receipt dict: ${(zstdMsgpackReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Compact block dict: ${(zstdCompactBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Compact receipt dict: ${(zstdCompactReceiptDict.length / 1024).toFixed(1)} KB`);

console.log('\n=== Individual Object Encoding/Decoding Benchmark ===');
console.log('(Simulating database row storage)\n');

interface Method {
    name: string;
    encode: (obj: any, datasetName: string) => Buffer | Uint8Array;
    decode: (data: Buffer | Uint8Array, datasetName: string) => any;
    setup?: (datasetName: string) => void;
    cleanup?: () => void;
}

// Create reusable compressor/decompressor instances
const zstdCompressors: Record<string, zstd.Compressor> = {};
const zstdDecompressors: Record<string, zstd.Decompressor> = {};

const methods: Method[] = [
    {
        name: 'JSON plain',
        encode: (obj) => Buffer.from(JSON.stringify(obj)),
        decode: (data) => JSON.parse(data.toString())
    },
    {
        name: 'JSON + lz4',
        encode: (obj) => lz4CompressSync(Buffer.from(JSON.stringify(obj))),
        decode: (data) => JSON.parse(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data)).toString())
    },
    {
        name: 'JSON + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? jsonBlockDict : jsonReceiptDict;
            return lz4CompressSync(Buffer.from(JSON.stringify(obj)), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? jsonBlockDict : jsonReceiptDict;
            return JSON.parse(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict).toString());
        }
    },
    {
        name: 'JSON + zstd',
        encode: (obj) => zstdCompressSync(Buffer.from(JSON.stringify(obj)), {
            params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
        }),
        decode: (data) => JSON.parse(zstdDecompressSync(data).toString())
    },
    {
        name: 'JSON + zstdNapi',
        setup: (datasetName) => {
            const key = `json-nodict-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `json-nodict-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(JSON.stringify(obj)));
        },
        decode: (data, datasetName) => {
            const key = `json-nodict-${datasetName}`;
            return JSON.parse(zstdDecompressors[key]!.decompress(data).toString());
        }
    },
    {
        name: 'JSON + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdJsonBlockDict : zstdJsonReceiptDict;
            const key = `json-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `json-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(JSON.stringify(obj)));
        },
        decode: (data, datasetName) => {
            const key = `json-${datasetName}`;
            return JSON.parse(zstdDecompressors[key]!.decompress(data).toString());
        }
    },
    {
        name: 'msgpackr plain',
        encode: (obj) => pack(obj),
        decode: (data) => unpack(data)
    },
    {
        name: 'msgpackr + lz4',
        encode: (obj) => lz4CompressSync(Buffer.from(pack(obj))),
        decode: (data) => unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data)))
    },
    {
        name: 'msgpackr + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? msgpackBlockDict : msgpackReceiptDict;
            return lz4CompressSync(Buffer.from(pack(obj)), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? msgpackBlockDict : msgpackReceiptDict;
            return unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict));
        }
    },
    {
        name: 'msgpackr + zstd',
        encode: (obj) => zstdCompressSync(Buffer.from(pack(obj)), {
            params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
        }),
        decode: (data) => unpack(zstdDecompressSync(data))
    },
    {
        name: 'msgpackr + zstdNapi',
        setup: (datasetName) => {
            const key = `msgpack-nodict-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `msgpack-nodict-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(obj)));
        },
        decode: (data, datasetName) => {
            const key = `msgpack-nodict-${datasetName}`;
            return unpack(zstdDecompressors[key]!.decompress(data));
        }
    },
    {
        name: 'msgpackr + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdMsgpackBlockDict : zstdMsgpackReceiptDict;
            const key = `msgpack-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `msgpack-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(obj)));
        },
        decode: (data, datasetName) => {
            const key = `msgpack-${datasetName}`;
            return unpack(zstdDecompressors[key]!.decompress(data));
        }
    },
    {
        name: 'compactMsgpackr plain',
        encode: obj => Buffer.from(pack(hex2bin(obj))),
        decode: data => bin2hex(unpack(data))
    },
    {
        name: 'compactMsgpackr + lz4',
        encode: obj => lz4CompressSync(Buffer.from(pack(hex2bin(obj)))),
        decode: data =>
            bin2hex(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data))))
    },
    {
        name: 'compactMsgpackr + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            return lz4CompressSync(Buffer.from(pack(hex2bin(obj))), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            return bin2hex(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict)));
        }
    },
    {
        name: 'compactMsgpackr + zstd',
        encode: obj =>
            zstdCompressSync(Buffer.from(pack(hex2bin(obj))), {
                params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
            }),
        decode: data => bin2hex(unpack(zstdDecompressSync(data)))
    },
    {
        name: 'compactMsgpackr + zstdNapi',
        setup: (datasetName) => {
            const key = `compact-nodict-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `compact-nodict-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(hex2bin(obj))));
        },
        decode: (data, datasetName) => {
            const key = `compact-nodict-${datasetName}`;
            return bin2hex(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    },
    {
        name: 'compactMsgpackr + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdCompactBlockDict : zstdCompactReceiptDict;
            const key = `compact-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `compact-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(hex2bin(obj))));
        },
        decode: (data, datasetName) => {
            const key = `compact-${datasetName}`;
            return bin2hex(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    },
    {
        name: 'optimizedCompact plain',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return Buffer.from(pack(converter(obj)));
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(data));
        }
    },
    {
        name: 'optimizedCompact + lz4',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return lz4CompressSync(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data))));
        }
    },
    {
        name: 'optimizedCompact + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return lz4CompressSync(Buffer.from(pack(converter(obj))), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict)));
        }
    },
    {
        name: 'optimizedCompact + zstd',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return zstdCompressSync(Buffer.from(pack(converter(obj))), {
                params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
            });
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressSync(data)));
        }
    },
    {
        name: 'optimizedCompact + zstdNapi',
        setup: (datasetName) => {
            const key = `optimized-compact-nodict-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `optimized-compact-nodict-${datasetName}`;
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return zstdCompressors[key]!.compress(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const key = `optimized-compact-nodict-${datasetName}`;
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    },
    {
        name: 'optimizedCompact + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdCompactBlockDict : zstdCompactReceiptDict;
            const key = `optimized-compact-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `optimized-compact-${datasetName}`;
            const converter = datasetName === 'blocks' ? optimizedHex2BinBlock : optimizedHex2BinReceipt;
            return zstdCompressors[key]!.compress(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const key = `optimized-compact-${datasetName}`;
            const converter = datasetName === 'blocks' ? optimizedBin2HexBlock : optimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    },
    {
        name: 'ultraCompact plain',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return Buffer.from(pack(converter(obj)));
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(data));
        }
    },
    {
        name: 'ultraCompact + lz4',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return lz4CompressSync(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data))));
        }
    },
    {
        name: 'ultraCompact + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return lz4CompressSync(Buffer.from(pack(converter(obj))), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict)));
        }
    },
    {
        name: 'ultraCompact + zstd',
        encode: (obj, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return zstdCompressSync(Buffer.from(pack(converter(obj))), {
                params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
            });
        },
        decode: (data, datasetName) => {
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressSync(data)));
        }
    },
    {
        name: 'ultraCompact + zstdNapi',
        setup: (datasetName) => {
            const key = `ultra-compact-nodict-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `ultra-compact-nodict-${datasetName}`;
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return zstdCompressors[key]!.compress(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const key = `ultra-compact-nodict-${datasetName}`;
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    },
    {
        name: 'ultraCompact + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdCompactBlockDict : zstdCompactReceiptDict;
            const key = `ultra-compact-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `ultra-compact-${datasetName}`;
            const converter = datasetName === 'blocks' ? ultraOptimizedHex2BinBlock : ultraOptimizedHex2BinReceipt;
            return zstdCompressors[key]!.compress(Buffer.from(pack(converter(obj))));
        },
        decode: (data, datasetName) => {
            const key = `ultra-compact-${datasetName}`;
            const converter = datasetName === 'blocks' ? ultraOptimizedBin2HexBlock : ultraOptimizedBin2HexReceipt;
            return converter(unpack(zstdDecompressors[key]!.decompress(data)));
        }
    }
];

// Test both blocks and receipts
const datasets = [
    { name: 'blocks', data: testBlocks },
    { name: 'receipts', data: testReceipts }
];

for (const dataset of datasets) {
    console.log(`\n--- Testing ${dataset.name} (${dataset.data.length} items) ---`);

    // Collect all results first
    const results: Array<{
        method: string;
        totalSizeMB: number;
        avgSizeKB: number;
        encodeTimeMs: number;
        decodeTimeMs: number;
        compressionRatio: number;
    }> = [];

    // First pass to get JSON plain baseline size
    let jsonPlainTotalSize = 0;
    for (const item of dataset.data) {
        jsonPlainTotalSize += methods[0]!.encode(item, dataset.name).length;
    }

    for (const method of methods) {
        // Setup method if needed
        if (method.setup) {
            method.setup(dataset.name);
        }

        // Encode all objects individually
        const encodedItems: (Buffer | Uint8Array)[] = [];
        const encodeStart = performance.now();
        for (const item of dataset.data) {
            encodedItems.push(method.encode(item, dataset.name));
        }
        const encodeTime = performance.now() - encodeStart;

        // Calculate total size
        const totalSize = encodedItems.reduce((sum, item) => sum + item.length, 0);
        const avgSize = totalSize / encodedItems.length;

        // Decode all objects individually
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            method.decode(encoded, dataset.name);
        }
        const decodeTime = performance.now() - decodeStart;

        results.push({
            method: method.name,
            totalSizeMB: totalSize / 1024 / 1024,
            avgSizeKB: avgSize / 1024,
            encodeTimeMs: encodeTime,
            decodeTimeMs: decodeTime,
            compressionRatio: jsonPlainTotalSize / totalSize
        });
    }

    // Sort by decode time ascending
    results.sort((a, b) => a.decodeTimeMs - b.decodeTimeMs);

    // Create and print table
    const table = new Table({
        title: `${dataset.name.toUpperCase()} Benchmark Results (${dataset.data.length} items)`,
        columns: [
            { name: 'method', title: 'Method', alignment: 'left' },
            { name: 'totalSizeMB', title: 'Total Size (MB)', alignment: 'right' },
            { name: 'avgSizeKB', title: 'Avg Size (KB)', alignment: 'right' },
            { name: 'compressionRatio', title: 'Compression', alignment: 'right' },
            { name: 'encodeTimeMs', title: 'Encode (ms)', alignment: 'right' },
            { name: 'decodeTimeMs', title: 'Decode (ms)', alignment: 'right' }
        ]
    });

    results.forEach(result => {
        table.addRow({
            method: result.method,
            totalSizeMB: result.totalSizeMB.toFixed(2),
            avgSizeKB: result.avgSizeKB.toFixed(2),
            compressionRatio: result.compressionRatio.toFixed(1) + 'x',
            encodeTimeMs: result.encodeTimeMs.toFixed(2),
            decodeTimeMs: result.decodeTimeMs.toFixed(2)
        });
    });

    table.printTable();

    // Calculate relative comparisons
    console.log('\n📊 Size & Decode Time Comparison (vs JSON plain):');

    // Get baseline (JSON plain) values
    const jsonPlainResult = results.find(r => r.method === 'JSON plain')!;
    const jsonPlainDecodeTime = jsonPlainResult.decodeTimeMs;

    // Calculate extrapolation factor for 100M items
    const ITEMS_TARGET = 100_000_000;
    const extrapolationFactor = ITEMS_TARGET / dataset.data.length;

    const comparisonTable = new Table({
        title: `${dataset.name.toUpperCase()} Comparison vs JSON Plain (extrapolated to 100M items)`,
        columns: [
            { name: 'method', title: 'Method', alignment: 'left' },
            { name: 'sizeRatio', title: 'Size Ratio', alignment: 'right' },
            { name: 'timeRatio', title: 'Time Ratio', alignment: 'right' },
            { name: 'extrapolatedSizeGB', title: 'Size (GB)', alignment: 'right' },
            { name: 'extrapolatedTimeMin', title: 'Time (min)', alignment: 'right' },
            { name: 'sizeReduction', title: 'Size Reduction', alignment: 'right' }
        ]
    });

    results.forEach(result => {
        const totalSizeBytes = result.totalSizeMB * 1024 * 1024;
        const sizeRatio = (totalSizeBytes / jsonPlainTotalSize);
        const timeRatio = (result.decodeTimeMs / jsonPlainDecodeTime);
        const sizeReduction = ((jsonPlainTotalSize - totalSizeBytes) / jsonPlainTotalSize * 100);

        const extrapolatedSizeGB = (totalSizeBytes * extrapolationFactor) / (1024 * 1024 * 1024);
        const extrapolatedTimeMinutes = (result.decodeTimeMs * extrapolationFactor) / (1000 * 60);

        let color = 'white';
        if (result.method === 'JSON plain') {
            color = 'cyan';
        } else if (sizeRatio < 0.5 && timeRatio < 2) {
            color = 'green';
        } else if (sizeRatio < 0.7 && timeRatio < 3) {
            color = 'yellow';
        }

        comparisonTable.addRow({
            method: result.method,
            sizeRatio: sizeRatio.toFixed(2) + 'x',
            timeRatio: timeRatio.toFixed(2) + 'x',
            extrapolatedSizeGB: extrapolatedSizeGB.toFixed(1),
            extrapolatedTimeMin: extrapolatedTimeMinutes.toFixed(1),
            sizeReduction: result.method === 'JSON plain' ? 'baseline' : sizeReduction.toFixed(1) + '%'
        }, { color });
    });

    comparisonTable.printTable();
}

console.log('\n=== Benchmark Complete ===');

process.exit(0);


function hex2bin<T>(x: T): T {
    if (typeof x === 'string' && HEX_RE.test(x)) {
        return Buffer.from(x.slice(2), 'hex') as unknown as T;
    }
    if (Array.isArray(x)) return x.map(hex2bin) as unknown as T;
    if (x && typeof x === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(x)) out[k] = hex2bin(v);
        return out as T;
    }
    return x;
}

function bin2hex<T>(x: T): T {
    if (Buffer.isBuffer(x)) return ('0x' + x.toString('hex')) as unknown as T;
    if (Array.isArray(x)) return x.map(bin2hex) as unknown as T;
    if (x && typeof x === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(x)) out[k] = bin2hex(v);
        return out as T;
    }
    return x;
}
