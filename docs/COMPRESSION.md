# Zstandard Dictionary Compression

This document describes the implementation of Zstandard dictionary compression in the blockchain indexer.

## Overview

Dictionary compression provides better compression ratios and improved compression speed for small files by training a dictionary on representative data samples. The blockchain indexer uses dictionary compression to reduce storage requirements for blocks and improve performance.

## Implementation

### Dictionary Training

When the following conditions are met, the system automatically selects blocks for dictionary training:
- More than 10,000 blocks are stored
- The node has caught up with the blockchain
- No dictionary exists yet
- Dictionary training is not already in progress

The training process:
1. Selects approximately 10,000 representative blocks by sampling evenly across the stored blocks
2. Stores the sample block numbers in the database
3. Marks samples as ready for external dictionary training

To actually train a dictionary:
1. Export the training samples: `await blockDB.exportDictionaryTrainingSamples('/path/to/samples')`
2. Install zstd: `apt-get install zstd` or `brew install zstd`
3. Train the dictionary: `zstd --train -r /path/to/samples -o dictionary --maxdict=262144`
4. Load the dictionary into the database (see API section)

Note: The exported samples are raw decompressed block data (not parsed JSON), which provides optimal dictionary training for the actual data patterns.

### Dictionary Compression

Once a dictionary is loaded:
- All new blocks are automatically compressed using the dictionary (codec=1)
- Existing blocks compressed without the dictionary (codec=0) are gradually recompressed
- Recompression happens in batches of 1,000 blocks during maintenance cycles

### Storage Format

The database tracks compression state using:
- `codec` field in blocks/txs tables:
  - 0: Compressed without dictionary
  - 1: Compressed with dictionary
- Key-value storage for tracking:
  - `dict_training_in_progress`: Whether dictionary training is active
  - `dict_training_samples`: JSON array of sample block numbers
  - `dict_training_samples_ready`: Whether samples are ready for training
  - `last_recompressed_block`: Progress of recompression
  - `recompression_complete`: Whether all blocks use dictionary compression

## Performance Characteristics

### Compression Ratio
- Dictionary compression typically provides 20-50% better compression for blockchain data
- Most effective for the first few KB of each block
- Diminishing returns for very large blocks

### Speed
- Dictionary compression is faster than standard compression for small data
- Recompression is throttled to avoid impacting normal operations
- Processes 1,000 blocks per maintenance cycle

### Memory Usage
- Dictionary size: 256KB (configurable)
- Loaded dictionaries are kept in memory for fast access
- Separate dictionaries for blocks, transactions, and traces

## API

### BlockDB Methods

#### `exportDictionaryTrainingSamples(outputPath: string): Promise<void>`
Exports selected sample blocks for external dictionary training. The exported files contain raw decompressed block data (not parsed JSON) for optimal dictionary training.

#### `getDictionary(name: string): Buffer | undefined`
Retrieves a stored dictionary by name ('blocks', 'txs', or 'traces').

#### `setDictionary(name: string, data: Buffer): void`
Stores a trained dictionary for use in compression.

### Example Usage

```typescript
// After dictionary training samples are ready (happens automatically after 10k blocks)
await blockDB.exportDictionaryTrainingSamples('/tmp/block_samples');

// Train dictionary externally (requires zstd CLI)
// $ zstd --train -r /tmp/block_samples -o /tmp/blocks.dict --maxdict=262144

// Or use the helper script for the complete workflow:
// $ ts-node blockFetcher/dictionary-helper.ts full ./blocks.db ./dict-workspace

// Load trained dictionary
import { readFileSync } from 'fs';
const dictionary = readFileSync('/tmp/blocks.dict');
blockDB.setDictionary('blocks', dictionary);

// New blocks now use dictionary compression automatically
// Old blocks are recompressed gradually during maintenance
```

## Maintenance

Dictionary compression is integrated into the periodic maintenance cycle:
1. Runs when the node is caught up and idle
2. Checks if dictionary training is needed
3. Recompresses old blocks in batches
4. Tracks progress to resume after restarts

The process is designed to be:
- **Automatic**: No manual intervention required
- **Non-blocking**: Doesn't interfere with normal operations
- **Resumable**: Can be interrupted and resumed safely
- **Efficient**: Processes data in transactions to maintain consistency

## Future Improvements

1. **Integrated Training**: Add native dictionary training support to eliminate external zstd dependency
2. **Adaptive Dictionaries**: Train new dictionaries periodically as data patterns change
3. **Per-Chain Dictionaries**: Use different dictionaries for different blockchain types
4. **Transaction Compression**: Extend dictionary compression to the transactions table