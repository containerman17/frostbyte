# Temporary disabled, please check methods shouldTrainTxDictionary and shouldTrainDictionary

# Zstandard Dictionary Compression

This document describes the implementation of Zstandard dictionary compression
in the blockchain indexer.

## Overview

Dictionary compression provides better compression ratios and improved
compression speed for small files by training a dictionary on representative
data samples. The blockchain indexer uses dictionary compression to reduce
storage requirements for blocks and improve performance.

## Implementation

### Dictionary Training

When the following conditions are met, the system automatically trains a
dictionary:

- More than 10,000 blocks are stored
- The node has caught up with the blockchain
- No dictionary exists yet
- Dictionary training is not already in progress

The training process (fully automatic):

1. Sets a lock to prevent multiple training processes
2. Exports ~10,000 sample blocks to a temporary directory
3. Runs `zstd --train` to create a 256KB dictionary
4. Loads the dictionary into the database
5. Cleans up all temporary files

Requirements:

- `zstd` command-line tool must be installed
- System: `apt-get install zstd` (Ubuntu/Debian) or `brew install zstd` (macOS)

Notes:

- Dictionary training happens automatically during periodic maintenance
- Takes a few minutes depending on system performance
- If training fails (e.g., zstd not installed), the process crashes with error
- Once trained, all new blocks use dictionary compression
- Existing blocks are gradually recompressed with the dictionary

### Dictionary Compression

Once a dictionary is loaded:

- All new blocks are automatically compressed using the dictionary (codec=1)
- Existing blocks compressed without the dictionary (codec=0) are gradually
  recompressed
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

- Dictionary compression typically provides 20-50% better compression for
  blockchain data
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

Exports sample blocks for external dictionary training. This method:

- Requires at least 10,000 blocks in the database
- Calculates samples on the fly (every Nth block for ~10,000 samples)
- Cleans the output directory before exporting
- Exports raw decompressed block data (not parsed JSON) for optimal training
- Throws an error if not enough blocks are available

#### `getDictionary(name: string): Buffer | undefined`

Retrieves a stored dictionary by name ('blocks', 'txs', or 'traces').

#### `setDictionary(name: string, data: Buffer): void`

Stores a trained dictionary for use in compression.

### Example Usage

Dictionary training is fully automatic! Just ensure `zstd` is installed:

```bash
# Ubuntu/Debian
sudo apt-get install zstd

# macOS
brew install zstd
```

Then the system will automatically:

1. Detect when you have enough blocks (>10k)
2. Train a dictionary during idle maintenance
3. Start using it for all new blocks
4. Gradually recompress old blocks

Manual dictionary management (if needed):

```typescript
// Check if dictionary exists
const dict = blockDB.getDictionary("blocks");
if (dict) {
  console.log("Dictionary size:", dict.length);
}

// Manually load a pre-trained dictionary
import { readFileSync } from "fs";
const dictionary = readFileSync("custom-dictionary.zstd");
blockDB.setDictionary("blocks", dictionary);
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

1. **Integrated Training**: Add native dictionary training support to eliminate
   external zstd dependency
2. **Adaptive Dictionaries**: Train new dictionaries periodically as data
   patterns change
3. **Per-Chain Dictionaries**: Use different dictionaries for different
   blockchain types
4. **Transaction Compression**: Extend dictionary compression to the
   transactions table
