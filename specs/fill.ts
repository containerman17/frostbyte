import fs from 'fs';
import YAML from 'yaml';
import { getEvmChainId } from './utils';

async function updateYamlFile(chainId: number, forceUpdate: boolean) {
    const yamlFilePath = './specs/txCount.yaml';
    const yamlContent = fs.readFileSync(yamlFilePath, 'utf8');

    // Split by --- to get individual documents
    const blocks = yamlContent.split('---\n').filter(block => block.trim());

    // Update first block
    if (!blocks[0]) throw new Error('No first block found');
    const firstBlock = YAML.parse(blocks[0]);
    firstBlock.chainId = chainId;
    blocks[0] = YAML.stringify(firstBlock).trim();

    // Update other blocks
    for (let i = 1; i < blocks.length; i++) {
        const blockContent = blocks[i];
        if (!blockContent) continue;
        const block = YAML.parse(blockContent);

        // Update glacierBody if --force flag is set, glacierBody is absent, or glacierBody is "TODO: add body"
        if (forceUpdate || !block.glacierBody || block.glacierBody === "TODO: add body") {
            const url = `https://metrics.avax.network/v2/chains/${chainId}${block.path}`;

            try {
                const response = await fetch(url);
                if (response.ok) {
                    const body = await response.json();
                    block.glacierBody = JSON.stringify(body, null, 2);

                    // If expectedBody doesn't exist or is empty, initialize it with glacierBody
                    if (!block.expectedBody || block.expectedBody === "TODO: add body" || block.expectedBody === "") {
                        block.expectedBody = block.glacierBody;
                    }
                }
            } catch (error) {
                console.error(`Failed to fetch ${url}:`, error);
            }
        }

        blocks[i] = YAML.stringify(block).trim();
    }

    // Write back
    fs.writeFileSync(yamlFilePath, blocks.join('\n---\n') + '\n');
}

// Parse command line arguments
const forceUpdate = process.argv.includes('--force')

const chainId = await getEvmChainId('http://localhost:3000/rpc');
console.log(chainId);
await updateYamlFile(chainId, forceUpdate);

export { }
