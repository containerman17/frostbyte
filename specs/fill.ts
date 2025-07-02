import fs from 'fs';
import path from 'path';
import YAML from 'yaml';


async function updateYamlFile(yamlFilePath: string, chainId: number, forceUpdate: boolean) {
    const yamlContent = fs.readFileSync(yamlFilePath, 'utf8');

    // Split by --- to get individual documents
    const blocks = yamlContent.split('---\n').filter(block => block.trim());

    // Update all blocks
    for (let i = 0; i < blocks.length; i++) {
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
const E2E_CHAIN_ID = 27827;

// Get all YAML files in specs directory
const specsDir = './specs';
const yamlFiles = fs.readdirSync(specsDir)
    .filter(file => file.endsWith('.yaml') || file.endsWith('.yml'))
    .map(file => path.join(specsDir, file));

// Process each YAML file
for (const yamlFile of yamlFiles) {
    console.log(`Processing ${yamlFile}...`);
    await updateYamlFile(yamlFile, E2E_CHAIN_ID, forceUpdate);
}

export { }
