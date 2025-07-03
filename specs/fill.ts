import fs from 'fs';
import path from 'path';
import YAML from 'yaml';

// Recursive function to find all YAML files
function findYamlFiles(dir: string): string[] {
    const yamlFiles: string[] = [];

    const files = fs.readdirSync(dir);
    for (const file of files) {
        const fullPath = path.join(dir, file);
        const stat = fs.statSync(fullPath);

        if (stat.isDirectory()) {
            // Recursively search subdirectories
            yamlFiles.push(...findYamlFiles(fullPath));
        } else if (file.endsWith('.yaml') || file.endsWith('.yml')) {
            yamlFiles.push(fullPath);
        }
    }

    return yamlFiles;
}

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

                // Always update status
                block.status = response.status;

                // Get the response as text first
                const responseText = await response.text();

                // Try to parse as JSON, otherwise use as plain text
                try {
                    const body = JSON.parse(responseText);
                    block.glacierBody = JSON.stringify(body, null, 2);
                } catch (jsonError) {
                    // If not valid JSON, use the text as is
                    block.glacierBody = responseText;
                }

                // If expectedBody doesn't exist or is empty, initialize it with glacierBody
                if (!block.expectedBody || block.expectedBody === "TODO: add body" || block.expectedBody === "") {
                    block.expectedBody = block.glacierBody;
                }

                if (!response.ok) {
                    console.error(`Request to ${url} returned HTTP ${response.status} ${response.statusText}`);
                }
            } catch (error) {
                console.error(`Failed to fetch ${url}:`, error);
                // Set status to 0 or similar to indicate network error
                block.status = 0;
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
const yamlFiles = findYamlFiles(specsDir);

// Process each YAML file
for (const yamlFile of yamlFiles) {
    console.log(`Processing ${yamlFile}...`);
    await updateYamlFile(yamlFile, E2E_CHAIN_ID, forceUpdate);
}

export { }
