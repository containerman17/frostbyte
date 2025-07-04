import { IndexerModule } from "./types";
import fs from 'node:fs';
import path from 'node:path';

export async function loadPlugins(pluginsDirs: string[]): Promise<IndexerModule[]> {
    const plugins: IndexerModule[] = [];
    for (const pluginsDir of pluginsDirs) {
        const pluginFiles = fs.readdirSync(pluginsDir);
        for (const file of pluginFiles) {
            if (file.endsWith('.ts')) {
                const plugin = await import(path.join(pluginsDir, file));
                plugins.push(plugin.default);
            }
        }
    }
    return plugins;
}
