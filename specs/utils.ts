
export async function getEvmChainId(url: string) {
    const data = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', method: 'eth_chainId', params: [], id: 1 }),
    }).then(res => res.ok ? res.json() : Promise.reject(`HTTP error! status: ${res.status}`)) as { result: string, error?: { message: string } };

    if (data.error) throw new Error(`RPC error: ${data.error.message}`);
    return parseInt(data.result, 16);
}
