make logsBloom an optional field for the stored receipt and block. do not store
this field. it is needed only in RPC, but I will reconstruct it later, don't
worry. actually, create a special function somewhere in lib/ to add a logs bloom
hex to a receipt / block . Check files blockFetcher/evmTypes.ts for types,
blockFetcher/BlocksDBHelper.ts is where it is all stored.

when we do an indexing query without filters it returns items, we grab the last
items tx id and next time start from there. this logic worked before we
introduced filters. Now it doesn't matter how heavy the query is - it will do it
evvery time. Redo it in a way that it can be lss wasteful. May be return max
current tx id?
