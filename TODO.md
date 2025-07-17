make logsBloom an optional field for the stored transaction. do not store this
field. it is needed only in RPC, but I will reconstruct it later, don't worry.
actually, create a special method somewhere in lib/. Check files

when we do an indexing query without filters it returns items, we grab the last
items tx id and next time start from there. this logic worked before we
introduced filters. Now it doesn't matter how heavy the query is - it will do it
evvery time. Redo it in a way that it can be lss wasteful. May be return max
current tx id?
