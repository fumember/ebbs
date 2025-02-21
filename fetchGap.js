require('dotenv').config();
const { createPublicClient, http } = require('viem');
const { base } = require('viem/chains');
const sqlite3 = require('sqlite3').verbose();

const TOKEN_ADDRESS = '0x06f71fb90f84b35302d132322a3c90e4477333b0';
const ALCHEMY_URL = process.env.ALCHEMY_URL;
const DB = new sqlite3.Database('token_data.db');

const client = createPublicClient({
    chain: base,
    transport: http(ALCHEMY_URL),
});

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getBlockByTime(timestamp) {
    const url = `https://api.basescan.org/api?module=block&action=getblocknobytime&timestamp=${timestamp}&closest=before&apikey=${process.env.BASESCAN_API_KEY}`;
    const response = await fetch(url);
    const data = await response.json();
    console.log(`Block fetch:`, JSON.stringify(data));
    if (data.status !== '1') throw new Error(`API status: ${data.status}, message: ${data.message}`);
    return parseInt(data.result);
}

async function fetchGap() {
    let allTransfers = [];
    const BLOCK_CHUNK = 1000;
    const END_BLOCK = await getBlockByTime(Math.floor(Date.now() / 1000));

    DB.get('SELECT MAX(blockNumber) as lastBlock FROM transfers', async (err, row) => {
        if (err) {
            console.error('DB Query Error:', err);
            return;
        }
        const START_BLOCK = row && row.lastBlock ? row.lastBlock + 1 : 24917847;
        console.log(`Fetching gap from block ${START_BLOCK} to ${END_BLOCK}`);

        for (let block = START_BLOCK; block < END_BLOCK; block += BLOCK_CHUNK) {
            const endBlock = Math.min(block + BLOCK_CHUNK - 1, END_BLOCK);
            try {
                const logs = await client.getLogs({
                    address: TOKEN_ADDRESS,
                    event: {
                        type: 'event',
                        name: 'Transfer',
                        inputs: [
                            { indexed: true, name: 'from', type: 'address' },
                            { indexed: true, name: 'to', type: 'address' },
                            { indexed: false, name: 'value', type: 'uint256' }
                        ]
                    },
                    fromBlock: BigInt(block),
                    toBlock: BigInt(endBlock)
                });

                for (const log of logs) {
                    const timestamp = Number((await client.getBlock({ blockNumber: log.blockNumber })).timestamp);
                    allTransfers.push({
                        txHash: log.transactionHash,
                        logIndex: log.logIndex,
                        fromAddr: log.args.from,
                        toAddr: log.args.to,
                        value: log.args.value.toString(),
                        timestamp: timestamp,
                        blockNumber: Number(log.blockNumber)
                    });
                }
                console.log(`Fetched ${logs.length} transfers from block ${block} to ${endBlock}`);
            } catch (e) {
                console.error(`Block ${block}-${endBlock} failed:`, e.message);
                break;
            }
            await delay(1000);
        }

        DB.serialize(() => {
            const stmt = DB.prepare(`INSERT OR IGNORE INTO transfers (txHash, logIndex, fromAddr, toAddr, value, timestamp, blockNumber) VALUES (?, ?, ?, ?, ?, ?, ?)`);
            allTransfers.forEach(tx => {
                stmt.run(tx.txHash, tx.logIndex, tx.fromAddr, tx.toAddr, tx.value, tx.timestamp, tx.blockNumber);
            });
            stmt.finalize();
            console.log(`Total gap transfers fetched: ${allTransfers.length}`);
            DB.close();
            console.log('Gap fetch complete. Ready for trackNew.js.');
        });
    });
}

fetchGap();