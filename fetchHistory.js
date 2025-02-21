require('dotenv').config();
const { createPublicClient, http } = require('viem');
const { base } = require('viem/chains');
const sqlite3 = require('sqlite3').verbose();
const axios = require('axios');

const TOKEN_ADDRESS = '0x06f71fb90f84b35302d132322a3c90e4477333b0';
const ALCHEMY_URL = process.env.ALCHEMY_URL;
const DB = new sqlite3.Database('token_data.db');

DB.serialize(() => {
    DB.run(`CREATE TABLE IF NOT EXISTS transfers (
        txHash TEXT,
        logIndex INTEGER,
        fromAddr TEXT,
        toAddr TEXT,
        value TEXT,
        timestamp INTEGER,
        blockNumber INTEGER,
        PRIMARY KEY (txHash, logIndex)
    )`);
});

const client = createPublicClient({
    chain: base,
    transport: http(ALCHEMY_URL),
});

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getBlockByTime(timestamp, retries = 3) {
    const url = `https://api.basescan.org/api?module=block&action=getblocknobytime&timestamp=${timestamp}&closest=before&apikey=${process.env.BASESCAN_API_KEY}`;
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await axios.get(url);
            const data = response.data;
            console.log(`Block fetch attempt ${attempt}:`, JSON.stringify(data));
            if (data.status === '1' && data.result) return parseInt(data.result);
            throw new Error(`API status: ${data.status}, message: ${data.message}`);
        } catch (e) {
            console.error(`Attempt ${attempt} failed:`, e.message);
            if (attempt === retries) {
                console.warn('Using fallback block: 26648185');
                return 26648185; // Fallback if all retries fail
            }
            await delay(1000 * attempt);
        }
    }
}

async function fetchTransfers(startBlock = null) {
    let allTransfers = [];
    const BLOCK_CHUNK = 10000;
    const START_BLOCK = startBlock || 24917847;
    const END_BLOCK = 24917900//await getBlockByTime(Math.floor(Date.now() / 1000));

    console.log(`Fetching from block ${START_BLOCK} to ${END_BLOCK}`);

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
    });

    console.log(`Total transfers fetched: ${allTransfers.length}`);
    DB.close();
}

DB.get('SELECT MAX(blockNumber) as lastBlock FROM transfers', (err, row) => {
    if (err) {
        console.error('DB Query Error:', err);
        fetchTransfers();
    } else {
        const lastBlock = row && row.lastBlock ? row.lastBlock : null;
        console.log(`Last block in db: ${lastBlock || 'none'}`);
        fetchTransfers(lastBlock);
    }
});