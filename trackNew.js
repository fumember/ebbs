require('dotenv').config();
const { createPublicClient, webSocket } = require('viem');
const { base } = require('viem/chains');
const sqlite3 = require('sqlite3').verbose();

const TOKEN_ADDRESS = '0x06f71fb90f84b35302d132322a3c90e4477333b0';
const ALCHEMY_URL = process.env.ALCHEMY_URL;
const DB = new sqlite3.Database('token_data.db');

// Handle BigInt serialization
const stringifyBigInt = (key, value) => typeof value === 'bigint' ? value.toString() : value;

function createListener() {
    const client = createPublicClient({
        chain: base,
        transport: webSocket(ALCHEMY_URL.replace('https', 'wss'), {
            retryCount: 5, // Retry up to 5 times
            retryDelay: 5000, // 5 seconds between retries
        }),
    });

    const abi = [{
        "type": "event",
        "name": "Transfer",
        "inputs": [
            { "indexed": true, "name": "from", "type": "address" },
            { "indexed": true, "name": "to", "type": "address" },
            { "indexed": false, "name": "value", "type": "uint256" }
        ]
    }];

    let unwatch; // To store the unwatch function

    function startWatching() {
        unwatch = client.watchContractEvent({
            address: TOKEN_ADDRESS,
            abi: abi,
            eventName: 'Transfer',
            onLogs: (logs) => {
                logs.forEach((log) => {
                    console.log('Raw Log:', JSON.stringify(log, stringifyBigInt, 2));
                    const { from, to, value } = log.args;
                    const txHash = log.transactionHash;
                    const timestamp = Math.floor(Date.now() / 1000);
                    console.log(`Parsed: from=${from}, to=${to}, value=${value.toString()}, txHash=${txHash}`);
                    DB.run(
                        `INSERT OR IGNORE INTO transfers (txHash, fromAddr, toAddr, value, timestamp) VALUES (?, ?, ?, ?, ?)`,
                        [txHash, from, to, value.toString(), timestamp],
                        (err) => { if (err) console.error('DB Error:', err); }
                    );
                });
            },
            onError: (error) => {
                console.error('Event Error:', error);
                if (error.code === 'SocketClosedError' || error.message.includes('closed')) {
                    console.log('Socket closed, attempting reconnect...');
                    unwatch(); // Stop current listener
                    setTimeout(startWatching, 5000); // Retry after 5s
                }
            }
        });
    }

    startWatching();
    console.log('Listening for new transfers...');
}

createListener();

// Keep process alive
process.on('SIGINT', () => {
    console.log('Shutting down...');
    DB.close();
    process.exit();
});