require('dotenv').config();
const { createPublicClient, http, webSocket } = require('viem');
const { base } = require('viem/chains');
const sqlite3 = require('sqlite3').verbose();

const TOKEN_ADDRESS = '0x06f71fb90f84b35302d132322a3c90e4477333b0';
const ALCHEMY_URL = process.env.ALCHEMY_URL;
const DB = new sqlite3.Database('token_data.db');

// Create a viem client for Base with WebSocket transport
const client = createPublicClient({
    chain: base,
    transport: webSocket(ALCHEMY_URL.replace('https', 'wss')), // Switch to WebSocket
});

// ABI for ERC-20 Transfer event
const abi = [{
    "type": "event",
    "name": "Transfer",
    "inputs": [
        { "indexed": true, "name": "from", "type": "address" },
        { "indexed": true, "name": "to", "type": "address" },
        { "indexed": false, "name": "value", "type": "uint256" }
    ]
}];

// Watch for Transfer events
client.watchContractEvent({
    address: TOKEN_ADDRESS,
    abi: abi,
    eventName: 'Transfer',
    onLogs: (logs) => {
        logs.forEach((log) => {
            const { from, to, value } = log.args;
            const txHash = log.transactionHash;
            const timestamp = Math.floor(Date.now() / 1000); // Rough estimate

            console.log(`New Transfer: ${from} -> ${to}, Value: ${value.toString()}`);

            DB.run(
                `INSERT OR IGNORE INTO transfers (txHash, fromAddr, toAddr, value, timestamp) VALUES (?, ?, ?, ?, ?)`,
                [txHash, from, to, value.toString(), timestamp],
                (err) => { if (err) console.error(err); }
            );
        });
    },
    onError: (error) => {
        console.error('Error watching events:', error);
    }
});

console.log('Listening for new transfers...');