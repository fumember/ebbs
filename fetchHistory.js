require('dotenv').config();
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

const TOKEN_ADDRESS = '0x06f71fb90f84b35302d132322a3c90e4477333b0';
const BASESCAN_API_KEY = process.env.BASESCAN_API_KEY;
const DB = new sqlite3.Database('token_data.db');

DB.serialize(() => {
    DB.run(`CREATE TABLE IF NOT EXISTS transfers (
    txHash TEXT PRIMARY KEY,
    fromAddr TEXT,
    toAddr TEXT,
    value TEXT,
    timestamp INTEGER
  )`);
});

async function fetchTransfers() {
    let page = 1;
    let allTransfers = [];

    while (true) {
        const url = `https://api.basescan.org/api?module=account&action=tokentx&contractaddress=${TOKEN_ADDRESS}&page=${page}&offset=10000&sort=asc&apikey=${BASESCAN_API_KEY}`;
        const response = await axios.get(url);
        const data = response.data;

        if (data.status !== '1' || !data.result.length) break;

        allTransfers = allTransfers.concat(data.result);
        console.log(`Fetched page ${page}: ${data.result.length} transfers`);
        page++;

        DB.serialize(() => {
            const stmt = DB.prepare(`INSERT OR IGNORE INTO transfers (txHash, fromAddr, toAddr, value, timestamp) VALUES (?, ?, ?, ?, ?)`);
            data.result.forEach(tx => {
                stmt.run(tx.hash, tx.from, tx.to, tx.value, tx.timeStamp);
            });
            stmt.finalize();
        });
    }

    console.log(`Total transfers fetched: ${allTransfers.length}`);
    DB.close();
}

fetchTransfers().catch(console.error);