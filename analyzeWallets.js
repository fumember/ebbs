const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');

const TOKEN_DECIMALS = 18;
const TOTAL_SUPPLY = 1e11 * 10 ** 18; // Placeholder: Update with exact value from Basescan (e.g., 100000000000000 * 10**18)

const db = new sqlite3.Database('./token_data.db', (err) => {
    if (err) console.error('DB Error:', err);
    console.log('Connected to token_data.db');
});

db.all('SELECT fromAddr, toAddr, value, timestamp FROM transfers', [], (err, rows) => {
    if (err) {
        console.error('Query Error:', err);
        return;
    }

    const wallets = {};
    rows.forEach(row => {
        const from = row.fromAddr.toLowerCase();
        const to = row.toAddr.toLowerCase();
        const value = BigInt(row.value);

        // Initialize wallet entries
        if (!wallets[from]) wallets[from] = { balance: BigInt(0), firstSeen: row.timestamp, txs: [] };
        if (!wallets[to]) wallets[to] = { balance: BigInt(0), firstSeen: row.timestamp, txs: [] };

        // Update balances and transaction history
        wallets[from].balance -= value;
        wallets[to].balance += value;
        wallets[from].firstSeen = Math.min(wallets[from].firstSeen, row.timestamp);
        wallets[to].firstSeen = Math.min(wallets[to].firstSeen, row.timestamp);
        wallets[from].txs.push({ type: 'send', value: value, timestamp: row.timestamp });
        wallets[to].txs.push({ type: 'receive', value: value, timestamp: row.timestamp });
    });

    // Process holders
    const holders = Object.entries(wallets)
        .filter(([_, data]) => data.balance > 0)
        .map(([addr, data]) => ({
            address: addr,
            balance: Number(data.balance) / 10 ** TOKEN_DECIMALS,
            firstSeen: new Date(data.firstSeen * 1000).toISOString(),
            holdingDays: (Date.now() / 1000 - data.firstSeen) / (60 * 60 * 24),
            txCount: data.txs.length,
            sends: data.txs.filter(tx => tx.type === 'send').length
        }));

    // Identify whales (>1% supply)
    const whaleThreshold = TOTAL_SUPPLY * 0.01 / 10 ** TOKEN_DECIMALS;
    const whales = holders.filter(h => h.balance >= whaleThreshold);

    // Output results
    console.log('Total Holders:', holders.length);
    console.log('Sample Holders:', holders.slice(0, 5));
    console.log('Whales (>1% supply):', whales);

    // Save to files
    fs.writeFileSync('holders.json', JSON.stringify(holders, null, 2));
    fs.writeFileSync('whales.json', JSON.stringify(whales, null, 2));

    db.close();
});