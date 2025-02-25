require('dotenv').config();
const { createPublicClient, http, webSocket } = require('viem');
const { base } = require('viem/chains');
const sqlite3 = require('sqlite3').verbose();

function validateEnvVariable(name, parser = (value) => value) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Environment variable ${name} is required but not set`);
    }
    try {
        return parser(value);
    } catch (error) {
        throw new Error(`Invalid value for ${name}: ${error.message}`);
    }
}

// Configuration
const CONFIG = {
    TOKEN_ADDRESS: validateEnvVariable('TOKEN_ADDRESS'),
    TOKEN_INFO: {
        name: validateEnvVariable('TOKEN_NAME'),
        symbol: validateEnvVariable('TOKEN_SYMBOL'),
        decimals: validateEnvVariable('TOKEN_DECIMALS', parseInt),
        creation_block: validateEnvVariable('TOKEN_CREATION_BLOCK', parseInt),
        total_supply: validateEnvVariable('TOKEN_TOTAL_SUPPLY')
    },
    BLOCK_CHUNK: validateEnvVariable('BLOCK_CHUNK', parseInt),
    RETRY_COUNT: validateEnvVariable('RETRY_COUNT', parseInt),
    RETRY_DELAY: validateEnvVariable('RETRY_DELAY', parseInt),
    DB_PATH: validateEnvVariable('DB_PATH'),
    BATCH_SIZE: {
        DB_INSERTS: validateEnvVariable('DB_BATCH_INSERT_SIZE', parseInt),
        TIMESTAMP_FETCHING: validateEnvVariable('TIMESTAMP_BATCH_SIZE', parseInt)
    },
    WEBSOCKET: {
        POLLING_INTERVAL: validateEnvVariable('WS_POLLING_INTERVAL', parseInt),
        MAX_RETRY_ATTEMPTS: validateEnvVariable('WS_MAX_RETRY_ATTEMPTS', parseInt) 
    }
};

// Transfer event ABI
const TRANSFER_EVENT_ABI = [{
    type: 'event',
    name: 'Transfer',
    inputs: [
        { indexed: true, name: 'from', type: 'address' },
        { indexed: true, name: 'to', type: 'address' },
        { indexed: false, name: 'value', type: 'uint256' }
    ]
}];

class TokenTracker {
    constructor() {
        this.db = new sqlite3.Database(CONFIG.DB_PATH);
        this.client = createPublicClient({
            chain: base,
            transport: http(process.env.ALCHEMY_URL)
        });
        this.wsClient = createPublicClient({
            chain: base,
            transport: webSocket(process.env.ALCHEMY_URL.replace('https', 'wss'), {
                retryCount: CONFIG.RETRY_COUNT,
                retryDelay: CONFIG.RETRY_DELAY,
            })
        });
    }

    async initialize() {
        await this.setupDatabase();
        await this.startTracking();
    }

    async setupDatabase() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                try {
                    // Create tables
                    this.db.run(`CREATE TABLE IF NOT EXISTS token_info (
                        token_address TEXT PRIMARY KEY,
                        name TEXT,
                        symbol TEXT,
                        decimals INTEGER,
                        creation_block INTEGER,
                        total_supply TEXT
                    )`);

                    this.db.run(`CREATE TABLE IF NOT EXISTS transfers (
                        txHash TEXT,
                        logIndex INTEGER,
                        fromAddr TEXT,
                        toAddr TEXT,
                        value TEXT,
                        timestamp INTEGER,
                        blockNumber INTEGER,
                        PRIMARY KEY (txHash, logIndex)
                    )`);

                    // Create indexes for better query performance
                    this.db.run(`CREATE INDEX IF NOT EXISTS idx_transfers_block 
                               ON transfers(blockNumber)`);
                    this.db.run(`CREATE INDEX IF NOT EXISTS idx_transfers_addresses 
                               ON transfers(fromAddr, toAddr)`);

                    // Insert token info
                    this.db.run(
                        `INSERT OR IGNORE INTO token_info 
                        (token_address, name, symbol, decimals, creation_block, total_supply) 
                        VALUES (?, ?, ?, ?, ?, ?)`,
                        [
                            CONFIG.TOKEN_ADDRESS,
                            CONFIG.TOKEN_INFO.name,
                            CONFIG.TOKEN_INFO.symbol,
                            CONFIG.TOKEN_INFO.decimals,
                            CONFIG.TOKEN_INFO.creation_block,
                            CONFIG.TOKEN_INFO.total_supply
                        ]
                    );
                    resolve();
                } catch (error) {
                    reject(error);
                }
            });
        });
    }

    async getBlockByTime(timestamp) {
        try {
            const url = `https://api.basescan.org/api?module=block&action=getblocknobytime&timestamp=${timestamp}&closest=before&apikey=${process.env.BASESCAN_API_KEY}`;
            const response = await fetch(url);
            const data = await response.json();
            
            if (data.status !== '1') {
                throw new Error(`API status: ${data.status}, message: ${data.message}`);
            }
            
            return parseInt(data.result);
        } catch (error) {
            console.error('Error fetching block by time:', error);
            throw error;
        }
    }

    async fetchTransferLogsWithRetry(fromBlock, toBlock, retryCount = 3) {
        for (let attempt = 1; attempt <= retryCount; attempt++) {
            try {
                const logs = await this.client.getLogs({
                    address: CONFIG.TOKEN_ADDRESS,
                    event: TRANSFER_EVENT_ABI[0],
                    fromBlock: BigInt(fromBlock),
                    toBlock: BigInt(toBlock)
                });
                return logs;
            } catch (error) {
                if (attempt === retryCount) throw error;
                
                const backoffMs = Math.min(1000 * Math.pow(2, attempt), 10000);
                console.log(`Attempt ${attempt} failed, retrying in ${backoffMs}ms...`);
                await new Promise(resolve => setTimeout(resolve, backoffMs));
            }
        }
    }

    async fetchBlockTimestamps(blockNumbers) {
        const uniqueBlocks = [...new Set(blockNumbers)];
        const timestamps = new Map();
        
        for (let i = 0; i < uniqueBlocks.length; i += CONFIG.BATCH_SIZE.TIMESTAMP_FETCHING) {
            const batch = uniqueBlocks.slice(i, i + CONFIG.BATCH_SIZE.TIMESTAMP_FETCHING);
            const promises = batch.map(async (blockNumber) => {
                try {
                    const block = await this.client.getBlock({ blockNumber: BigInt(blockNumber) });
                    return { blockNumber, timestamp: Number(block.timestamp) };
                } catch (error) {
                    console.error(`Failed to fetch timestamp for block ${blockNumber}:`, error);
                    return { blockNumber, timestamp: Math.floor(Date.now() / 1000) };
                }
            });
            
            const results = await Promise.all(promises);
            results.forEach(({ blockNumber, timestamp }) => {
                timestamps.set(blockNumber, timestamp);
            });
            
            if (i + CONFIG.BATCH_SIZE.TIMESTAMP_FETCHING < uniqueBlocks.length) {
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }
        
        return timestamps;
    }

    async processTransferLogs(logs) {
        const blockNumbers = logs.map(log => Number(log.blockNumber));
        const timestamps = await this.fetchBlockTimestamps(blockNumbers);
        
        return logs.map(log => ({
            txHash: log.transactionHash,
            logIndex: log.logIndex,
            fromAddr: log.args.from,
            toAddr: log.args.to,
            value: log.args.value.toString(),
            timestamp: timestamps.get(Number(log.blockNumber)),
            blockNumber: Number(log.blockNumber)
        }));
    }

    async saveTransfersInBatches(transfers) {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                try {
                    this.db.run('BEGIN TRANSACTION');
                    
                    const stmt = this.db.prepare(
                        `INSERT OR IGNORE INTO transfers 
                        (txHash, logIndex, fromAddr, toAddr, value, timestamp, blockNumber) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)`
                    );

                    for (let i = 0; i < transfers.length; i += CONFIG.BATCH_SIZE.DB_INSERTS) {
                        const batch = transfers.slice(i, i + CONFIG.BATCH_SIZE.DB_INSERTS);
                        batch.forEach(tx => {
                            stmt.run(
                                tx.txHash,
                                tx.logIndex,
                                tx.fromAddr,
                                tx.toAddr,
                                tx.value,
                                tx.timestamp,
                                tx.blockNumber
                            );
                        });
                    }

                    stmt.finalize();
                    this.db.run('COMMIT', (err) => {
                        if (err) {
                            console.error('Error committing transaction:', err);
                            this.db.run('ROLLBACK');
                            reject(err);
                        } else {
                            resolve();
                        }
                    });
                } catch (error) {
                    console.error('Error in batch save:', error);
                    this.db.run('ROLLBACK');
                    reject(error);
                }
            });
        });
    }

    async fetchGap(startBlock, endBlock) {
        console.log(`Fetching gap from block ${startBlock} to ${endBlock}`);
        let allTransfers = [];
        let failedRanges = [];

        // Ensure block numbers are integers
        startBlock = Math.floor(startBlock);
        endBlock = Math.floor(endBlock);
        
        const totalBlocks = endBlock - startBlock;
        // Ensure chunk size is always an integer
        const INITIAL_CHUNK_SIZE = Math.floor(Math.min(CONFIG.BLOCK_CHUNK, Math.ceil(totalBlocks / 10)));
        let currentChunkSize = INITIAL_CHUNK_SIZE;

        for (let block = startBlock; block < endBlock;) {
            // Ensure all block numbers are integers
            const currentBlock = Math.floor(block);
            const chunkEnd = Math.floor(Math.min(currentBlock + currentChunkSize - 1, endBlock));
            
            try {
                const logs = await this.fetchTransferLogsWithRetry(currentBlock, chunkEnd);
                const transfers = await this.processTransferLogs(logs);
                allTransfers.push(...transfers);
                
                console.log(`Fetched ${logs.length} transfers from block ${currentBlock} to ${chunkEnd}`);
                
                // Ensure chunk size calculations result in integers
                currentChunkSize = Math.floor(Math.min(currentChunkSize * 1.5, CONFIG.BLOCK_CHUNK));
                block = chunkEnd + 1; // Move to next block range
                
                await new Promise(resolve => setTimeout(resolve, 200));
            } catch (error) {
                console.error(`Failed to fetch blocks ${currentBlock}-${chunkEnd}:`, error);
                
                // Ensure reduced chunk size is an integer
                currentChunkSize = Math.max(Math.floor(currentChunkSize / 2), 100);
                failedRanges.push({ start: currentBlock, end: chunkEnd });
                
                // Move to next block range even on failure
                block = chunkEnd + 1;
                
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }

        if (failedRanges.length > 0) {
            console.log(`Retrying ${failedRanges.length} failed ranges...`);
            for (const range of failedRanges) {
                try {
                    // Ensure range boundaries are integers
                    const transfers = await this.fetchGap(
                        Math.floor(range.start), 
                        Math.floor(range.end)
                    );
                    allTransfers.push(...transfers);
                } catch (error) {
                    console.error(`Failed to fetch range ${range.start}-${range.end} after retry:`, error);
                }
            }
        }

        if (allTransfers.length > 0) {
            await this.saveTransfersInBatches(allTransfers);
        }

        return allTransfers.length;
    }

    async fetchTransferLogsWithRetry(fromBlock, toBlock, retryCount = 3) {
        // Ensure block numbers are integers
        fromBlock = Math.floor(fromBlock);
        toBlock = Math.floor(toBlock);

        for (let attempt = 1; attempt <= retryCount; attempt++) {
            try {
                const logs = await this.client.getLogs({
                    address: CONFIG.TOKEN_ADDRESS,
                    event: TRANSFER_EVENT_ABI[0],
                    fromBlock: BigInt(fromBlock),
                    toBlock: BigInt(toBlock)
                });
                return logs;
            } catch (error) {
                if (attempt === retryCount) throw error;
                
                const backoffMs = Math.min(1000 * Math.pow(2, attempt), 10000);
                console.log(`Attempt ${attempt} failed, retrying in ${backoffMs}ms...`);
                await new Promise(resolve => setTimeout(resolve, backoffMs));
            }
        }
    }

    async getLastProcessedBlock() {
        return new Promise((resolve) => {
            this.db.get('SELECT MAX(blockNumber) as lastBlock FROM transfers', async (err, row) => {
                if (err) {
                    console.error('Error querying last block:', err);
                    resolve(CONFIG.TOKEN_INFO.creation_block);
                    return;
                }
                resolve(row?.lastBlock || CONFIG.TOKEN_INFO.creation_block);
            });
        });
    }

    async startTracking() {
        try {
            const startBlock = await this.getLastProcessedBlock();
            const currentBlock = await this.getBlockByTime(Math.floor(Date.now() / 1000));
            
            console.log(`Starting from block ${startBlock}, current block: ${currentBlock}`);
    
            if (startBlock < currentBlock) {
                await this.fetchGap(startBlock, currentBlock);
            }
    
            console.log('Starting real-time tracking...');
            try {
                this.wsClient.watchContractEvent({
                    address: CONFIG.TOKEN_ADDRESS,
                    abi: TRANSFER_EVENT_ABI,
                    eventName: 'Transfer',
                    onLogs: this.handleNewTransfers.bind(this),
                    onError: this.handleWebSocketError.bind(this),
                    poll: true, // Enable polling fallback
                    pollingInterval: 1000, // Poll every second if WebSocket fails
                });
            } catch (wsError) {
                console.error('WebSocket setup failed, falling back to polling:', wsError);
                // Implement polling fallback
                this.startPolling(currentBlock);
            }
        } catch (error) {
            console.error('Error in startTracking:', error);
            throw error;
        }
    }

    async startPolling(fromBlock) {
        const pollInterval = setInterval(async () => {
            try {
                const currentBlock = await this.getBlockByTime(Math.floor(Date.now() / 1000));
                const logs = await this.fetchTransferLogsWithRetry(fromBlock, currentBlock);
                
                if (logs.length > 0) {
                    await this.handleNewTransfers(logs);
                    fromBlock = currentBlock + 1;
                }
            } catch (error) {
                console.error('Polling error:', error);
            }
        }, CONFIG.RETRY_DELAY);
    
        // Store interval reference for cleanup
        this.pollInterval = pollInterval;
    }

    async handleNewTransfers(logs) {
        try {
            // Log incoming transfers
            console.log(`Received ${logs.length} new transfers from WebSocket/polling`);
    
            // Process the transfers with timestamps
            const transfers = await this.processTransferLogs(logs);
            
            // Save to database
            await this.saveTransfersInBatches(transfers);
            
            // Log success with block numbers for tracking
            const blocks = [...new Set(transfers.map(t => t.blockNumber))];
            console.log(`Successfully processed and saved ${transfers.length} transfers from blocks ${Math.min(...blocks)} to ${Math.max(...blocks)}`);
        } catch (error) {
            console.error('Error handling new transfers:', error);
            // Optionally, you could implement a retry mechanism here
            const retryQueue = async () => {
                try {
                    console.log('Retrying failed transfers...');
                    await this.handleNewTransfers(logs);
                } catch (retryError) {
                    console.error('Retry failed:', retryError);
                }
            };
            setTimeout(retryQueue, CONFIG.RETRY_DELAY);
        }
    }

    handleWebSocketError(error) {
        console.error('WebSocket error:', error);
        if (error.code === 'SocketClosedError' || error.message.includes('closed')) {
            console.log('Socket closed, attempting to reconnect...');
            
            const reconnectWithBackoff = (attempt = 1) => {
                const backoffMs = Math.min(1000 * Math.pow(2, attempt), CONFIG.RETRY_DELAY);
                console.log(`Reconnection attempt ${attempt} in ${backoffMs}ms...`);
                
                setTimeout(async () => {
                    try {
                        await this.startTracking();
                    } catch (reconnectError) {
                        console.error(`Reconnection attempt ${attempt} failed:`, reconnectError);
                        reconnectWithBackoff(attempt + 1);
                    }
                }, backoffMs);
            };
    
            reconnectWithBackoff();
        }
    }

    async shutdown() {
        console.log('Shutting down tracker...');
        try {
            // Clear polling interval if it exists
            if (this.pollInterval) {
                clearInterval(this.pollInterval);
            }

            // Unsubscribe from WebSocket events if active
            if (this.wsSubscription) {
                await this.wsSubscription.unsubscribe();
            }

            // Close database connection
            if (this.db) {
                await new Promise((resolve, reject) => {
                    this.db.close((err) => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            }

            console.log('Shutdown completed successfully');
        } catch (error) {
            console.error('Error during shutdown:', error);
        }
    }
}

// Main execution
const tracker = new TokenTracker();

tracker.initialize().catch(error => {
    console.error('Initialization error:', error);
    process.exit(1);
});

process.on('SIGINT', () => {
    tracker.shutdown();
    process.exit();
});