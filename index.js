const { Client } = require("pg");
const { Managers, Transactions, Identities, Utils } = require("@arkecosystem/crypto");
const { BurnTransaction } = require("@solar-network/solar-crypto").Transactions;
Transactions.TransactionRegistry.registerTransactionType(BurnTransaction);

Managers.configManager.setFromPreset("devnet");
Managers.configManager.setHeight("20000000000");

// Old network database connection info
const databaseHost = "localhost";
const databasePort = 5432;
const databaseUser = "solar";
const databaseName = "solar_devnet2";
const databasePassword = "password";

// This script supports only genesis blocks with 0 balance genesis delegates
const genesisDelegatesAddresses = [
    "D68RJzKYsCToEZTRwyNqzKqrxGkDpaRtu5",
    "DEFzMvLFSUuXabT4E3BMbsdx1RQZ9o2V8w",
    "DKxMLweDUWGeTSQLXjVQ3hcFgRszoWnfjU",
    "DMDhBWnjSLKkJ7YMnRC8pTYcEn5tyitLUM",
    "DJjuD3AaegstYouf3cFpbHfNLmBdtmafo7",
    "DTDKjEHGmxeSPFk4MRzNQJN1Vv6qXHdDdA",
    "DKUD4jcvhk7KMmyA4z3WSgu5SG8NLVN2NP",
    "DGfBdJms4MWivAF86W66C3tN28EUvga6CV",
    "DAJwoaDSUJwWn7AxHweDAwmbL6eXro3LTA",
    "DKKj2Dnix2S2RGtLwj6dM7PdZj1fhrWD14",
    "D5YdijUnmapK9kGPEsJBEbpBHEjCrmKhTU",
    "DBLQ9cw3d6rDsSzvKX6wK9pz9dtW7KjSxu",
    "DE8ysdVSMmZPVmwWRWVYV4TsMgR2PRBrsJ",
    "DPRzUjCwepbZhj2CnXgjhvD25SZC6YAkCN",
    "DGV821m87qK1Bi2DMVKtaiKk2Ro1akEzeT",
    "DP4y9tFuGBf424hb6AFzGLPFnrByhKR1eg",
    "DJLvEGefsFFcrYVaKogCdkzQaobursC1w2",
    "D5LBpz86Xu1syAF1kQ9LKCEX7ksFvWqoht",
    "DLRCshJng9td7u4iWt5jffBjXsh2F1W34h",
    "DDxSc5d3c113xghZeoYuLWQ2HRx3Pwx6V7",
    "DK7MsTui18ZNQotLKEg1CwFBKENYXQgPea",
    "DJYdij7p6zJMgxRZcrKiA6Xq4qsRuEfkm8",
    "DBbFo45XABShNAZQBFypksENYXHQgmrT8h",
    "D8vEC9KpcoVjTGky4U1N1b21wPBHr1Acby",
    "DE1wCUPQY6fUgiGjFoHeZzZayoNdTp9xkv",
    "D77n32g1bhVBtSjj6TgerHvQPuCZKGxY3N",
    "DD1vxUsGz4TbDhvTx1X6NJEFULLzagMmt3",
    "D7gbxd6TzNDn3LAGC6qXCNeiPXCnqLb8df",
    "DHsDLYnVevNehmTSTnfrNf9wputqqNYpi2",
    "DQDH1i4t6YCsjSztJRneCZhskgCyJWwnkK",
    "DGbEJ45sHTy54TDbnB6W62eZuA7VajjnTp",
    "DFxbA1jQc9mwJf9LuqWFvVPhhZkzfsdxHK",
    "DR34H571QqNY4WMiT1ACFXq8PgfPXyuTRg",
    "D6jwbcmgRhpVLxgRuFRGSuVfEzm3cHYPvz",
    "DRnCEL2YLyMqzUG9Rc58nz3MWLLgK9twuw",
    "DF5eBjUra4yRCdV9UJkvhBo6Q92CFw9eQ3",
    "DPdyWxuD5BcJUybTjLD8JKmxKL2Z46qhd1",
    "DN1dBqThVWab7afwhACEqx2A3uLE8Zsg4D",
    "DQrVKHwd91AEwRdjkrsi4DeXZ155PqPB3f",
    "DKh8oWdKfwYQcbi2YnhY2f9xD4Ujr7ajq6",
    "DCxv3y98Mgbu3V5yjkaG1HCp1dvbUmrM7B",
    "DBekBUWLsdeDTRAGtf1JctQUV4BcxjpQM9",
    "D7TtMZC659WiwjesV4CgKZ8LCLnQmaahCB",
    "DSVbmhJ1shKEN84Cranx8WcQRp5vUz7XVR",
    "DQ1hJU1fZWXi8rx6f5PKaEDsYuYQD3C22u",
    "DEkV6xhSqgZyoo5GuxnEgbBygHuQxWb6D2",
    "DBqT9pZnA3uypABHtr4Yjv9FsDMP8QaH6i",
    "DF9ANea2jVe8KrATPxfMqmEgSuZBRWyenp",
    "DT6sGWvp2MRUKFGqHJXhmhpdNwutvhByA6",
    "DQo5BXVQ1GrocHKd1bbBW93tBueKDMXPbe",
    "DPg8idHEC6s4rvNMUm5bPVwn4XEEWubevk",
    "D5iC8dxF7Dvg4eRPXcdG5petoSj73n8hqt",
    "DRYpgcx9dB7rMFi3ANDQUjmHBYPvC3w9zN"
];

const genesisAddress = "DANNttGq27aVxynEUZbxnyhgenh9WUEZgu";
// Genesis wallet balance after genesis block (The initial supply MUST cover outgoing transactions PLUS all rewards (and fees) forged in the old network)
const initalGenesisBalance = "52073757600000000";

// In solar case, all funds were sent to another address from genesisBlock as first transaction so we need this wallet passphrase
const genesisPassphrase = "XXX";

const transactionsInABlock = 150;

const client = new Client({
    user: databaseUser,
    host: databaseHost,
    database: databaseName,
    password: databasePassword,
    port: databasePort
});

client.connect();

// Transaction batches
let batches = [];

const addressMap = {}; // address -> {balance, pendingTransactions}

let currentDelegates = genesisDelegatesAddresses;

let registeringDelegates = [];

const votesPerDelegate = {};

// Initialise the genesis wallet data
addressMap[genesisAddress] = {};
addressMap[genesisAddress].balance = Utils.BigNumber.make(initalGenesisBalance);
addressMap[genesisAddress].nextBlockBalance = Utils.BigNumber.ZERO;
addressMap[genesisAddress].pendingTransactions = [];
addressMap[genesisAddress].index = 0;
addressMap[genesisAddress].broadcast = 1;
addressMap[genesisAddress].vote = 1;

async function go() {
    const blocks = await client.query("SELECT generator_public_key, SUM(total_fee + reward) FROM blocks GROUP BY generator_public_key;");

    const forgers = blocks.rows;
    forgers.forEach((o) => (o.recipientId = Identities.Address.fromPublicKey(o.generator_public_key)));

    // We create the first transaction for genesis (in this case the wallet where funds were sent to) to send all forging rewards to delegates
    // All later transactions MUST be recreated with nonce = nonce + 1
    const multiPayment = Transactions.BuilderFactory.multiPayment().nonce(1).fee(10000000);
    for (const forger of forgers) {
        if (forger.sum > 0) {
            multiPayment.addPayment(forger.recipientId, forger.sum);
        }
    }
    multiPayment.sign(genesisPassphrase);

    const results = await client.query("SELECT encode(serialized, 'hex') FROM transactions WHERE block_height > 1 ORDER BY nonce;");

    // Solar specific: We initialise the wallet where genesis funds were sent to
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"] = {};
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].balance = Utils.BigNumber.ZERO;
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].nextBlockBalance = Utils.BigNumber.ZERO;
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].pendingTransactions = [];
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].index = 0;
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].broadcast = 1;
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].vote = 1;
    addressMap["DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr"].pendingTransactions.push(multiPayment.getStruct());

    client.end();

    for (const result of results.rows) {
        try {
            let transaction = Transactions.TransactionFactory.fromSerialized(Buffer.from(result.encode, "hex")).data;
            // Exceptions for DKo1Co1ihFpWqFt2zH5Xhb8C2xr8VF44Lr transactions. They are replaced with new transactions with new nonce
            if (transaction.id === "05b3fa595cb07e6dcbf3439cf709ec7ae6a117121586880b04704f22ca0e6f46") {
                transaction = {
                    id: "8a595ef4726be2ba20f464733c936a3f300a441d016d5ae74619d9301a6c53a9",
                    signature: "24d7ff75e275df38f275f29654cbe02cdf167bdf226376ea72ba35ed32378a6c455e3685a2c3aa0964c5b988505252f8887bf8107ebd5f4bee041f6468501b1d",
                    version: 2,
                    type: 0,
                    fee: Utils.BigNumber.make("10000000"),
                    senderPublicKey: "03dd1c48f72c24c200a3ce5d4c90b65a9a474141dcb70960394d3e844db7c084f2",
                    typeGroup: 1,
                    nonce: "2",
                    amount: Utils.BigNumber.make("500000000000"),
                    recipientId: "DNd2nfU6FWs1CJdbDyWbm6nPAXZcSexGKL",
                    expiration: 0
                };
            }
            if (transaction.id === "06d1810da75ef11e88398bddd7d9d8b4c49150c9702da434e9994858e1919f0f") {
                transaction = {
                    id: "d2af3b8775104bfa87a3dd2a26ee91c731088cc5da09788b1cdc188ad18026c2",
                    signature: "6fcf9f3a0ef4dc4d8451facfe6f71d10f1ca417d79283d93b7e3beb67884a9cc37840c60482840e6ae0c317b00fd6a4ee1ab79c59364d6e239c4371ee4351716",
                    version: 2,
                    type: 0,
                    fee: Utils.BigNumber.ZERO,
                    senderPublicKey: "03dd1c48f72c24c200a3ce5d4c90b65a9a474141dcb70960394d3e844db7c084f2",
                    typeGroup: 2,
                    nonce: "3",
                    amount: Utils.BigNumber.make("10000000000000000")
                };
            }

            // Create all the senders wallet info and push their transactions in pendingTransactions
            const sender = Identities.Address.fromPublicKey(transaction.senderPublicKey);
            if (addressMap[sender]) {
                addressMap[sender].pendingTransactions.push(transaction);
            } else {
                addressMap[sender] = {};
                addressMap[sender].pendingTransactions = [];
                addressMap[sender].pendingTransactions.push(transaction);
                addressMap[sender].balance = Utils.BigNumber.ZERO;
                addressMap[sender].nextBlockBalance = Utils.BigNumber.ZERO;
                addressMap[sender].index = 0;
                addressMap[sender].broadcast = 1;
                addressMap[sender].vote = 1;
            }

            // This structure is needed to avoid resigning delegates before every vote is cast
            // It just counts all the voted transactions for every delegate
            if (transaction.type === 3) {
                // 3 vote
                for (const vote of transaction.asset.votes) {
                    if (vote.slice(0, 1) === "-") {
                        continue;
                    }
                    const voted = Identities.Address.fromPublicKey(vote.slice(1));
                    if (votesPerDelegate[voted]) {
                        votesPerDelegate[voted] += 1;
                    } else {
                        votesPerDelegate[voted] = 1;
                    }
                }
            }
        } catch (err) {
            console.log("Not recognised transaction");
            throw err;
        }
    }

    let batch = [];
    let oldBatchLength = -1;
    let oldBatchesLength = -1;

    // Cycle until no new blocks are added
    while (oldBatchesLength !== batches.length) {
        oldBatchesLength = batches.length;
        // Cycle until no new transactions can be added to this block or we reached maximum
        while (batch.length < transactionsInABlock && oldBatchLength !== batch.length) {
            oldBatchLength = batch.length;
            // Cycle through all the wallets and check if they can issue a transaction in the current block
            for (const [key, address] of Object.entries(addressMap)) {
                if (batch.length >= transactionsInABlock) {
                    break;
                }
                // Broadcast = 0 means that the wallet cannot send any transaction in the current block
                // It happens for example when the wallet has a second signature registration in the same block
                if (address.broadcast == 0) {
                    continue;
                }
                // Check if all the transactions are sent already
                if (address.pendingTransactions.length <= address.index) {
                    continue;
                }

                const transaction = address.pendingTransactions[address.index];
                switch (transaction.type) {
                    case 0: {
                        // 0 transfer and burn
                        // Check if the wallet has enough funds to issue this transaction
                        if (address.balance.isGreaterThanEqual(transaction.amount.plus(transaction.fee))) {
                            // If it does, update balance of sender and recipient (recipient funds are added to nextBlockBalance and will be added to the actual balance only from next block)
                            address.balance = address.balance.minus(transaction.amount.plus(transaction.fee));
                            if (addressMap[transaction.recipientId] && transaction.typeGroup == 1) {
                                addressMap[transaction.recipientId].nextBlockBalance = addressMap[transaction.recipientId].nextBlockBalance.plus(transaction.amount);
                            }
                            // The index points to the next transaction to be issued
                            address.index += 1;
                            batch.push(transaction);
                        }
                        continue;
                    }
                    case 1: {
                        // 1 second signature
                        if (address.balance.isGreaterThanEqual(transaction.fee)) {
                            address.balance = address.balance.minus(transaction.fee);
                            address.index += 1;
                            batch.push(transaction);
                            // This transaction means that no other transaction on this wallet can be issued in this block
                            address.broadcast = 0;
                        }
                        break;
                    }
                    case 2: {
                        // 2 delegate registration
                        if (address.balance.isGreaterThanEqual(transaction.fee)) {
                            address.balance = address.balance.minus(transaction.fee);
                            address.index += 1;
                            // We add the delegate to delegates that are registering in this block but not still registered
                            registeringDelegates.push(key);
                            batch.push(transaction);
                        }
                        break;
                    }
                    case 3: {
                        // 3 vote
                        // Can only vote if you didn't already vote on the same block
                        if (address.balance.isGreaterThanEqual(transaction.fee) && address.vote === 1) {
                            // Check that the delegate we are voting for was already registered
                            const vote = transaction.asset.votes.filter((o) => o.slice(0, 1) === "+")[0];
                            if (vote !== undefined && !currentDelegates.includes(Identities.Address.fromPublicKey(vote.slice(1)))) {
                                continue;
                            }

                            // Update the votesPerDelegate struct by decreasing by one for every vote transaction for that delegate
                            // This will be used to avoid delegate resignation before all votes are cast
                            for (const vote of transaction.asset.votes) {
                                if (vote.slice(0, 1) === "-") {
                                    continue;
                                }
                                const voted = Identities.Address.fromPublicKey(vote.slice(1));
                                votesPerDelegate[voted] -= 1;
                            }

                            address.balance = address.balance.minus(transaction.fee);
                            address.index += 1;
                            batch.push(transaction);
                            // No other voting transaction is allowed from the same address in the current block
                            address.vote = 0;
                        }
                        break;
                    }
                    case 6: {
                        // 6 multipayment
                        let amount = Utils.BigNumber.ZERO;
                        for (const recipient of transaction.asset.payments) {
                            amount = amount.plus(recipient.amount);
                        }
                        if (address.balance.isGreaterThanEqual(amount.plus(transaction.fee))) {
                            address.balance = address.balance.minus(amount.plus(transaction.fee));
                            for (const recipient of transaction.asset.payments) {
                                if (addressMap[recipient.recipientId]) {
                                    addressMap[recipient.recipientId].nextBlockBalance = addressMap[recipient.recipientId].nextBlockBalance.plus(recipient.amount);
                                }
                            }
                            address.index += 1;
                            batch.push(transaction);
                        }
                        continue;
                    }
                    case 7: {
                        // 7 resignation
                        if (address.balance.isGreaterThanEqual(transaction.fee)) {
                            // We check that all the votes for this delegate are already cast
                            if (votesPerDelegate[key] !== 0 && votesPerDelegate[key] !== undefined) {
                                continue;
                            }

                            address.balance = address.balance.minus(transaction.fee);
                            address.index += 1;
                            // We remove it from current available delegates
                            currentDelegates = currentDelegates.filter((delegate) => delegate != key);
                            batch.push(transaction);
                        }
                        break;
                    }
                    default:
                        console.log("Someone sent some other transaction");
                }
            }
        }

        // Here we are when a block is complete

        for (const [_, address] of Object.entries(addressMap)) {
            // Vote flag is reset
            address.vote = 1;
            // Broadcast flag is reset
            address.broadcast = 1;
            // Sent balances are now available in the recipients wallets
            address.balance = address.balance.plus(address.nextBlockBalance);
            address.nextBlockBalance = Utils.BigNumber.ZERO;
        }

        if (batch.length > 0) {
            // We push the new block in batches
            batches.push(batch);
            batch = [];
            oldBatchLength = -1;
            // We make delegate registration effective
            registeringDelegates.forEach((o) => currentDelegates.push(o));
            registeringDelegates = [];
        }
    }

    console.error(`We have a total of ${batches.length} blocks to replay`);
    let allReplayed = true;
    let notReplayedCount = 0;
    for (const [_, address] of Object.entries(addressMap)) {
        if (address.pendingTransactions.length <= address.index) {
            continue;
        }
        allReplayed = false;
        notReplayedCount += address.pendingTransactions.length - address.index;
    }
    if (!allReplayed) {
        console.error(`Not all transactions have been replayed. This could be a bug. ${notReplayedCount} transaction(s) missing.`);
    }
    console.log(JSON.stringify(batches));
}

go();
