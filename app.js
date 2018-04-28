// const steem = require('steem');
const MongoClient = require("mongodb").MongoClient;
const f = require('util').format;
const u = encodeURIComponent("steemit");
const password = encodeURIComponent("steemit");
const moment = require("moment");
const authMechanism = 'DEFAULT';
// Connection URL
const url = f('mongodb://%s:%s@mongo1.steemdata.com:27017/SteemData?authMechanism=%s',
u, password, authMechanism);
const steem_per_mvests = require("./steem_per_mvests.json");
const steem_per_mvests_blocks = Object.keys(steem_per_mvests).map(a => parseInt(a, 10)).sort((a, b) => a - b);
const fs = require('fs');
let client, db;
if (process.argv.length < 3) {
    const path = require('path');
    let fileName = path.basename(__filename);
    console.log(`Usage: node ${fileName} userName`);
    process.exit();
}

const user = process.argv[2];
const CHECK = process.argv[3] === "true";
const NO_GROUPING = process.argv[4] === "true";
const FILTER_TYPE = process.argv[5];

/*
* For accounting purposes we should record steem equivalents of any
* income/mining rewards received as VESTS. To keep balances correct, the STEEM
* equivalents will be recorded then immediately transfered away
 */
const RECORD_STEEM_EQUIVALENTS = true;

/* Define some blockchain constants */
const STEEM_BLOCK_INTERVAL = 3;
const STEEM_BLOCKS_PER_DAY = (24*60*60/STEEM_BLOCK_INTERVAL);
const STEEM_START_MINER_VOTING_BLOCK = (STEEM_BLOCKS_PER_DAY * 30);
const STEEM_HARDFORK_0_1_TIME = 1461605400; // 2016-04-25 17:30:00.000Z
const STEEM_HARDFORK_0_1_TIME_MOMENT = moment(STEEM_HARDFORK_0_1_TIME * 1000);
const STEEMIT_BLOCKCHAIN_PRECISION = 1000;
const VESTS_SHARE_SPLIT = 1000000;
const STEEM_PER_MVESTS_MAX_BLOCK = steem_per_mvests_blocks.reduce((a, b) => {
    return (b > a ? b : a);
}, 0);
const EXCHANGE = "STEEM Blockchain";
const POWER = "STEEM-Power";

let assetMovements = {
    STEEM: [],
    SBD: [],
    mVESTS: []
};
let transfers = {};
let runningBalance = {
    STEEM: [],
    SBD: [],
    mVESTS: []
};
let movementTypes = {};

let sum = 0;
function trackMovements(asset, amount, type, timestamp) {
    assetMovements[asset].push(amount);
    runningBalance[asset].push([type, amount, new Date(timestamp)]);

    if (!movementTypes[asset]) movementTypes[asset] = {};
    if (!movementTypes[asset][type]) movementTypes[asset][type] = {deposit: [], withdrawal: []};

    movementTypes[asset][type][amount > 0 ? "deposit" : "withdrawal"].push(amount);
}

function getFinalBalance(asset) {
    let sum = 0;
    if (!assetMovements[asset]) return 0;
    assetMovements[asset].forEach(movement => {
        sum += movement;
    });
    return sum;
}

function connectMongo() {
    return new Promise((resolve, rej) => {
        MongoClient.connect(url).then((cli) => {
            client = cli;
            db = cli.db();
            resolve();
        }).catch(rej);
    })
}

function disconnectMongo() {
    client.close();
}

let accountBalances = {};
let accountInfo = {};
function getBatch() {
    // Fetch all of the account's operations from SteemData
    return new Promise((resolve, reject) => {
        const collection = db.collection( "AccountOperations" );
        const accountCollection = db.collection("Accounts");
        console.time("**** Done fetching data, time taken: ");
        console.log(`**** FETCHING DATA FOR ${user}, THIS MAY TAKE SEVERAL MINUTES.... ****`)
        Promise.all([
            collection.find({account: user, type: {"$nin": ["vote", "comment", "feed_publish", "comment_options"]}}).toArray(),
            accountCollection.find({name: user}).toArray()
        ])
        .then(results => {
            console.timeEnd("**** Done fetching data, time taken: ");

            let [result, account] = results;
            accountBalances["STEEM"] = parseCurrency(account[0].balance, account[0].updatedAt);
            accountBalances["STEEM_SAVINGS"] = parseCurrency(account[0].savings_balance, account[0].updatedAt);
            accountBalances["SBD"] = parseCurrency(account[0].sbd_balance, account[0].updatedAt);
            accountBalances["SBD_SAVINGS"] = parseCurrency(account[0].savings_sbd_balance, account[0].updatedAt);
            accountBalances["mVESTS"] = parseCurrency(account[0].vesting_shares, account[0].updatedAt);
            accountInfo.mined = account[0].mined;
            accountInfo.created = account[0].created;
            console.log("\n____ " + user + " ____\n");
            console.log("# of entries found:", result.length);
            resolve(result);
        }).catch(err => {
            console.log("getBatch error:", err);
        })
    });
}

function mVESTS(amount) {
    return parseFloat((amount / 1000000));
}

function parseCurrency(amount, timestamp) {
    if (amount.asset === "VESTS") {
        /*
        * On April 25 a reverse share-split hardfork was implemented to address
        * VESTS precision, making 1 old VESTS equal to VESTS_SHARE_SPLIT new VESTS
        * VESTS_SHARE_SPLIT = 1000000
        */
        if (new Date(timestamp).getTime() / 1000 <= STEEM_HARDFORK_0_1_TIME) {
            return {
                amount: amount.amount,
                currency: "mVESTS"
            }
        }

        return {
            amount: mVESTS(amount.amount),
            currency: "mVESTS"
        }
    }
    return {
        amount: parseInt(amount.amount * STEEMIT_BLOCKCHAIN_PRECISION, 10),
        currency: amount.asset
    };
}

function printAmount(amount) {
    if (amount.currency === "mVESTS") return amount.amount;
    else {
        return (amount.amount / STEEMIT_BLOCKCHAIN_PRECISION).toFixed(3);
    }
}

// https://steemit.com/steemdev/@holger80/how-to-estimate-historic-steempermvests-values-for-converting-old-rewards-from-vest-to-steem
function steemPerMvests(timestamp, block) {

    /*
    * The above formula is highly inaccurate for the early period of Steem,
    * so we use stored values of steem_per_mvests from the blockchain instead
    * (values extracted manually by svk31)
    */
    let actual;
    if (block <= STEEM_PER_MVESTS_MAX_BLOCK) {
        let index = steem_per_mvests_blocks.findIndex(a => {
            return a >= block;
        });

        let x0 = steem_per_mvests_blocks[index - 1];
        let y0 = steem_per_mvests[x0];
        let x1 = steem_per_mvests_blocks[index];
        let y1 = steem_per_mvests[x1];
        actual = y0 + (block - x0)*(y1 - y0)/(x1 - x0);
    }
    const a = 2.1325476281078992e-05;
    const b = -31099.685481490847;

    const a2 = 2.9019227739473682e-07;
    const b2 = 48.41432402074669;

    /* Formula debugging */
    // if (timestamp < (b2-b)/(a-a2)) {
    //     if (actual) console.log(new Date(timestamp * 1000), block, "mVestsDelta:", actual -( a * timestamp + b));
    // } else if (actual) {
    //     console.log(new Date(timestamp * 1000), block, "mVestsDelta", actual - (a2 * timestamp + b2));
    // } else {
    //     console.log(new Date(timestamp * 1000), block, "no actual mVests calc");
    // }

    if (actual) return actual;
    if (timestamp < (b2-b)/(a-a2)) {
        return a * timestamp + b;
    } else {
        return a2 * timestamp + b2;
    }
}

function filterEntries(entries) {
    let previous_pow;
    let possibleDuplicates = {};
    let entriesKeys = Object.keys(entries);
    for (var i = entriesKeys.length - 1; i >= 0; i--) {
        let trx_id = entriesKeys[i];
        let {
            timestamp,
            type,
            data
        } = entries[trx_id];
        let t1 = moment(timestamp);

        if (!possibleDuplicates[type]) possibleDuplicates[type] = {};

        if (!!FILTER_TYPE) {
            if (type !== FILTER_TYPE) {
                delete entries[trx_id];
                continue;
            }
        }

        switch (type) {

            case "fill_vesting_withdraw": {
                /*
                * fill_vesting_withdraw operations have a weird double
                * accounting with duplicate entries and entries with 0 deposited
                * or withdrawn amounts, these should be filtered out
                */
                if (data.deposited.amount === 0 || data.withdrawn.amount === 0) {
                    delete entries[trx_id];
                    break;
                }

                let key = data.block + data.index + data.withdrawn.amount + data.deposited.amount;
                if (!!possibleDuplicates[type][key]) {
                    delete entries[trx_id];
                }
                possibleDuplicates[type][key] = true;
                break;
            }

            case "producer_reward":
            case "curation_reward":
            case "pow":
            case "author_reward":
                /*
                * some operations have duplicate entries that
                * should be filtered out
                */
                let k = data.block + data.index;
                if (!!possibleDuplicates[type][k]) {
                    // console.log(`Remove duplicate ${type}`, data);
                    delete entries[trx_id];
                }
                possibleDuplicates[type][k] = true;
                break;

            default:
                let key = data.block + data.index;
                if (possibleDuplicates[type][key]) {
                    console.log("*** Possible duplicate:", data, possibleDuplicates[type][key]);
                }
                possibleDuplicates[type][key] = data;
                break;
        }
    }
    console.log(`Removed ${entriesKeys.length - Object.keys(entries).length} entries by filtering`);
    return entries;
}

function groupEntries(entries) {
    let previous_producer_reward, previous_curation_reward, previous_fill, previous_author_reward;
    let entriesKeys = Object.keys(entries);
    for (var i = entriesKeys.length - 1; i >= 0; i--) {
        let trx_id = entriesKeys[i];
        let {
            timestamp,
            type,
            data
        } = entries[trx_id];
        let t1 = moment(timestamp);

        switch (type) {
            case "producer_reward": {
                /* Group all producer rewards received in the same week */
                let t0 = !!previous_producer_reward ? moment(previous_producer_reward.timestamp) : null;

                let t1BeforeHardFork = t1.isBefore(STEEM_HARDFORK_0_1_TIME_MOMENT);

                if (
                    !!previous_producer_reward &&
                    t0.isSame(t1, "day") &&
                    previous_producer_reward.data.vesting_shares.asset === data.vesting_shares.asset
                ) {
                    let t0BeforeHardfork = t0.isBefore(STEEM_HARDFORK_0_1_TIME_MOMENT);

                    /* Only group producer_rewards if they're both either before or after the hardfork */
                    if (t1BeforeHardFork === t0BeforeHardfork) {
                        data.vesting_shares.amount = data.vesting_shares.amount + previous_producer_reward.data.vesting_shares.amount;
                        entries[trx_id].data = data;
                        delete entries[previous_producer_reward.trx_id];
                    }
                }
                previous_producer_reward = {data, timestamp, trx_id};
                break;
            }

            case "curation_reward": {
                /* Group all curation rewards received in the same week*/
                let t0 = !!previous_curation_reward ? moment(previous_curation_reward.timestamp) : null;
                if (
                    !!previous_curation_reward &&
                    t0.isSame(t1, "day") &&
                    previous_curation_reward.data.reward.asset === data.reward.asset
                ) {
                    data.reward.amount = data.reward.amount + previous_curation_reward.data.reward.amount;
                    entries[trx_id].data = data;
                    delete entries[previous_curation_reward.trx_id];
                }
                previous_curation_reward = {data, timestamp, trx_id};
                break;
            }

            case "author_reward": {
                /* Group all curation rewards received in the same week*/
                let t0 = !!previous_author_reward ? moment(previous_author_reward.timestamp) : null;
                if (
                    !!previous_author_reward &&
                    t0.isSame(t1, "day")
                ) {
                    data.sbd_payout.amount = data.sbd_payout.amount + previous_author_reward.data.sbd_payout.amount;
                    data.steem_payout.amount = data.steem_payout.amount + previous_author_reward.data.steem_payout.amount;
                    data.vesting_payout.amount = data.vesting_payout.amount + previous_author_reward.data.vesting_payout.amount;
                    entries[trx_id].data = data;
                    delete entries[previous_author_reward.trx_id];
                }
                previous_author_reward = {data, timestamp, trx_id};
                break;
            }

            case "fill_order": {
                /* Group all fill_orders received within 1 hour of each other*/
                let t0 = !!previous_fill ? moment(previous_fill.timestamp) : null;
                if (
                    !!previous_fill &&
                    t0.isSame(t1, "hour") &&
                    previous_fill.data.current_owner === data.current_owner &&
                    previous_fill.data.open_owner === data.open_owner &&
                    previous_fill.data.current_pays.asset === data.current_pays.asset &&
                    previous_fill.data.open_pays.asset === data.open_pays.asset
                ) {
                    data.current_pays.amount = data.current_pays.amount + previous_fill.data.current_pays.amount;
                    data.open_pays.amount = data.open_pays.amount + previous_fill.data.open_pays.amount;
                    entries[trx_id].data = data;
                    delete entries[previous_fill.trx_id];
                }
                previous_fill = {data, timestamp, trx_id};
            }
        }
    }
    console.log(`Removed ${entriesKeys.length - Object.keys(entries).length} entries by grouping`);
    return entries;
}

function asSteemPower(amount, timestamp, block) {
    let spmv = steemPerMvests(timestamp, block);

    return {amount: amount.amount * spmv, currency: "STEEM"};
}

function addEquivalentSteem(output) {
    let balances = runningBalance["mVESTS"];
    let now = moment();

    if (!balances.length) return;

    let startDate = "2016-04-25T17:31:00.000Z"; // 30 days after genesis block, end of mining
    let date = moment(startDate);

    function findBalance(date, bal) {
        if (date.isBefore(moment(bal[0][2]))) return 0;
        if (date.isAfter(moment(bal[bal.length - 1][2]))) return Math.max(0, bal[bal.length - 1][3]);
        let balanceIndex = bal.findIndex((a, i) => {
            let d = moment(a[2]);
            // console.log("date.isBefore(d)", date.isBefore(d), "date", date, "d:", d);
            return date.isBefore(d);
        });
        return Math.max(0, bal[Math.max(0, balanceIndex - 1)][3]);
    }

    let previousBalance;
    while (date.isBefore(now)) {

        let spmv = steemPerMvests(date.unix());
        let mvestBalance = findBalance(date, balances);
        let balance = parseCurrency({amount: mvestBalance * spmv, asset: "STEEM"});

        if (balance) {
            if (!previousBalance) {
                output = addOutputEntry(output, "Deposit", balance, null, null, "STEEM-Power",
                    "Steem Equivalents", `${user} estimated mVESTS value`, date.toString(), "dummy");
            } else if (balance.amount !== previousBalance.amount) {
                /* Add withdrawal of previous amount */
                if (previousBalance.amount !== 0) output = addOutputEntry(output, "Withdrawal", null, previousBalance, null, "STEEM-Power",
                    "Steem Equivalents", `${user} estimated mVESTS value`, date.subtract(1, "s").toString(), "dummy");

                /* Add the new equivalent value */
                if (balance.amount !== 0) output = addOutputEntry(output, "Deposit", balance, null, null, "STEEM-Power",
                    "Steem Equivalents", `${user} estimated mVESTS value`, date.add(1, "s").toString(), "dummy");

            }
            previousBalance = balance;
        }

        /* Iterate throught the dates of the 1st, the 15th, and the last day of the month */
        if (date.date() === 1) date.date(15);
        else if (date.date() === 15) date.date(31);
        else date.add(1, "month").date(1);

    }

    return output;
}

function addOutputEntry(output, type, buy, sell, fee, exchange, tradeGroup, comment, date, opType, block) {
    if (!buy) buy = {amount: "", currency: ""};
    if (!sell) sell = {amount: "", currency: ""};
    if (!fee) fee = {amount: "", currency: ""};

    if (opType !== "dummy" && buy.amount) trackMovements(buy.currency, buy.amount, opType, date);
    if (opType !== "dummy" && sell.amount) trackMovements(sell.currency, -sell.amount, opType, date);
    if (opType !== "dummy" && fee.amount) trackMovements(fee.currency, -fee.amount, opType, date);

    output.push([
        type, printAmount(buy), buy.currency, printAmount(sell),
        sell.currency, printAmount(fee), fee.currency, exchange || EXCHANGE,
        tradeGroup || "", comment || "", date
    ]);

    return output;
}

function recordSteemEquivalent(out, recordType, vests, comment, timestamp, type, block) {
    if (!RECORD_STEEM_EQUIVALENTS) return out;
    let trxDate = new Date(timestamp).getTime() / 1000;
    let steemEquivalent = parseCurrency({
        amount: steemPerMvests(trxDate, block)  * vests.amount,
        asset: "STEEM"
    });

    out = addOutputEntry(
        out, recordType, steemEquivalent, null, null,
        null, null, comment, timestamp, type, block
    );
    out = addOutputEntry(
        out, "Withdrawal", null, steemEquivalent, null,
        null, null, comment, timestamp, type, block
    );

    return out;
}

function doReport(recordData) {
    let out = [];
    out.push([
        'Type',
        "Buy Amount",
        "Buy Currency",
        "Sell Amount",
        "Sell Currency",
        "Fee Amount",
        "Fee Currency",
        "Exchange",
        "Trade Group",
        'Comment',
        'Date'
    ]);

    recordData = filterEntries(recordData);
    if (!NO_GROUPING) recordData = groupEntries(recordData);

    let typeCounts = {};

    let pow = [];
    for (let trx_id of Object.keys(recordData)) {
        const {
            timestamp,
            type,
            data
        } = recordData[trx_id];

        if (!typeCounts[type]) typeCounts[type] = 0;
        typeCounts[type]++;

        let fee = null;

        switch (type) {
            case 'claim_reward_balance': {
                /*
                * Don't include these operations as they will cause double entries
               * since the rewards are also registered as author_reward and curation_reward
               * operations
                */

                break;
            }

            case "pow": {
                /*
                * Early mining rewards do not generate virtual ops (except the first one), so we need
                * to hack a workaround. There will most likely be a negative transfer balance
                * from any early mining account, if so assign the difference
                * to the pow events
                * https://github.com/steemit/steem/issues/2173
                */
                if (new Date(timestamp).getTime() / 1000 <= STEEM_HARDFORK_0_1_TIME) {
                    pow.push({timestamp, block: data.block});
                }
                break;
            }

            case "producer_reward": {
                let vests = parseCurrency(data.vesting_shares, timestamp);
                if (vests.amount > 0) {

                    out = addOutputEntry(
                        out, "Mining", vests, null, null,
                        POWER, null, `${user}`, timestamp, type, data.block
                    );

                    /*
                    * All producer rewards received after the end of mining are
                    * in VESTS, but for accounting purposes we can record their
                    * STEEM value at the time of reception and then immediately
                    * after transfer them away so they don't mess up the balances
                    */
                    out = recordSteemEquivalent(
                        out, "Mining", vests, `${user} mining mVESTS STEEM value`,
                        timestamp, type, data.block
                    );

                }
                break;
            }
            case 'transfer': {
                let funds = parseCurrency(data.amount, timestamp);
                if (data.to == user) {
                    // Funds coming in to the account
                    out = addOutputEntry(
                        out, "Deposit", funds, null, null,
                        null, null, `From ${data.from}`, timestamp, type, data.block
                    );

                } else {
                    // Funds leaving the account
                    out = addOutputEntry(
                        out, "Withdrawal", null, funds, null,
                        null, null, `To ${data.to}`, timestamp, type, data.block
                    );
                }
                break;
            }
            case 'fill_order': {
                if (data.open_owner == user) {
                    // Someone filled our limit order
                    let boughtFunds = parseCurrency(data.current_pays, timestamp);
                    let soldFunds = parseCurrency(data.open_pays, timestamp);

                    out = addOutputEntry(
                        out, "Trade", boughtFunds, soldFunds, null,
                        null, null, `Purchased from ${data.current_owner}`, timestamp, type, data.block
                    );
                } else {
                    // We filled someone else's limit order
                    let boughtFunds = parseCurrency(data.open_pays, timestamp);
                    let soldFunds = parseCurrency(data.current_pays, timestamp);

                    out = addOutputEntry(
                        out, "Trade", boughtFunds, soldFunds, null,
                        null, null, `Purchased from ${data.open_owner}`, timestamp, type, data.block
                    );
                }
                break;
            }

            case 'fill_vesting_withdraw': {
                let trxDate = Date.parse(timestamp)/1000;
                let deposited = parseCurrency(data.deposited, timestamp);
                let withdrawn = parseCurrency(data.withdrawn, timestamp);

                /* Filter entries with 0 amounts */
                if (data.deposited.amount == 0 || data.withdrawn.amount == 0) break;


                if ((data.to_account === user || data.to_account === "") && data.from_account === user) {
                    /* From me to me */
                    out = addOutputEntry(
                        out, "Withdrawal", null, withdrawn, null,
                        POWER, null, `fill_vesting_withdraw ${deposited.amount.toFixed(4)} ${deposited.currency}`, timestamp, type, data.block
                    );

                    out = addOutputEntry(
                        out, "Deposit", deposited, null, null,
                        deposited.currency === "mVESTS" ? POWER : null, null, `fill_vesting_withdraw ${withdrawn.amount.toFixed(4)} ${withdrawn.currency}`, timestamp, type, data.block
                    );
                } else if (data.to_account === user) {
                    /* From another to me */
                    out = addOutputEntry(
                        out, "Deposit", deposited, null, null,
                        deposited.currency === "mVESTS" ? POWER : null, null, `fill_vesting_withdraw ${withdrawn.amount.toFixed(4)} ${withdrawn.currency} from ${data.from_account}`, timestamp, type, data.block
                    );

                } else if (data.from_account === user) {
                    /* To another account */
                    out = addOutputEntry(
                        out, "Withdrawal", null, withdrawn, null,
                        POWER, null, `fill_vesting_withdraw ${deposited.amount.toFixed(4)} ${deposited.currency} to ${data.to_account}`, timestamp, type, data.block
                    );
                }
                break;
            }

            case 'transfer_to_vesting': {
                let trxDate = Date.parse(timestamp)/1000;
                let funds = parseCurrency(data.amount, timestamp);
                let mVests = data.amount.amount / steemPerMvests(trxDate, data.block)

                let isBeforeHardFork = moment(timestamp).isBefore(STEEM_HARDFORK_0_1_TIME_MOMENT);
                let vests = parseCurrency({amount: mVests * (isBeforeHardFork ? 1 : 1000000), asset: "VESTS"}, timestamp);

                if (data.from == user && (data.to == user || data.to === "")) {
                    // Converted STEEM to STEEM Power
                    out = addOutputEntry(
                        out, "Withdrawal", null, funds, null,
                        null, null, `To ${vests.amount} mVESTS`, timestamp, type, data.block
                    );

                    out = addOutputEntry(
                        out, "Deposit", vests, null, null,
                        POWER, null, `From ${funds.amount} STEEM`, timestamp, type, data.block
                    );

                } else if (data.from == user) {
                    /* Vest to someone else */
                    out = addOutputEntry(
                        out, "Withdrawal", null, funds, null,
                        null, null, `To ${data.to} as ${vests.amount.toFixed(2)} mVESTS`, timestamp, type, data.block
                    );
                } else {
                    /* Transfer to vesting from someone else to me */
                    out = addOutputEntry(
                        out, "Deposit", vests, null, null,
                        POWER, null, `${funds.amount} ${funds.currency} vested from ${data.from}`, timestamp, type, data.block
                    );
                }
                break;
            }

            case "interest": {
                let interest = parseCurrency(data.interest, timestamp);

                out = addOutputEntry(
                    out, "Income", interest, null, null,
                    null, null, "Inflation interest", timestamp, type, data.block
                );

                break;
            }

            case "comment_benefactor_reward": {
                if (data.benefactor === user) {
                    let reward = parseCurrency(data.reward, timestamp);
                    out = addOutputEntry(
                        out, "Income", reward, null, null,
                        reward.currency === "mVESTS" ? POWER : null, null, "Benefactor reward", timestamp, type, data.block
                    );
                }

                break;
            }

            case "fill_convert_request": {
                let amount_in = parseCurrency(data.amount_in, timestamp);
                let amount_out = parseCurrency(data.amount_out, timestamp);
                if (data.owner === user) {

                    out = addOutputEntry(
                        out, "Deposit", amount_out, null, null,
                        null, null, "Convert SBD to STEEM", timestamp, type, data.block
                    );

                    out = addOutputEntry(
                        out, "Withdrawal", null, amount_in, null,
                        null, null, "Convert SBD to STEEM", timestamp, type, data.block
                    );
                }
                break;
            }

            case "curation_reward": {
                let trxDate = Date.parse(timestamp)/1000;
                let vests = parseCurrency(data.reward, timestamp);

                if (vests.amount > 0) {
                    out = addOutputEntry(
                        out, "Income", vests, null, null,
                        POWER, null, `${user} Curation reward`, timestamp, type, data.block
                    );

                    /*
                    * For accounting purposes we can record the STEEM value of
                    * VESTS curation_reward at the time of reception and then immediately
                    * after transfer them away so they don't mess up the balances
                    */
                    let trxDate = new Date(timestamp).getTime() / 1000;
                    let steemEquivalent = parseCurrency({
                        amount: steemPerMvests(trxDate, data.block)  * vests.amount,
                        asset: "STEEM"
                    });
                    out = addOutputEntry(
                        out, "Income", steemEquivalent, null, null,
                        null, "Steem Equivalents", `${user} curation STEEM value`, timestamp, type, data.block
                    );
                    out = addOutputEntry(
                        out, "Withdrawal", null, steemEquivalent, null,
                        null, "Steem Equivalents", `${user} curation STEEM value`, timestamp, type, data.block
                    );
                }

                break;
            }

            case "account_create":
                /* Account creation takes the fee and converts it to VESTS for the account*/
                let trxDate = Date.parse(timestamp)/1000;
                fee = parseCurrency(data.fee);
                let vestedSteem = data.fee.amount / steemPerMvests(trxDate, data.block);
                let isBeforeHardFork = moment(timestamp).isBefore(STEEM_HARDFORK_0_1_TIME_MOMENT);
                let vests = parseCurrency({amount: vestedSteem * (isBeforeHardFork ? 1 : 1000000), asset: "VESTS"}, timestamp);
                if (data.creator === user) {

                    out = addOutputEntry(
                        out, "Withdrawal", null, fee, null,
                        null, null, `Create account ${data.new_account_name}`, timestamp, type, data.block
                    );
                }
                if (data.new_account_name === user) {
                    out = addOutputEntry(
                        out, "Gift", vests, null, null,
                        POWER, null, `${user} Account creation VESTS`, timestamp, type, data.block
                    );
                }
                break;

            case "account_create_with_delegation": {
                if (data.creator === user) {
                    fee = parseCurrency(data.fee, timestamp);
                    out = addOutputEntry(
                        out, "Withdrawal", null, fee, null,
                        null, null, `account_create_with_delegation`, timestamp, type, data.block
                    );
                }
                break;
            }

            case "author_reward": {
                let trxDate = Date.parse(timestamp)/1000;
                let sbdReward = parseCurrency(data.sbd_payout, timestamp);
                let steemReward = parseCurrency(data.steem_payout, timestamp);
                let vestingReward = parseCurrency(data.vesting_payout, timestamp);
                if (steemReward.amount > 0 ) {

                    out = addOutputEntry(
                        out, "Income", steemReward, null, null,
                        null, null, `${user} author_reward`, timestamp, type, data.block
                    );
                }

                if (vestingReward.amount > 0 ) {

                    out = addOutputEntry(
                        out, "Income", vestingReward, null, null,
                        POWER, null, `${user} author_reward`, timestamp, type, data.block
                    );

                    /*
                    * For accounting purposes we can record the STEEM value of
                    * VESTS author_rewards at the time of reception and then immediately
                    * after transfer them away so they don't mess up the balances
                    */
                    let trxDate = new Date(timestamp).getTime() / 1000;
                    let steemEquivalent = parseCurrency({
                        amount: steemPerMvests(trxDate, data.block)  * vestingReward.amount,
                        asset: "STEEM"
                    });
                    out = addOutputEntry(
                        out, "Income", steemEquivalent, null, null,
                        null, "Steem Equivalents", `${user} author STEEM value`, timestamp, type, data.block
                    );
                    out = addOutputEntry(
                        out, "Withdrawal", null, steemEquivalent, null,
                        null, "Steem Equivalents", `${user} author STEEM value`, timestamp, type, data.block
                    );
                }

                if (sbdReward.amount > 0 ) {
                    out = addOutputEntry(
                        out, "Income", sbdReward, null, null,
                        null, null, `${user} author_reward`, timestamp, type, data.block
                    );
                }
                break;
            }

            default:
                console.log("unhandled type", type, data);
        }
    }

    /* Hack to assign pow rewards */
    let finalBalance = getFinalBalance("STEEM");
    let steemDelta = accountBalances["STEEM"].amount - finalBalance
    if (steemDelta !== 0 && pow.length) {
        console.log(`\n*** Assigning dummy entries for ${steemDelta / STEEMIT_BLOCKCHAIN_PRECISION} STEEM to pow_rewards to balance STEEM holdings ***`);
        pow.slice(1, 0);
        let powReward = parseInt(steemDelta / pow.length, 10);
        let remainder = steemDelta % powReward;
        let trxDate;
        for (var i = 0; i < pow.length; i++) {
            trxDate = new Date(pow[i].timestamp).getTime() / 1000;
            out = addOutputEntry(
                out, "Mining", {amount: powReward, currency: "STEEM"}, null, null,
                null, null, `${user} Early pow reward (dummy entry)`, pow[i].timestamp, "pow_reward"
            );
        }

        if (remainder > 0) {
            out = addOutputEntry(
                out, "Mining", {amount: remainder, currency: "STEEM"}, null, null,
                null, null, `${user} Early pow reward (dummy entry)`, pow[pow.length - 1].timestamp, "pow_reward"
            );
        }
    }

    /* Complete running balance calculation */
    Object.keys(runningBalance).forEach(asset => {
        if (!runningBalance[asset][0]) return;
        runningBalance[asset].sort((a, b) => a[2].getTime() - b[2].getTime());
        runningBalance[asset][0].push(runningBalance[asset][0][1]);

        for (var i = 1; i < runningBalance[asset].length; i++) {
            runningBalance[asset][i].push(runningBalance[asset][i][1] + runningBalance[asset][i-1][3]);
        }
    })

    /* Add or subtract the mVEST delta to the final entry in order to properly balance the books */
    if (runningBalance["mVESTS"]) {
        let finalVestBalance = getFinalBalance("mVESTS");
        let deltaMvests = accountBalances["mVESTS"].amount - finalVestBalance;
        if (parseInt(Math.abs(deltaMvests), 10) > 0) {
            runningBalance["mVESTS"][runningBalance["mVESTS"].length - 1][3] += deltaMvests;
        }
    }
    /* Accounting trick to add STEEM equivalences every 2 weeks as necessary */
    out = addEquivalentSteem(out);
    // Sort out array by Date
    out.sort((a, b) => {
        return new Date(a[10]).getTime() - new Date(b[10]).getTime();
    });

    /* Remove comments */
    // for (var i = 0; i < out.length; i++) {
    //     out[i][9] = "";
    // }

    /* Some checking code here */
    let assetsToCheck = ["SBD", "mVESTS", "STEEM"];
    let assets = Object.keys(assetMovements).sort(), mVestsDelta = 0;
    console.log("");
    assets.forEach(asset => {
        let bal = getFinalBalance(asset) / (asset === "mVESTS" ? 1 : STEEMIT_BLOCKCHAIN_PRECISION)
        let assetName = asset;
        while (assetName.length < 6) {
            assetName += " ";
        }
        if (asset === "mVESTS") mVestsDelta = accountBalances[asset].amount - bal;
        console.log(`${assetName} | Actual balance: ${(accountBalances[asset].amount / (asset === "mVESTS" ? 1 : STEEMIT_BLOCKCHAIN_PRECISION)).toFixed(6)} | Calculated balance: ${bal.toFixed(6)} | delta: ${(accountBalances[asset].amount / (asset === "mVESTS" ? 1 : STEEMIT_BLOCKCHAIN_PRECISION) - bal)}`);
    });
    if (CHECK || (Math.abs(mVestsDelta) > 0.1)) {



        console.log("");
        assetsToCheck.forEach(assetToCheck => {
            console.log(`**** Asset movement by type for ${assetToCheck}: ****\n`)
            getFinalBalance(assetToCheck);
            function getTotal(array) {
                let sum = 0;
                array.forEach(i => {
                    sum += i;
                })
                return sum;
            }

            if (movementTypes[assetToCheck]) {
                Object.keys(movementTypes[assetToCheck]).forEach(type => {
                    let scale = (assetToCheck === "mVESTS" ? 1 : 1000)
                    let deposit = getTotal(movementTypes[assetToCheck][type].deposit);
                    if (deposit > 0) console.log(type, "in :", deposit / scale);
                    let out = getTotal(movementTypes[assetToCheck][type].withdrawal);
                    if (out < 0) console.log(type, "out:", out / scale);
                    if (out < 0 && deposit > 0) console.log(type, "net: ", (deposit + out) / scale, "\n");
                    else console.log("");
                })
            }
        })
        console.log("\nTransaction type counts:\n", typeCounts);
    }

    if (CHECK) {
        assetsToCheck.forEach(assetToCheck => {
            fs.open(`output/${user}-${assetToCheck}-running-balances.csv`, 'w', (err, fd) => {
                if (err) throw err;
                let contents = '';
                runningBalance[assetToCheck].forEach(line => {
                    contents += line.join(',') + "\n";
                });
                fs.write(fd, contents, () => {
                    console.log(`\nWrote running balances to output/${user}-${assetToCheck}-running-balances.csv!\n`);
                });
            });
        });
    }

    // Output the CSV
    fs.open(`output/${user}-steem-transactions.csv`, 'w', (err, fd) => {
        if (err) throw err;
        let contents = '';
        for (let line of out) {
            contents += line.join(',') + "\n";
        }
        fs.write(fd, contents, () => {
            console.log(`\nWrote report to output/${user}-steem-transactions.csv!\n`);
        });
    });
}


async function doWork() {

    // Connect to mongodb first
    await connectMongo();


    /* Fetch data */
    let result = await getBatch();
    console.log(`Parsing ${result.length} documents...`);

    /* Filter and assign data */
    let recordData = {};
    result.map(record => {
        const {
            _id,
            timestamp
        } = record;
        const type = record.type;
        const data = record;
        switch (type) {

            /* Ignore these op types */
            case "custom_json":
            case "account_update":
            case "account_witness_vote":
            case "pow2":
            case "delegate_vesting_shares":
            case "account_witness_proxy":
            case "witness_update":
            case "set_withdraw_vesting_route":
            case "limit_order_cancel":
            case "limit_order_create":
            case "delete_comment":
            case "shutdown_witness":
            case "return_vesting_delegation":
            case "feed_publish":
            case "withdraw_vesting":
            case "transfer_from_savings":
            case "transfer_to_savings":
            case "fill_transfer_from_savings":
            case "convert":
            case "claim_reward_balance":
            break;

            default:
            recordData[_id] = {
                timestamp,
                type,
                data
            };
        }
    });

    /* Disconnect mongodb */
    disconnectMongo();

    /* Parse the data and write the csv reports */
    doReport(recordData);
}
doWork();
