const MongoClient = require("mongodb").MongoClient;
const config = require("./config");

const f = require('util').format;
const authMechanism = 'DEFAULT';
// Connection URL
const url = f(`mongodb://${config.mongoUrl}:${config.mongoPort}/SteemData?authMechanism=%s`,
authMechanism);
const {parseCurrency} = require("./utils");
const moment = require("moment");
const {STEEM_HARDFORK_0_1_TIME_MOMENT} = require("./constants");

let client, db;

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
    return new Promise((resolve) => {
        client.close();
        client = null, db = null;
        resolve();
    })
}

function getData(dbFilter, user, DEBUG = true) {
    // Fetch all of the account's operations from SteemData
    return new Promise((resolve, reject) => {
        const collection = db.collection( "AccountOperations" );
        const accountCollection = db.collection("Accounts");
        if (DEBUG) console.time("**** Done fetching data, time taken: ");
        if (DEBUG) console.log(`**** FETCHING DATA FOR ${user}, THIS MAY TAKE SEVERAL MINUTES.... ****`)
        Promise.all([
            collection.find(dbFilter).toArray(),
            accountCollection.find({name: user}).toArray()
        ])
        .then(results => {
            if (DEBUG) console.timeEnd("**** Done fetching data, time taken: ");

            let [result, account] = results;
            let balances = {};
            let info = {};
            balances["STEEM"] = parseCurrency(account[0].balance, account[0].updatedAt);
            balances["STEEM_SAVINGS"] = parseCurrency(account[0].savings_balance, account[0].updatedAt);
            balances["SBD"] = parseCurrency(account[0].sbd_balance, account[0].updatedAt);
            balances["SBD_SAVINGS"] = parseCurrency(account[0].savings_sbd_balance, account[0].updatedAt);
            balances["mVESTS"] = parseCurrency(account[0].vesting_shares, account[0].updatedAt);
            info.mined = account[0].mined;
            info.created = account[0].created;
            if (DEBUG) console.log("\n____ " + user + " ____\n");
            if (DEBUG) console.log("# of entries found:", result.length);
            resolve({result, balances, info});
        }).catch(err => {
            console.log("getData error:", err);
        })
    });
}

function filterEntries(entries, FILTER_TYPE) {
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

module.exports = {
    connect: connectMongo,
    disconnect: disconnectMongo,
    getData,
    filterEntries,
    groupEntries
}
