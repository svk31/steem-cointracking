const moment = require("moment");
const {steemPerMvests, mVESTS, parseCurrency, printAmount} = require("./utils");
const {connect, disconnect, getData, filterEntries, groupEntries} = require("./db");
const {STEEMIT_BLOCKCHAIN_PRECISION, STEEM_HARDFORK_0_1_TIME_MOMENT,
    POWER, EXCHANGE, STEEM_HARDFORK_0_1_TIME} = require("./constants");

const fs = require('fs');
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

function asSteemPower(amount, timestamp) {
    let spmv = steemPerMvests(timestamp);

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
            return date.isBefore(d);
        });
        return Math.max(0, bal[Math.max(0, balanceIndex - 1)][3]);
    }

    let previousBalance;
    while (date.isBefore(now)) {

        let spmv = steemPerMvests(date.unix());
        let mvestBalance = findBalance(date, balances);
        let balance = parseCurrency({amount: mvestBalance * spmv, asset: "STEEM"});

        if (balance && balance.amount > 0) {
            if (!previousBalance) {
                output = addOutputEntry(output, "Deposit", balance, null, null, "STEEM-Power",
                "Steem Equivalents Accounting", `${user} estimated mVESTS value`, date.toString(), "dummy");
            } else if (balance.amount !== previousBalance.amount) {
                /* Add withdrawal of previous amount */
                if (previousBalance.amount !== 0) output = addOutputEntry(output, "Withdrawal", null, previousBalance, null, "STEEM-Power",
                "Steem Equivalents Accounting", `${user} estimated mVESTS value`, date.subtract(1, "s").toString(), "dummy");

                /* Add the new equivalent value */
                if (balance.amount !== 0) output = addOutputEntry(output, "Deposit", balance, null, null, "STEEM-Power",
                "Steem Equivalents Accounting", `${user} estimated mVESTS value`, date.add(1, "s").toString(), "dummy");

            }
            previousBalance = balance;
        }

        /* Iterate through the dates of the last day of every month from the start */
        if (date.isSame(startDate)) {
            date.add(1, "month").date(1).subtract(1, "day").hour(23);
        }
        else {
            date.add(2, "month").date(1).subtract(1, "day");
        }
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
        tradeGroup || "", comment || "", new Date(date).toISOString()
    ]);

    return output;
}

function recordSteemEquivalent(out, recordType, vests, comment, timestamp, type, block) {
    if (!RECORD_STEEM_EQUIVALENTS) return out;
    let trxDate = new Date(timestamp).getTime() / 1000;
    let steemEquivalent = parseCurrency({
        amount: steemPerMvests(trxDate)  * vests.amount,
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

    recordData = filterEntries(recordData, FILTER_TYPE);
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
                    * after transfer them away so they don't mess up the balances.
                    * An additional complicating factor is the vesting period,
                    * which means that the STEEM was not immediately available.
                    * At best, the user could receive 1/104th of the reward
                    * starting one week after receiving it. This was changed to 1/13
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
                        POWER, null, `fill_vesting_withdraw ${printAmount(deposited)} ${deposited.currency}`, timestamp, type, data.block
                    );

                    out = addOutputEntry(
                        out, "Deposit", deposited, null, null,
                        deposited.currency === "mVESTS" ? POWER : null, null, `fill_vesting_withdraw ${printAmount(withdrawn)} ${withdrawn.currency}`, timestamp, type, data.block
                    );
                } else if (data.to_account === user) {
                    /* From another to me */
                    out = addOutputEntry(
                        out, "Deposit", deposited, null, null,
                        deposited.currency === "mVESTS" ? POWER : null, null, `fill_vesting_withdraw ${printAmount(withdrawn)} ${withdrawn.currency} from ${data.from_account}`, timestamp, type, data.block
                    );

                } else if (data.from_account === user) {
                    /* To another account */
                    out = addOutputEntry(
                        out, "Withdrawal", null, withdrawn, null,
                        POWER, null, `fill_vesting_withdraw ${printAmount(deposited)} ${deposited.currency} to ${data.to_account}`, timestamp, type, data.block
                    );
                }
                break;
            }

            case 'transfer_to_vesting': {
                let trxDate = Date.parse(timestamp)/1000;
                let funds = parseCurrency(data.amount, timestamp);
                let mVests = data.amount.amount / steemPerMvests(trxDate)

                let isBeforeHardFork = moment(timestamp).isBefore(STEEM_HARDFORK_0_1_TIME_MOMENT);
                let vests = parseCurrency({amount: mVests * (isBeforeHardFork ? 1 : 1000000), asset: "VESTS"}, timestamp);

                if (data.from == user && (data.to == user || data.to === "")) {
                    // Converted STEEM to STEEM Power
                    out = addOutputEntry(
                        out, "Withdrawal", null, funds, null,
                        null, null, `To ${printAmount(vests)} ${vests.currency}`, timestamp, type, data.block
                    );

                    out = addOutputEntry(
                        out, "Deposit", vests, null, null,
                        POWER, null, `From ${printAmount(funds)} ${funds.currency}`, timestamp, type, data.block
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
                        POWER, null, `${printAmount(funds)} ${funds.currency} vested from ${data.from}`, timestamp, type, data.block
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
                        amount: steemPerMvests(trxDate)  * vests.amount,
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
            let vestedSteem = data.fee.amount / steemPerMvests(trxDate);
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
                    out, "Deposit", vests, null, null,
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
                        amount: steemPerMvests(trxDate)  * vestingReward.amount,
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

let db, accountBalances;
async function doWork() {

    // Connect to mongodb first
    await connect();

    /* Fetch data */

    let {result, balances} = await getData({account: user, type: {"$nin": ["vote", "comment", "feed_publish", "comment_options"]}}, user);
    accountBalances = balances;
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
    disconnect();

    /* Parse the data and write the csv reports */
    doReport(recordData);
}
doWork();
