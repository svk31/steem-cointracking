const steem_per_mvests = require("./steem_per_mvests.json");
const steem_per_mvests_timestamps = Object.keys(steem_per_mvests).map(a => parseInt(a, 10)).sort((a, b) => a - b);

const STEEM_PER_MVESTS_MAX_TIME = steem_per_mvests_timestamps.reduce((a, b) => {
    return (b > a ? b : a);
}, 0);
const a = 2.1325476281078992e-05;
const b = -31099.685481490847;

const a2 = 2.9019227739473682e-07;
const b2 = 48.41432402074669;

// https://steemit.com/steemdev/@holger80/how-to-estimate-historic-steempermvests-values-for-converting-old-rewards-from-vest-to-steem
function steemPerMvests(timestamp, debug = false) {

    /*
    * The above formula is highly inaccurate for the early period of Steem,
    * so we use stored values of steem_per_mvests from the blockchain instead
    * (values extracted manually by svk31)
    */
    let actual;
    if (timestamp <= Math.min(STEEM_PER_MVESTS_MAX_TIME, (b2-b)/(a-a2))) {
        if (timestamp <= 1459444203) actual = 1;
        else {
            let index = steem_per_mvests_timestamps.findIndex(a => {
                return a >= timestamp;
            });

            let x0 = steem_per_mvests_timestamps[index - 1];
            let y0 = steem_per_mvests[x0];
            let x1 = steem_per_mvests_timestamps[index];
            let y1 = steem_per_mvests[x1];
            actual = y0 + (timestamp - x0)*(y1 - y0)/(x1 - x0);
        }
    }

    /* Formula debugging */
    if (debug) {
        if (timestamp < (b2-b)/(a-a2)) {
            if (actual) console.log(new Date(timestamp * 1000), "1 mVestsDelta:", actual -( a * timestamp + b), "actual:", actual);
        } else if (actual) {
            console.log(new Date(timestamp * 1000), "2 mVestsDelta", actual - (a2 * timestamp + b2), "actual:", actual);
        } else {
            console.log(new Date(timestamp * 1000), "no actual mVests calc");
        }
    }

    if (actual) return actual;
    if (timestamp < (b2-b)/(a-a2)) {
        return a * timestamp + b;
    } else {
        return a2 * timestamp + b2;
    }
}

module.exports = {
    steemPerMvests
};
