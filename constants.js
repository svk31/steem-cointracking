const moment = require("moment");

const STEEM_HARDFORK_0_1_TIME = 1461605400;
const STEEM_HARDFORK_0_1_TIME_MOMENT = moment(STEEM_HARDFORK_0_1_TIME * 1000);
const STEEM_BLOCK_INTERVAL = 3;
const STEEM_BLOCKS_PER_DAY = (24*60*60 / STEEM_BLOCK_INTERVAL);
module.exports = {
    STEEM_BLOCK_INTERVAL,
    STEEM_BLOCKS_PER_DAY,
    STEEM_START_MINER_VOTING_BLOCK: (STEEM_BLOCKS_PER_DAY * 30),
    STEEM_HARDFORK_0_1_TIME, // 2016-04-25 17:30:00.000,
    STEEM_HARDFORK_0_1_TIME_MOMENT,
    STEEMIT_BLOCKCHAIN_PRECISION: 1000,
    VESTS_SHARE_SPLIT: 1000000,
    EXCHANGE: "STEEM Blockchain",
    POWER: "STEEM-Power"
}
