const {connect, disconnect, getData} = require("./db");
const {parseCurrency, steemPerMvests, printAmount} = require("./utils");
const moment = require("moment");

/* Users is a string separated by commas */
const users = process.argv[2].split(",");
const year = process.argv[3];

function parseResult(entry) {
    let date = moment(entry.timestamp);
    let output = {year: date.year()};
    let trxDate = new Date(entry.timestamp).getTime() / 1000;

    switch (entry.type) {
        case "comment":
        case "vote":
            return output;
            break;

        case "author_reward":
            let sbdReward = parseCurrency(entry.sbd_payout, entry.timestamp);
            let steemReward = parseCurrency(entry.steem_payout, entry.timestamp);
            let vestingReward = parseCurrency(entry.vesting_payout, entry.timestamp);

            let steemEquivalent = parseCurrency({
                amount: steemPerMvests(trxDate)  * vestingReward.amount,
                asset: "STEEM"
            });

            let blog_rewards = {
                sbdReward: sbdReward.amount, steemReward: steemReward.amount, vestingReward: vestingReward.amount, steemEquivalent: steemEquivalent.amount
            }
            if (entry.isTopLevel) {
                output.topLevelRewards = blog_rewards;
            } else {
                output.commentRewards = blog_rewards;
            }
            return output;
            break;

        case "curation_reward":
            let curationReward = parseCurrency(entry.reward, entry.timestamp);
            let curationSteemEquivalent = parseCurrency({
                amount: steemPerMvests(trxDate)  * curationReward.amount,
                asset: "STEEM"
            });

            let curation_rewards = {
                vestingReward: curationReward.amount || 0, steemEquivalent: curationSteemEquivalent.amount || 0
            }
            if (entry.isSelfVote) {
                output.selfVoteRewards = curation_rewards;
            } else {
                output.otherVoteRewards = curation_rewards;
            }

            return output;
            break;
    }
}

function filterResults(results) {
    let uniques = [];
    const length = results.length;
    for (var i = results.length - 1; i >= 0; i--) {
        let key = results[i].permlink + results[i].author + results[i].parent_author;
        if (uniques.indexOf(key) === -1) uniques.push(key);
        else {
            results.splice(i, 1);
        }
    }

    console.log("Removed", length - results.length, "entries by filtering");
    return results;

}

const blogRewardTypes = ["topLevelRewards", "commentRewards"];
async function getBlogStats(user, blogStats = {}) {
    /* Fetch data */
    let {result: topLevelPosts} = await getData({account: user, type: "comment", parent_author: "", author: user}, user, false);
    let {result: comments} = await getData({account: user, type: "comment", parent_author: {$ne: ""}, author: user}, user, false);
    let {result: authorRewards} = await getData({account: user, type: "author_reward"}, user, false);

    /* Filter posts by unique permlinks to remove edits */
    topLevelPosts = filterResults(topLevelPosts);
    comments = filterResults(comments);

    /* Parse top-level author stats */
    let topLevelPostIds = [];
    topLevelPosts.forEach(function(p) {
        topLevelPostIds.push(p.permlink);
        let parsed = parseResult(p);
        if (!blogStats[parsed.year]) {
            blogStats[parsed.year] = {topLevelPosts: 0, comments: 0};
        }
        blogStats[parsed.year].topLevelPosts++;
    });

    /* Parse comment author stats */
    comments.forEach(function(p) {
        let parsed = parseResult(p);
        if (!blogStats[parsed.year]) {
            blogStats[parsed.year] = {topLevelPosts: 0, comments: 0};
        }
        blogStats[parsed.year].comments++;
    });

    console.log("Author rewards count:", authorRewards.length);
    authorRewards.forEach(function(p) {
        p.isTopLevel = topLevelPostIds.indexOf(p.permlink) !== -1;
        let parsed = parseResult(p);
        blogRewardTypes.forEach(rType => {
            if (!blogStats[parsed.year][rType]) blogStats[parsed.year][rType] = {
                sbdReward: 0, steemReward: 0, vestingReward: 0, steemEquivalent: 0
            }
            for (let r in parsed[rType]) {
                blogStats[parsed.year][rType][r] += parsed[rType][r];
            }
        })
    });

    return blogStats;
}

const curationRewardTypes = ["selfVoteRewards", "otherVoteRewards"];
async function getCurationStats(user, curationStats) {

    /* Fetch data */
    let {result: selfVotes} = await getData({account: user, type: "vote", voter: user, author: user}, user, false);
    let {result: otherVotes} = await getData({account: user, type: "vote", voter: user, author: {"$ne": user}}, user, false);
    let {result: curationRewards} = await getData({account: user, curator: user, type: "curation_reward"}, user, false);

    /* Parse top-level author stats */
    let selfVoteId = [];
    selfVotes.forEach(function(p) {
        selfVoteId.push(p.permlink);
        let parsed = parseResult(p);
        if (!curationStats[parsed.year]) {
            curationStats[parsed.year] = {selfVotes: 0, otherVotes: 0};
        }
        curationStats[parsed.year].selfVotes++;
    });

    /* Parse comment author stats */
    otherVotes.forEach(function(p) {
        let parsed = parseResult(p);
        if (!curationStats[parsed.year]) {
            curationStats[parsed.year] = {selfVotes: 0, otherVotes: 0};
        }
        curationStats[parsed.year].otherVotes++;
    });

    console.log("Curation rewards count:", curationRewards.length);
    curationRewards.forEach(function(p) {
        p.isSelfVote = selfVoteId.indexOf(p.comment_permlink) !== -1;
        let parsed = parseResult(p);
        curationRewardTypes.forEach(rType => {
            if (!curationStats[parsed.year][rType]) curationStats[parsed.year][rType] = {
                vestingReward: 0, steemEquivalent: 0
            }
            for (let r in parsed[rType]) {
                curationStats[parsed.year][rType][r] += parsed[rType][r];
            }
        })
    });

    return curationStats;
}

function parseRewardNumbers(stats, rewardTypes) {
    for (let year in stats) {
        rewardTypes.forEach(rType => {
            for (let r in stats[year][rType]) {
                let currency = r === "vestingReward" ? "mVESTS" : "";
                let p = printAmount({amount: stats[year][rType][r], currency});
                stats[year][rType][r] = p;
            }
        })
    }
    return stats;
}

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array)
  }
}

async function getStats() {
    // Connect to mongodb first
    await connect();
    let blogStats = {}, curationStats = {};

    await asyncForEach(users, async (user) => {
        console.log("Gettings stats for user", user);
        blogStats = await getBlogStats(user, blogStats);
        curationStats = await getCurationStats(user, curationStats);
    })

    blogStats = parseRewardNumbers(blogStats, blogRewardTypes)
    curationStats = parseRewardNumbers(curationStats, curationRewardTypes)

    if (year) {
        console.log("\n*** blogStats:", year, "***\n", blogStats[year]);
        console.log("\n*** curationStats:", year, "***\n", curationStats[year]);
    } else {
        console.log("\n*** blogStats ***\n:", blogStats);
        console.log("\n*** curationStats ***\n:", curationStats);
    }


    disconnect();
}
getStats();
