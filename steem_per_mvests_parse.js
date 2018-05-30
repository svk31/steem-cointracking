// const data = require("./steem_per_mvests.js");
const csvRead = require("csv-reader");
const fs = require("fs");

let data = [];
let blocks = [];
const cutoffDate = new Date("2017-05-01T00:00:00Z").getTime() / 1000;
var inputStream = fs.createReadStream('/home/data/Blockchains/parsed_output.csv', 'utf8');
inputStream
	.pipe(csvRead({ parseNumbers: true, parseBooleans: true, trim: true }))
	.on('data', function (row) {
		let date = new Date(row[4]).getTime() / 1000;
        if (blocks.indexOf(row[1]) === -1 && date <= cutoffDate) {

            let total_vesting_fund_steem  = parseFloat(row[6].replace(/ STEEM/, ""))
            let total_vesting_shares  = parseFloat(row[8].replace(/ VESTS/, ""))
            let steem_per_mvests = total_vesting_fund_steem / total_vesting_shares;
            if (steem_per_mvests < 1) {
                steem_per_mvests *= 1000000;
            }

			let min_step = steem_per_mvests / 4000;

            if (steem_per_mvests > 200 && data.length > 1 && (steem_per_mvests - data[data.length - 1][1] <  min_step)) { // minimum increment of 0.05 steem_per_mvests after 300
                return; // console.log("skipping", date, data[data.length - 1][0]);
            }

            data.push([date, steem_per_mvests]);
            blocks.push(row[1]);

            console.log([row[1], row[4], steem_per_mvests]);
        }
	})
	.on('end', function () {
	    console.log('No more rows!');
        parseData(data);
	});
return;

function parseData(data) {
    let map = {};
    data.forEach(d => {
        map[d[0]] = d[1]; // map[timestamp] = steem_per_mvests;
    })
    fs.open(`steem_per_mvests.json`, 'w', (err, fd) => {
        if (err) throw err;
        let contents = JSON.stringify(map);
        fs.write(fd, contents, () => {
            console.log('Done writing steem_per_mvests.json!');
        });
    });
};
