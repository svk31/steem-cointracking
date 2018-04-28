const data = require("./steem_per_mvests.js");
const fs = require("fs");

let map = {};
data.forEach(d => {
    map[d[0]] = d[1];
})
fs.open(`steem_per_mvests.json`, 'w', (err, fd) => {
    if (err) throw err;
    let contents = JSON.stringify(map);
    fs.write(fd, contents, () => {
        console.log('Done writing steem_per_mvests.json!');
    });
});
