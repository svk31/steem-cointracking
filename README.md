This is an import script for getting STEEM transactions into the CoinTracking.info site. It also includes a Poloniex history parser that groups orders by order number and reformats deposit/withdrawal history in the standard CoinTracking csv format.

This script started out as a fork of [steem-report](https://github.com/MidnightLightning/steem-report) but has since been refactored almost completely, since the `getAccountHistory` used in that repo appears to be broken.

[CoinTracking](https://cointracking.info) is a portfolio website for tracking cryptocurrency assets. They have lots of exchanges and blockchains already integrated with the site to import automatically, but [STEEM](https://steem.io/) is not one of them. However, they do have a "Bulk CSV Import" option to add in data from different exchanges that don't have a dedicated import option.

This script uses the amazing [SteemData](https://steemdata.com/) service provided by @netherdrake to fetch all transactions for a given user, and converts it to a CSV file that can be imported into CoinTracking.

[![cc-by-sa](https://i.creativecommons.org/l/by-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-sa/4.0/)

# Usage
This script requires Node to run; install Node locally and run:

```
npm install
node app.js myUsername [debug] [no_grouping] [op_type_filter]
```

Replace `myUsername` with the STEEM user you wish to make a report for. Since STEEM data is completely open, there are no login credentials needed to get a full transaction report on any user.

The debug, no_grouping and op_type_filter parameters are optional. Debug=true will print out a summary of operations and balances.
`debug = true|false, default = false`
`no_grouping = true|false, default = false`
`op_type_filter = transfer, fill_order, etc, default=none`

Running the script will create a `{username}-steem-transactions.csv` file in the `output` folder of the project. Head to the [CSV Import](https://cointracking.info/import/import_csv/) screen of CoinTracking (Enter Coins > Bulk Imports > CSV Import) and select that CSV file as the target.

## Automating multiple accounts
If you have several accounts you can rename `run_accounts_example.sh` and input your desired accounts there as shown. Then run it using:
`. ./run_accounts.sh`

This will fetch data for all accounts and save the output in `output`.

To merge all the different CSV files together for one single import operation, use the `merge.sh` script:

`. ./merge.sh`

 This will generate a file called `all-merged.csv` in the root folder. Instead of importing all the different files manually you can import this file directly in Cointracking as explained above.

# Caveats
I've classified block production as Mining, and Author/Curation rewards as Income. You're free to change this however you like, the same goes for any of the many operation types.

I've also added grouping for many operation types, since STEEM generates an enormous amount of transactions. Most groupings are by day, for example author rewards and curation rewards.

 If you see a better way of doing things, please open an issue and let me know.

## Early mining
There are issues with the STEEM history for older accounts that participated in the initial mining phase. The producer rewards for that phase are not recorded in the current history, causing missing balances. To compensate for this, I've assigned the difference between the final balance and the actual balance to these early mining operations.

## VESTS to STEEM conversion
In order to track the actual value of VESTS, I've added bi-weekly deposits of STEEM that correspond to the current balance of VESTS. This conversion is done using a formula to calculate the steem_per_mvests ratio found here: https://steemit.com/steemdev/@holger80/how-to-estimate-historic-steempermvests-values-for-converting-old-rewards-from-vest-to-steem

I noticed that this formula broke down completely for the early mining phase, so I extracted actual values manually from the blockchain and stored them in a json file. These values were then used to interpolate a better value for early steem_per_mvests ratios.

## VESTS inaccuracies
Despite having spent a lot of time debugging the various operations and tracking down many bugs, there are still accounts where the final calculated balances are different from the actual balances. One of my accounts is a good example, `witness.svk`. This account has a large discrepancy in the final balance of MVESTS, and I've so far been unable to track down why. There may be other inaccuracies as well.
