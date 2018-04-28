rm ./all-merged.csv
find . -type f -wholename './output/*transactions.csv' -exec cat {} + >> all-merged.csv
