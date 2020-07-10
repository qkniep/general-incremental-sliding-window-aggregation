#! /bin/sh

for ws in "0.25" "0.5" 1 2 4 8 16 32 64; do
    for sl in "0.25" "0.5" 1 2; do
        timeout 128s python test.py $ws $sl;
    done
done
