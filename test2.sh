#! /bin/sh

for ws in 1 2 4 8 16 32 64; do
    timeout 128s python -u test.py $ws 1 &>> $1;
done
