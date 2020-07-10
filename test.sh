#! /bin/sh

for ws in 1 2 4 8 16 32; do
    timeout 32s python -u test.py $ws 0.03125 &>> $1;
done
