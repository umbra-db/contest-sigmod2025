#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <binary>"
    exit 1
fi

BINARY=$1

echo "Extracting global constructors from: $BINARY"

# Get the addresses from .init_array (two addresses per line)
# Get the addresses from .init_array
ADDRESSES=$(objdump -s -j .init_array "$BINARY" | awk 'NR>3 {print $2 $3; print $4 $5}' | sed 's/\(..\)\(..\)\(..\)\(..\)\(..\)\(..\)\(..\)\(..\)/0x\8\7\6\5\4\3\2\1/')
if [ -z "$ADDRESSES" ]; then
    echo "No global constructors found."
    exit 1
fi

echo $ADDRESSES

echo "Found constructor addresses:"
for ADDR in $ADDRESSES; do
    SYMBOL=$(objdump -S --start-address=$ADDR "$BINARY" 2>/dev/null | head -n 10 | grep -Eo "<[^>]+>" | head -n 1)
    echo "$ADDR -> ${SYMBOL:-<unknown>}"
done
