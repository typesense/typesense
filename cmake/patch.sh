#! /bin/sh

set +x
set -euo pipefail


patch="$1"; shift

# ignore the error if the patch is already applied
if ! out=$(patch -p1 -N -r "rejects.bin" < "$patch")
then
    echo "$out" | grep -q "Reversed (or previously applied) patch detected!  Skipping patch."
    test -s "rejects.bin" # Make sure we have rejects.
else
    test -f "rejects.bin" && ! test -s "rejects.bin" # Make sure we have no rejects.
fi

rm -f "rejects.bin"