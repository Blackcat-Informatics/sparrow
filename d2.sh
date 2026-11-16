#!/bin/bash
set -e
perl -0pi -e 's/- expiry deletes an artifact entry \(with the last of its records, or, for a\n  recordless artifact, with its time-index key\);/- expiry deletes an artifact entry (an artifact with records, in the batch that\n  deletes its first due record; a recordless artifact, with its time-index key);/' interfaces.md
