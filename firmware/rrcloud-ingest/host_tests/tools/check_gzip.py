#!/usr/bin/env python3
"""Decode the gzip stream written by test_proto (stored-deflate writer) with the stdlib."""
import gzip, sys
data = gzip.decompress(open(sys.argv[1], "rb").read())
exp = b'{"written_server_ts":1,"cursors":{},"proto":1}\n' + bytes((ord('a') + (i % 26)) for i in range(69999)) + b"\n"
assert data == exp, (len(data), len(exp))
print("gzip ok", len(data))
