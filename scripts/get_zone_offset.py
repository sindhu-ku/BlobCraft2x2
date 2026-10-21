#!/usr/bin/env python3

import datetime as DT
import sys

from BlobCraft2x2 import local_tz

# when morcs end_time is blank (like for the current run),
# do nothing, instead of crashing
if sys.argv[1] == '':
    sys.exit()

datetime_str = sys.argv[1]

t = DT.datetime.fromisoformat(datetime_str)
# won't work for silly timezones
offset = local_tz.utcoffset(t).seconds // 3600
prefix = '-' if offset < 0 else '+'

print(f'{prefix}{abs(offset):02}:00')
