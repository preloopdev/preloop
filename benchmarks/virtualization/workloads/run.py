#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from common import SUPPORTED, run
p=argparse.ArgumentParser(); p.add_argument("kind",choices=SUPPORTED); p.add_argument("--iterations",type=int,default=1000)
a=p.parse_args(); print(json.dumps(run(a.kind,a.iterations),sort_keys=True))
