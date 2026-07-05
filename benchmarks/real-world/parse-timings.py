#!/usr/bin/env python3
"""Parse aksh-runner log and extract step timings."""
import sys, re, json
from datetime import datetime

def parse_ts(line):
    m = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z', line)
    if m:
        ts = m.group(1)[:26]  # truncate to microseconds
        return datetime.fromisoformat(ts)
    return None

def main(logfile):
    steps = []
    job_start = None
    job_end = None
    current_step = None
    current_start = None

    with open(logfile) as f:
        for line in f:
            line = line.strip()
            ts = parse_ts(line)
            if not ts:
                continue

            if "Starting job:" in line:
                job_start = ts
            elif "Running step:" in line:
                if current_step and current_start:
                    steps.append({"name": current_step, "start": current_start, "end": ts,
                                  "duration_ms": int((ts - current_start).total_seconds() * 1000)})
                m = re.search(r'Running step: (.+)', line)
                current_step = m.group(1) if m else "unknown"
                current_start = ts
            elif "Job " in line and "completed:" in line:
                if current_step and current_start:
                    steps.append({"name": current_step, "start": current_start, "end": ts,
                                  "duration_ms": int((ts - current_start).total_seconds() * 1000)})
                job_end = ts
                m = re.search(r'completed: (\w+)', line)
                result = m.group(1) if m else "Unknown"

    if not steps:
        print("No steps found in log")
        return

    total_ms = int((job_end - job_start).total_seconds() * 1000) if job_start and job_end else 0

    print(f"\n{'Step':<35} {'Duration':>10}")
    print("-" * 47)
    for s in steps:
        dur = f"{s['duration_ms']}ms"
        if s['duration_ms'] >= 1000:
            dur = f"{s['duration_ms']/1000:.1f}s"
        print(f"  {s['name']:<33} {dur:>10}")
    print("-" * 47)
    total_s = total_ms / 1000
    print(f"  {'JOB TOTAL':<33} {total_s:.1f}s")
    print(f"  Result: {result if 'result' in dir() else 'Unknown'}")
    print()

    # JSON output
    output = {
        "steps": [{
            "name": s["name"],
            "duration_ms": s["duration_ms"]
        } for s in steps],
        "total_ms": total_ms,
        "result": result if 'result' in dir() else "Unknown"
    }
    print(json.dumps(output))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <logfile>")
        sys.exit(1)
    main(sys.argv[1])
