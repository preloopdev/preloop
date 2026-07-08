#!/usr/bin/env python3
"""Extract and compare timing data from benchmark JSONL results and VM logs."""
import sys, json, re, os, glob
from datetime import datetime
from collections import defaultdict

def parse_vm_log(logfile):
    """Parse a VM log to extract step timings."""
    steps = []
    job_start = None
    job_end = None
    current_step = None
    current_start = None

    if not os.path.exists(logfile):
        return steps, None, None, "Unknown"

    with open(logfile, errors='replace') as f:
        for line in f:
            line = line.strip()
            m = re.match(r'^\[.*?(\d{2}:\d{2}:\d{2}\.\d+)\]', line)
            if not m:
                continue
            ts_str = m.group(1)
            
            if "Running step:" in line:
                if current_step and current_start:
                    steps.append({"name": current_step, "start": current_start, "end": ts_str})
                m2 = re.search(r'Running step: (.+)', line)
                current_step = m2.group(1) if m2 else "unknown"
                current_start = ts_str
            elif "Job " in line and "completed:" in line:
                if current_step and current_start:
                    steps.append({"name": current_step, "start": current_start, "end": ts_str})
                job_end = ts_str
                m2 = re.search(r'completed: (\w+)', line)
                result = m2.group(1) if m2 else "Unknown"

    return steps, job_start, job_end, result


def parse_jsonl_results(results_dir):
    """Parse JSONL result files."""
    records = []
    for f in sorted(glob.glob(os.path.join(results_dir, '*.jsonl'))):
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else 'benchmarks/real-world/results'
    tmp_dir = sys.argv[2] if len(sys.argv) > 2 else None

    print("=" * 80)
    print("  BENCHMARK TIMING REPORT")
    print(f"  {datetime.now().isoformat()}")
    print("=" * 80)

    records = parse_jsonl_results(results_dir)
    if not records:
        print("No results found.")
        return

    by_mode = defaultdict(list)
    for r in records:
        by_mode[r.get('mode', 'unknown')].append(r)

    print(f"\n{'Mode':<25} {'Workflow':<25} {'Total (s)':>10} {'VM Failures':>12}")
    print("-" * 75)
    for r in records:
        total_s = r.get('total_ms', 0) / 1000
        print(f"  {r.get('mode','?'):<23} {r.get('workflow','?'):<23} {total_s:>9.1f}s {r.get('vm_failures',0):>11}")

    # Parse VM logs if tmp_dir provided
    if tmp_dir and os.path.isdir(tmp_dir):
        print(f"\n{'='*80}")
        print("  PER-STEP TIMINGS (from VM logs)")
        print(f"{'='*80}")
        for logfile in sorted(glob.glob(os.path.join(tmp_dir, 'vm-*.log'))):
            basename = os.path.basename(logfile)
            steps, start, end, result = parse_vm_log(logfile)
            if steps:
                print(f"\n  [{basename}]  Result: {result}")
                print(f"  {'Step':<40} {'Start':>15} {'End':>15}")
                print(f"  {'-'*70}")
                for s in steps:
                    print(f"  {s['name']:<40} {s['start']:>15} {s['end']:>15}")


if __name__ == '__main__':
    main()
