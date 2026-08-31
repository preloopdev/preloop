#!/usr/bin/env python3
"""Pure, deterministic analysis of virtualization campaign JSONL evidence."""
from __future__ import annotations
import argparse, json, math, random, statistics
from pathlib import Path
from typing import Any, Iterable
SCHEMA_VERSION=1
REQUIRED_FIELDS=("schema_version","manifest_sha256","campaign","phase","arm","repetition","monotonic_ns","timestamp_utc","event_kind","success","failure_class","diagnostic","values","units")
KNOWN_UNITS={"ns","us","ms","s","bytes","count","percent","bytes_per_second","unknown"}
WEIGHTS={"throughput":.30,"warm_p95":.20,"cold_p95":.10,"density":.15,"reliability":.15,"operational":.10}
DEPLOYMENT_CLASSES=("trusted_single_tenant","untrusted_multi_tenant")
PRIMARY_METRIC_ALIASES={
 "throughput":("throughput_jobs_per_second","throughput_per_host","throughput"),
 "warm_p95":("warm_p95_ms","warm_latency_p95_ms"),
}
LOWER_IS_BETTER={"warm_p95","cold_p95"}
PRACTICAL_EQUIVALENCE=0.05

def percentile(values:Iterable[float],p:float)->float|None:
 xs=sorted(float(x) for x in values)
 if not xs:return None
 if not 0<=p<=100:raise ValueError("percentile must be between 0 and 100")
 pos=(len(xs)-1)*p/100; lo,hi=math.floor(pos),math.ceil(pos)
 return xs[lo]+(xs[hi]-xs[lo])*(pos-lo)

def mad(values:Iterable[float])->float|None:
 xs=list(values); m=percentile(xs,50)
 return None if m is None else percentile((abs(x-m) for x in xs),50)

def bootstrap_ci(values:Iterable[float],statistic="median",seed=0,samples=2000)->list[float|None]:
 xs=list(map(float,values))
 if not xs:return [None,None]
 rng=random.Random(seed)
 fn=statistics.mean if statistic=="mean" else lambda x: percentile(x,50)
 ys=[float(fn([xs[rng.randrange(len(xs))] for _ in xs])) for _ in range(samples)]
 return [percentile(ys,2.5),percentile(ys,97.5)]

def summarize(values:Iterable[float],seed=0)->dict[str,Any]:
 xs=list(map(float,values))
 return {"n":len(xs),"median":percentile(xs,50),"p90":percentile(xs,90),"p95":percentile(xs,95),"p99":percentile(xs,99),"maximum":max(xs) if xs else None,"mad":mad(xs),"bootstrap_95":bootstrap_ci(xs,seed=seed)}

def paired_ratios(left:dict[Any,float],right:dict[Any,float])->dict[str,Any]:
 keys=sorted(set(left)&set(right),key=str); ratios=[left[k]/right[k] for k in keys if right[k]]; diffs=[left[k]-right[k] for k in keys]
 return {"n":len(ratios),"ratios":ratios,"ratio_median":percentile(ratios,50),"ratio_bootstrap_95":bootstrap_ci(ratios,seed=17),"difference_median":percentile(diffs,50),"keys":[str(k) for k in keys]}

def _truth(x:Any)->bool:return x is True or (isinstance(x,str) and x.lower() in ("true","pass","passed"))
def quality(events:list[dict[str,Any]])->dict[str,Any]:
 errors=[]; previous={}; terminals=set(); manifests=set(); campaigns=set()
 for i,e in enumerate(events):
  missing=[f for f in REQUIRED_FIELDS if f not in e]
  if missing:errors.append(f"event {i}: missing {','.join(missing)}")
  if e.get("manifest_sha256"): manifests.add(e["manifest_sha256"])
  if e.get("campaign"): campaigns.add(e["campaign"])
  if not isinstance(e.get("event_kind"),str) or not e.get("event_kind"):
   errors.append(f"event {i}: empty event_kind")
  if isinstance(e.get("units"),dict):
   for unit in e["units"].values():
    if unit not in KNOWN_UNITS: errors.append(f"event {i}: unknown unit {unit}")
  key=(e.get("arm"),e.get("phase"),e.get("repetition")); mono=e.get("monotonic_ns")
  if isinstance(mono,(int,float)) and key in previous and mono<previous[key]:errors.append(f"event {i}: decreasing monotonic_ns")
  if isinstance(mono,(int,float)):previous[key]=mono
  if e.get("event_kind") in ("terminal","result","job_completed"):
   if key in terminals:errors.append(f"event {i}: duplicate terminal")
   terminals.add(key)
 return {"event_count":len(events),"manifest_count":len(manifests),"campaign_count":len(campaigns),"valid":not errors,"errors":errors}

def _gates(events:list[dict[str,Any]],arm:str)->dict[str,bool]:
 es=[e for e in events if e.get("arm")==arm]
 def gate(kind):
  found=[e for e in es if e.get("event_kind") in (kind,kind+"_gate")]
  return all(_truth(e.get("success")) for e in found) if found else False
 correctness,isolation=gate("correctness"),gate("isolation")
 reliability,capacity=gate("reliability"),gate("capacity")
 posture=[e.get("values",{}).get("isolation_posture") for e in es if isinstance(e.get("values"),dict) and e["values"].get("isolation_posture")]
 multi_tenant=bool(posture) and all(p == "multi_tenant_eligible" for p in posture)
 safety=gate("safety") and not any(e.get("failure_class") in ("host_oom","production_impact","resource_leak","secret_persistence") for e in es)
 return {
  "correctness":correctness,"isolation":isolation,"reliability":reliability,
  "capacity":capacity,"multi_tenant":multi_tenant,"safety":safety,
  "eligible":correctness and isolation and safety and reliability and capacity,
 }

def weighted_score(metrics:dict[str,float],gates:dict[str,Any]|None=None)->float|None:
 if gates is not None and not gates.get("eligible",False):return None
 if any(k not in metrics for k in WEIGHTS):return None
 return sum(WEIGHTS[k]*float(metrics[k]) for k in WEIGHTS)

def _metric_value_from_values(values:Any,metric:str)->float|None:
 if not isinstance(values,dict): return None
 for key in PRIMARY_METRIC_ALIASES.get(metric,(metric,)):
  value=values.get(key)
  if isinstance(value,(int,float)) and math.isfinite(float(value)) and float(value)>0:
   return float(value)
 return None

def _metric_value(event:dict[str,Any],metric:str)->float|None:
 return _metric_value_from_values(event.get("values"),metric)

def _paired_primary(events:list[dict[str,Any]],metric:str)->dict[str,Any]:
 """Return the preregistered Hypeman/SmolVM ratio evidence for one metric."""
 arms={}
 for event in events:
  if event.get("arm") not in ("smolvm","hypeman-fc") or event.get("warmup"):
   continue
  value=_metric_value(event,metric)
  if value is None or not _truth(event.get("success")): continue
  key=(event.get("repetition"),event.get("workload"),
       event.get("resource_profile"),event.get("cache_class"),event.get("phase"))
  arms.setdefault(event["arm"],{}).setdefault(key,[]).append(value)
 paired={
  arm:{key:percentile(values,50) for key,values in rows.items()}
  for arm,rows in arms.items()
 }
 evidence=paired_ratios(paired.get("hypeman-fc",{}),paired.get("smolvm",{}))
 evidence["metric"]=metric
 evidence["practical_equivalence"]=PRACTICAL_EQUIVALENCE
 return evidence

def _outside_equivalence(ci:list[float|None])->bool:
 return ci[0] is not None and ci[1] is not None and (
  ci[0]>1+PRACTICAL_EQUIVALENCE or ci[1]<1-PRACTICAL_EQUIVALENCE
 )

def _comparison_winner(evidence:dict[str,Any])->str|None:
 ci=evidence.get("ratio_bootstrap_95",[None,None])
 if not _outside_equivalence(ci): return None
 median=evidence.get("ratio_median")
 if not isinstance(median,(int,float)): return None
 if evidence["metric"] in LOWER_IS_BETTER:
  return "hypeman-fc" if median<1-PRACTICAL_EQUIVALENCE else "smolvm"
 return "hypeman-fc" if median>1+PRACTICAL_EQUIVALENCE else "smolvm"

def _decision(scores, gates, primary):
 gate_eligible=[k for k,g in gates.items() if g.get("eligible")]
 if not gate_eligible: return "no eligible arm"
 eligible=[(v,k) for k,v in scores.items() if v is not None and gates[k].get("eligible")]
 if not eligible: return "no material performance winner"
 if len(eligible)==1: return eligible[0][1]
 eligible.sort(reverse=True)
 advantage=(eligible[0][0]/eligible[1][0])-1 if eligible[1][0] else 0
 winners=[_comparison_winner(primary[name]) for name in ("throughput","warm_p95")]
 if advantage>=0.05 and winners[0] == winners[1] == eligible[0][1]:
  return eligible[0][1]
 return "no material performance winner"
def render_report(summary):
 lines=["# Virtualization benchmark analysis","",f"Campaign: `{summary.get('campaign')}`",
        f"Data quality: {'PASS' if summary['quality']['valid'] else 'FAIL'}",
        f"Events: {summary['quality']['event_count']}",
        "","## Data quality"]
 lines += [f"- manifests: {summary['quality']['manifest_count']}",
           f"- campaigns: {summary['quality']['campaign_count']}"]
 if summary["quality"]["errors"]:
  lines += [f"- error: `{error}`" for error in summary["quality"]["errors"]]
 lines += ["","## Populations"]
 for arm, populations in summary["populations"].items():
  for cache, values in populations.items():
   lines.append(f"- `{arm}` / `{cache}`: n={values['n']} "
                f"median={values['median']} p95={values['p95']} p99={values['p99']}")
 lines += ["","## Eligibility"]
 lines += [f"- `{a}`: {'eligible' if g['eligible'] else 'ineligible'} "
           f"(correctness={g['correctness']}, isolation={g['isolation']}, "
           f"reliability={g['reliability']}, capacity={g['capacity']}, "
           f"multi_tenant={g['multi_tenant']}, safety={g['safety']})"
           for a,g in summary['gates'].items()]
 lines += ["","## Phase metrics"]
 for arm, phases in summary.get("phase_metrics", {}).items():
  for phase, values in phases.items():
   lines.append(f"- `{arm}` / `{phase}`: n={values['n']} "
                f"median={values['median']} p95={values['p95']}")
 lines += ["","## Unnormalized metrics"]
 for arm, values in summary["metrics"].items():
  lines.append(f"- `{arm}`: " + ", ".join(f"{name}={value}" for name,value in sorted(values.items())))
 lines += ["","## Scores"]
 for deployment, scores in summary["scores"].items():
  lines += [f"- `{deployment}` / `{arm}`: {score}" for arm, score in scores.items()]
 lines += ["","## Primary paired comparisons"]
 for metric, evidence in summary["primary_comparisons"].items():
  lines.append(f"- `{metric}`: n={evidence['n']} ratio_median={evidence['ratio_median']} "
               f"bootstrap_95={evidence['ratio_bootstrap_95']} "
               f"practical_equivalence=±{evidence['practical_equivalence']:.0%}")
 lines += ["","## Decisions"]+[f"- {c}: `{d}`" for c,d in summary["decisions"].items()]
 lines += ["","## Limitations",
           "- Metrics absent from raw events are not imputed or scored.",
           "- No decision is valid until correctness, isolation, safety, reliability, and capacity gates have evidence."]
 return "\n".join(lines)+"\n"
def analyze_events(events:list[dict[str,Any]],manifest_sha256:str|None=None):
 q=quality(events); arms=sorted({str(e.get("arm")) for e in events}); populations={}
 for arm in arms:
  populations[arm]={}
  for cache in ("image-cold","snapshot-warm","runner-warm","dependency-cold","dependency-warm"):
   vals=[e["values"]["duration_ms"] for e in events if e.get("arm")==arm and e.get("cache_class")==cache and isinstance(e.get("values"),dict) and isinstance(e["values"].get("duration_ms"),(int,float)) and _truth(e.get("success"))]
   populations[arm][cache]=summarize(vals)
 gates={a:_gates(events,a) for a in arms}
 phase_metrics={}
 for arm in arms:
  phase_metrics[arm]={}
  phases=sorted({str(e.get("phase")) for e in events if e.get("arm")==arm})
  for phase in phases:
   durations=[e["values"]["duration_ms"] for e in events
              if e.get("arm")==arm and e.get("phase")==phase and _truth(e.get("success"))
              and isinstance(e.get("values"),dict)
              and isinstance(e["values"].get("duration_ms"),(int,float))]
   phase_metrics[arm][phase]=summarize(durations)
 metrics={}
 for arm in arms:
  rows=[e.get("values",{}) for e in events if e.get("arm")==arm and isinstance(e.get("values"),dict)]
  metrics[arm]={k:sum(float(value) for value in (_metric_value_from_values(r,k) for r in rows) if value is not None) /
      len([value for value in (_metric_value_from_values(r,k) for r in rows) if value is not None])
      for k in WEIGHTS if any(_metric_value_from_values(r,k) is not None for r in rows)}
 scores={}
 for deployment in DEPLOYMENT_CLASSES:
  eligible_gates={
   a:dict(gates[a],eligible=(gates[a]["eligible"] and
      q["valid"] and
      (deployment == "trusted_single_tenant" or gates[a]["multi_tenant"])))
   for a in arms
  }
  eligible_metrics=[metrics[a] for a in arms if eligible_gates[a]["eligible"]]
  normalized={}
  for arm in arms:
   normalized[arm]={}
   for metric in WEIGHTS:
    values=[row.get(metric) for row in eligible_metrics if isinstance(row.get(metric),(int,float))]
    value=metrics[arm].get(metric)
    if not isinstance(value,(int,float)) or not values: continue
    best=(min(values) if metric in LOWER_IS_BETTER else max(values))
    normalized[arm][metric]=(best/value if metric in LOWER_IS_BETTER else value/best) if value else 0
  scores[deployment]={
   arm:weighted_score(normalized[arm],dict(eligible_gates[arm],eligible=eligible_gates[arm]["eligible"]))
   for arm in arms
  }
 primary={metric:_paired_primary(events,metric) for metric in PRIMARY_METRIC_ALIASES}
 decisions={}
 for deployment in DEPLOYMENT_CLASSES:
  deployment_gates={
   a:dict(gates[a],eligible=(gates[a]["eligible"] and
      q["valid"] and
      (deployment == "trusted_single_tenant" or gates[a]["multi_tenant"])))
   for a in arms
  }
  decisions[deployment]=_decision(scores[deployment],deployment_gates,primary)
 summary={"schema_version":SCHEMA_VERSION,"manifest_sha256":manifest_sha256 or next(iter({e.get('manifest_sha256') for e in events if e.get('manifest_sha256')}),None),"campaign":events[0].get("campaign") if events else None,"quality":q,"populations":populations,"phase_metrics":phase_metrics,"gates":gates,"metrics":metrics,"scores":scores,"primary_comparisons":primary,"decisions":decisions}
 return summary,render_report(summary)
def load_events(campaign:Path):
 events=[]
 for path in sorted(campaign.glob("*.jsonl")):
  with path.open(encoding="utf-8") as f:
   events.extend(json.loads(line) for line in f if line.strip())
 return events
def main(argv=None):
 p=argparse.ArgumentParser()
 p.add_argument("--campaign",required=True,type=Path)
 p.add_argument("--output",type=Path)
 p.add_argument("--pilot",action="store_true")
 p.add_argument("--decision",action="store_true")
 a=p.parse_args(argv)
 summary,report=analyze_events(load_events(a.campaign)); out=a.output or a.campaign;out.mkdir(parents=True,exist_ok=True)
 summary["pilot"] = bool(a.pilot)
 (out/"summary.json").write_text(json.dumps(summary,sort_keys=True,indent=2,separators=(",",":"))+"\n",encoding="utf-8")
 (out/"report.md").write_text(report,encoding="utf-8")
 if a.decision:
  names={"smolvm":"SMOLVM","hypeman-fc":"HYPEMAN_FIRECRACKER","no eligible arm":"NO_ELIGIBLE_CANDIDATE","no material performance winner":"NO_MATERIAL_WINNER"}
  for deployment,decision in summary["decisions"].items():
   print(f"{deployment}: {names.get(decision, 'NO_MATERIAL_WINNER')}")
 return 0
if __name__=="__main__":raise SystemExit(main())
