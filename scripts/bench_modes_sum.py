#!/usr/bin/env python3
"""Aggregate bench_modes.sh output: median (min-max) per label/syms/dist/phase/batch."""
import re, sys, statistics as st, collections
rows = collections.OrderedDict()
for l in sys.stdin:
    m = re.match(r'(\S+) RESULT (.*)', l)
    if not m: 
        if l.strip(): print("#", l.strip())
        continue
    kv = dict(x.split('=', 1) for x in m[2].split() if '=' in x)
    rows.setdefault((m[1], kv.get('syms'), kv.get('dist'), kv.get('phase'), kv.get('batch'), kv.get('ticks')), []).append(kv)
def f(v, k): 
    xs = [float(x[k]) for x in v if k in x]
    return (st.median(xs), min(xs), max(xs)) if xs else (0, 0, 0)
for key, v in rows.items():
    r, c, rss = f(v, 'rate'), f(v, 'cpu_pct'), f(v, 'rss_net_mb')
    print(f"{key[0]:18s} syms={key[1]:>4s} {key[2]:7s} {key[3]:6s} b={key[4]:>4s} T={int(key[5])//1000}k n={len(v)} rate={r[0]:.0f} ({r[1]:.0f}-{r[2]:.0f}) cpu%={c[0]:.0f} rss_mb={rss[0]:.0f} fd={f(v,'fd_max')[0]:.0f} "
          f"tpc={f(v,'avg_ticks_per_commit')[0]:.0f} p50={f(v,'lat_p50_ms')[0]:.1f} p99={f(v,'lat_p99_ms')[0]:.1f} w/tick={f(v,'io_write_bytes_per_tick')[0]:.0f} disk/tick={f(v,'disk_bytes_per_tick')[0]:.1f} "
          f"rtr%={f(v,'router_cpu_pct')[0]:.0f} rss/child={f(v,'rss_per_child_mb')[0]:.1f} thr={f(v,'thr_max')[0]:.0f}")
