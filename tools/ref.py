#!/usr/bin/env python3
"""Slow, straightforward reference of rules.md, for cross-checking. Writes CSVs like dev-answer.
usage: ref.py DATA_DIR OUT_DIR"""
import os, sys, json, glob, datetime
from collections import defaultdict

DATA, OUT = sys.argv[1], sys.argv[2]
DAY = 86400000

def lines(path):
    with open(path, 'rb') as f:
        data = f.read()
    parts = data.split(b'\n')
    if parts and parts[-1] == b'':
        parts.pop()
    return parts[1:]

def fields(line, n):
    fs = line.split(b',')
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and f.count(b'"') == 2:
                f = f[1:-1]
            else:
                return None
        out.append(f if f != b'' else None)
    return out

DIG = set(b'0123456789')
def alldig(b):
    return len(b) > 0 and all(c in DIG for c in b)

def civil(s):
    # s: bytes YYYY-MM-DDTHH:MM:SS
    if not (s[4:5] == b'-' and s[7:8] == b'-' and s[10:11] == b'T' and s[13:14] == b':' and s[16:17] == b':'):
        return None
    for a, b in ((0, 4), (5, 7), (8, 10), (11, 13), (14, 16), (17, 19)):
        if not alldig(s[a:b]):
            return None
    y, mo, d, h, mi, se = int(s[0:4]), int(s[5:7]), int(s[8:10]), int(s[11:13]), int(s[14:16]), int(s[17:19])
    if h > 23 or mi > 59 or se > 59:
        return None
    try:
        dt = datetime.datetime(y, mo, d, h, mi, se, tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    return (dt - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)) // datetime.timedelta(milliseconds=1)

def ts(b):
    if b is None:
        return None
    if len(b) == 20 and b[19:20] == b'Z':
        return civil(b[:19])
    if len(b) == 25 and b[19:20] in (b'+', b'-') and b[22:23] == b':':
        t = civil(b[:19])
        if t is None or not alldig(b[20:22]) or not alldig(b[23:25]):
            return None
        oh, om = int(b[20:22]), int(b[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000
        return t - off if b[19:20] == b'+' else t + off
    if alldig(b):
        return int(b)
    return None

man = json.load(open(os.path.join(DATA, 'manifest.json')))
WS = ts(man['window_start'].encode()); WE = ts(man['window_end'].encode())

products = {}
for l in lines(os.path.join(DATA, 'products.csv')):
    f = fields(l, 2)
    if f: products[f[0]] = f[1]
campaigns = {}
for l in lines(os.path.join(DATA, 'campaigns.csv')):
    f = fields(l, 2)
    if f: campaigns[f[0]] = f[1]

# links
parent = {}
def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x
for p in glob.glob(os.path.join(DATA, 'links', '*.csv')):
    for l in lines(p):
        f = fields(l, 2)
        if not f or f[0] is None or f[1] is None:
            continue
        for x in f:
            parent.setdefault(x, x)
        a, b = find(f[0]), find(f[1])
        if a != b:
            parent[a] = b
comp_min = {}
for x in list(parent):
    r = find(x)
    if r not in comp_min or x < comp_min[r]:
        comp_min[r] = x
def person(u):
    if u in parent:
        return comp_min[find(u)]
    return u

# events
best = {}
for p in glob.glob(os.path.join(DATA, 'events', '*.csv')):
    for l in lines(p):
        f = fields(l, 7)
        if not f or f[0] is None:
            continue
        ing = ts(f[3])
        cur = best.get(f[0])
        if cur is None or ing < cur[0]:
            best[f[0]] = (ing, f)
per = defaultdict(list)
for eid, (ing, f) in best.items():
    _, user, t, _, et, prod, price = f
    if user is None or prod is None:
        continue
    t = ts(t)
    if t is None or not (WS <= t < WE):
        continue
    if et not in (b'view', b'cart', b'purchase'):
        continue
    pr = 0
    if et == b'purchase':
        if price is None or not alldig(price):
            continue
        pr = int(price)
        if not (1 <= pr <= 10_000_000):
            continue
    if ing - t > DAY:
        continue
    cat = products.get(prod, b'unknown') if pr else None
    per[person(user)].append((t, pr, cat))

clicks = defaultdict(list)
for p in glob.glob(os.path.join(DATA, 'clicks', '*.csv')):
    for l in lines(p):
        f = fields(l, 4)
        if not f or f[0] is None or f[1] is None:
            continue
        t = ts(f[3])
        if t is None or not (WS - 7 * DAY <= t < WE):
            continue
        if f[2] not in campaigns:
            continue
        clicks[person(f[1])].append((t, f[0], f[2]))

ud = {}
cd = defaultdict(lambda: [0, 0, 0, 0])
for pr_, evs in per.items():
    evs.sort(key=lambda e: e[0])
    sessions = []
    for e in evs:
        if sessions and e[0] - sessions[-1][-1][0] <= 1800000:
            sessions[-1].append(e)
        else:
            sessions.append([e])
    cl = clicks.get(pr_, [])
    for s in sessions:
        st = s[0][0]
        day = st // DAY
        rev = sum(e[1] for e in s)
        purch = any(e[1] for e in s)
        k = ud.setdefault((pr_, day), [0, 0, 0, defaultdict(int)])
        k[0] += 1; k[1] += len(s); k[2] += rev
        for e in s:
            if e[1]:
                k[3][e[2]] += e[1]
        q = [c for c in cl if c[0] <= st and st - c[0] <= 7 * DAY]
        if q:
            q.sort(key=lambda c: (-c[0], c[1]))
            camp = q[0][2]
        else:
            camp = b'organic'
        r = cd[(camp, day)]
        r[0] += 1; r[1] += 1 if purch else 0; r[2] += rev
        for c in set(c[2] for c in q):
            cd[(c, day)][3] += 1

os.makedirs(OUT, exist_ok=True)
def dstr(d):
    return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()
with open(os.path.join(OUT, 'user_daily.csv'), 'w', encoding='utf-8', newline='') as f:
    f.write('user_id,day,sessions,events,revenue_cents,top_category\n')
    for (u, d), (s, e, r, cats) in ud.items():
        top = ''
        if cats:
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
        f.write(f'{u.decode()},{dstr(d)},{s},{e},{r},{top}\n')
with open(os.path.join(OUT, 'campaign_daily.csv'), 'w', encoding='utf-8', newline='') as f:
    f.write('campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n')
    for (c, d), (s, ps, r, a) in cd.items():
        ch = b'organic' if c == b'organic' else campaigns[c]
        f.write(f'{c.decode()},{ch.decode()},{dstr(d)},{s},{ps},{r},{a}\n')
