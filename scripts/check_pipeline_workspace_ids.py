#!/usr/bin/env python3
import base64
import json
import subprocess
import time
import urllib.request

ws = "c516831e-6a69-4de6-b503-7e492fbe9ecd"
item = "d3bc08d8-2544-473c-91ab-3cee209ff080"

tok = subprocess.check_output(
    [
        "az",
        "account",
        "get-access-token",
        "--resource",
        "https://analysis.windows.net/powerbi/api",
        "--query",
        "accessToken",
        "-o",
        "tsv",
    ],
    text=True,
).strip()

req = urllib.request.Request(
    url=f"https://api.powerbi.com/v1/workspaces/{ws}/items/{item}/getDefinition",
    data=b"{}",
    headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req) as r:
    op = r.headers["Location"]

for _ in range(20):
    with urllib.request.urlopen(urllib.request.Request(op, headers={"Authorization": f"Bearer {tok}"})) as sr:
        st = json.loads(sr.read().decode()).get("status", "")
    if st.lower() == "succeeded":
        break
    time.sleep(3)

with urllib.request.urlopen(
    urllib.request.Request(op + "/result", headers={"Authorization": f"Bearer {tok}"})
) as rr:
    res = json.loads(rr.read().decode())

part = next(p for p in res["definition"]["parts"] if p["path"] == "pipeline-content.json")
content = base64.b64decode(part["payload"]).decode("utf-8", errors="replace")
obj = json.loads(content)
vals = [a.get("typeProperties", {}).get("workspaceId") for a in obj.get("properties", {}).get("activities", [])]
print(vals)
print(sorted({str(v) for v in vals}))
