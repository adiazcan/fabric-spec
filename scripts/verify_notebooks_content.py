#!/usr/bin/env python3
import base64
import json
import subprocess
import time
import urllib.request

ws = "c516831e-6a69-4de6-b503-7e492fbe9ecd"
items = {
    "00_watermarks": "81809d6c-3323-4558-852d-dad0f7d17912",
    "helpers": "d30580b4-8246-449e-ac46-9da11bfe3c13",
}

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

for name, item_id in items.items():
    req = urllib.request.Request(
        url=f"https://api.powerbi.com/v1/workspaces/{ws}/items/{item_id}/getDefinition",
        data=b"{}",
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        op = r.headers["Location"]

    # Poll long-running operation until completion.
    for _ in range(12):
        with urllib.request.urlopen(
            urllib.request.Request(op, headers={"Authorization": f"Bearer {tok}"})
        ) as status_resp:
            status = json.loads(status_resp.read().decode()).get("status", "")
        if status.lower() == "succeeded":
            break
        time.sleep(5)

    with urllib.request.urlopen(
        urllib.request.Request(op + "/result", headers={"Authorization": f"Bearer {tok}"})
    ) as rr:
        res = json.loads(rr.read().decode())

    print(f"--- {name}")
    for part in res["definition"]["parts"]:
        decoded = base64.b64decode(part["payload"])
        preview = decoded.decode("utf-8", errors="replace").splitlines()
        first_line = preview[0] if preview else "EMPTY"
        print(f"{part['path']}: bytes={len(decoded)} first_line={first_line}")
