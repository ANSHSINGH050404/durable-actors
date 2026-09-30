import base64
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(os.environ["TERSE_LIFECYCLE_DIRECTORY"])
R = json.loads((ROOT / "resources.json").read_text())
S = json.loads(Path(os.environ["TERSE_LIFECYCLE_SETTINGS"]).read_text())
ENV = {
    **os.environ,
    "PATH": "/opt/homebrew/share/google-cloud-sdk/bin:" + os.environ["PATH"],
    "KUBECONFIG": S["kubeconfig"],
}


def run(args):
    p = subprocess.run(
        args, text=True, capture_output=True, env=ENV, timeout=120, check=False
    )
    if p.returncode:
        raise RuntimeError(p.stderr)
    return p.stdout.strip()


def kubectl(*args):
    return run(["kubectl", *args])


def query(sql):
    return json.loads(
        kubectl(
            "-n",
            R["namespace"],
            "exec",
            "postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        )
    )


def inventory():
    return query(
        "SELECT coalesce(json_agg(json_build_object('name',name,'status',status,'host',host_id,'resourceId',handle::json->>'resourceId','region',handle::json->>'canonicalRegion','createdAt',created_at,'expiresAt',expires_at)), '[]'::json) FROM durable_actors_spares"
    )


def pod_info(pod):
    return {
        "name": pod["metadata"]["name"],
        "uid": pod["metadata"]["uid"],
        "createdAt": pod["metadata"]["creationTimestamp"],
        "runtimeClass": pod["spec"].get("runtimeClassName"),
        "node": pod["spec"].get("nodeName"),
        "phase": pod["status"].get("phase"),
        "conditions": pod["status"].get("conditions", []),
        "containers": [
            {
                "name": s["name"],
                "state": s.get("state"),
                "restartCount": s.get("restartCount"),
            }
            for s in pod["status"].get("containerStatuses", [])
        ],
    }


def pods():
    return [
        pod_info(p)
        for p in json.loads(
            kubectl("-n", R["sandbox_namespace"], "get", "pods", "-o", "json")
        )["items"]
    ]


def ready():
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        names = query(
            "SELECT coalesce(json_agg(name),'[]'::json) FROM durable_actors_spares WHERE status='ready' AND expires_at>clock_timestamp()+interval '30 seconds'"
        )
        available = [
            p
            for p in pods()
            if p["name"] in names
            and p["runtimeClass"] == "gvisor"
            and any(
                c["type"] == "Ready" and c["status"] == "True" for c in p["conditions"]
            )
        ]
        if len(available) >= 2:
            return available
        time.sleep(1)
    raise RuntimeError("prewarmed capacity unavailable")


def actor_path(actor):
    project = S["project_id"]
    key = f"object.v4.{project}:Counter:{actor}"
    enc = lambda x: base64.urlsafe_b64encode(x.encode()).decode().rstrip("=")
    return (
        hashlib.sha256(key.encode()).hexdigest()[:2]
        + "/"
        + enc(project)
        + "/"
        + enc("Counter")
        + "/"
        + enc(actor)
    )


def released(actor):
    key = "durable-actors/v3/owners/" + actor_path(actor) + ".json"
    owner = json.loads(
        run(
            [
                "gcloud",
                "storage",
                "cat",
                f"gs://{R['buckets']['owner']}/{key}",
                f"--project={R['project']}",
            ]
        )
    )
    assert owner["lease"]["expires_at_ms"] == 0 and owner["sealed"], (
        "old actor ownership still active"
    )
    host = owner["lease"]["id"]
    row = next((r for r in inventory() if r["host"] == host), None)
    evidence = next((p for p in pods() if row and p["name"] == row["name"]), None)
    assert evidence is None or evidence["phase"] in ["Succeeded", "Failed"], (
        "old host process still running"
    )
    return {
        "host": host,
        "epoch": owner["epoch"],
        "sealed": True,
        "leaseExpiresAtMs": 0,
        "pod": evidence,
        "snapshot": owner.get("base"),
        "prefix": f"durable-actors/v3/snapshots/{actor_path(actor)}/{owner['epoch']:032x}/",
    }


def archive(prefixes):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        archived = {}
        for prefix in prefixes:
            actor = prefix.split("/snapshots/", 1)[1].rsplit("/", 2)[0]
            owner_key = "durable-actors/v3/owners/" + actor + ".json"
            owner = json.loads(run(["gcloud", "storage", "cat", f"gs://{R['buckets']['owner']}/{owner_key}", f"--project={R['project']}"]))
            snapshot = owner.get("base")
            if snapshot is None:
                archived[prefix] = None
                continue
            logical = snapshot["object"].split("/snapshots/", 1)[1]
            parts = logical.split("/", 4)
            actor_hash = base64.urlsafe_b64encode(hashlib.sha256("/".join(parts[:4]).encode()).digest()).decode().rstrip("=")
            key = "durable-actors-v3-snapshots-" + actor_hash + "~" + parts[4].replace("/", "~")
            result = subprocess.run(["gcloud", "storage", "cat", f"gs://{R['buckets']['archive']}/{key}", f"--project={R['project']}"], capture_output=True)
            if result.returncode or base64.urlsafe_b64encode(hashlib.sha256(result.stdout).digest()).decode().rstrip("=") != snapshot["digest"]:
                break
            archived[prefix] = {"object": key, "digest": snapshot["digest"]}
        if len(archived) == len(prefixes):
            return archived
        time.sleep(1)
    raise RuntimeError("Standard archive did not catch up")


def evidence(host):
    rows = inventory()
    row = next(r for r in rows if r["host"] == host)
    pod = next(p for p in pods() if p["name"] == row["name"])
    log = kubectl("-n", R["sandbox_namespace"], "logs", row["name"])
    (ROOT / "logs").mkdir(exist_ok=True)
    (ROOT / "logs" / f"{row['name']}.jsonl").write_text(log + "\n")
    return {
        "registry": row,
        "pod": pod,
        "startup": [
            json.loads(line)
            for line in log.splitlines()
            if line.startswith("{") and "actor_host_startup" in line
        ],
    }


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "ready":
        result = ready()
    elif action == "inventory":
        result = inventory()
    elif action == "released":
        result = released(sys.argv[2])
    elif action == "archive":
        result = archive(json.loads(Path(sys.argv[2]).read_text()))
    elif action == "evidence":
        result = evidence(sys.argv[2])
    else:
        raise RuntimeError("unknown action")
    print(json.dumps(result))
