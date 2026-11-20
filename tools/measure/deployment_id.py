"""Look up a deployment of the measured app by the components of its composition, for the fault experiments.

The deployment ids (d-...) change whenever a layout is redeployed (e.g. after a baseline run of the nightly
campaign), so the fault commands look them up from the configurator API instead of hard-coding them.

Usage:
  python tools/measure/deployment_id.py --components position-service,ru-fusion-service            -> d-...
  python tools/measure/deployment_id.py --components position-service,ru-fusion-service --field node -> knative-m02
The composition must consist of exactly the given components (in any order). Only the deployments of the active
layout are printed (one line each): warm backups and standby deployments of the same composition on other nodes
are not the target of a fault.
"""
import argparse
import json
import subprocess
import sys

APP = "4dr6u38ufksqzmew9f3twvriw"
API = "/api/v1/namespaces/configurator/services/lsf-configurator:80/proxy/function_apps/"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--components", required=True, help="comma-separated component names of the composition")
    p.add_argument("--field", default="id", choices=["id", "node"], help="what to print of each deployment")
    p.add_argument("--app", default=APP)
    args = p.parse_args()

    raw = subprocess.run(["kubectl", "get", "--raw", API + args.app], capture_output=True, text=True, check=True)
    app = json.loads(raw.stdout)
    wanted = sorted(c.strip() for c in args.components.split(","))
    active = (app.get("layout_candidates") or {}).get(app.get("active_layout_key"), {})
    active_nodes = {node for node, ci in active.items()
                    if sorted(p["name"] for p in ci.get("ComponentProfiles") or ci.get("component_profiles") or []) == wanted}
    found = [d[args.field] for fc in app.get("compositions") or []
             if sorted(fc.get("components") or []) == wanted
             for d in fc.get("deployments") or [] if d.get("node") in active_nodes]
    if not found:
        sys.exit(f"no deployment of the active layout {app.get('active_layout_key')} with exactly the components {wanted}")
    print("\n".join(found))


if __name__ == "__main__":
    main()