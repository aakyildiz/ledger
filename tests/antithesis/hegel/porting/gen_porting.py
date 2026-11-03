#!/usr/bin/env python3
# usage: python3 porting/gen_porting.py tests/antithesis/hegel [appendix.md] > PORTING.md
"""Builds HG/PORTING.md: a summary table plus every HG/porting/*.md (task 99)."""
import glob, os, re, sys

HG = sys.argv[1]
W = os.path.join(HG, "..", "workload")
porting = sorted(glob.glob(os.path.join(HG, "porting", "*.md")))
texts = {os.path.basename(p)[:-3]: open(p, encoding="utf-8").read() for p in porting}
appendix = open(sys.argv[2], encoding="utf-8").read() if len(sys.argv) > 2 else ""

K8S = {"singleton_driver_quorum_recovery", "singleton_driver_rolling_restart", "singleton_driver_scaling",
       "singleton_driver_scaling_chaos", "singleton_driver_scaling_structured", "singleton_driver_config_change"}
SPECIAL = {
    "first_default_ledger": "00c `setup.rs` (creates `default` at setup, untracked)",
}

def homes(pattern):
    return [n for n, t in texts.items() if re.search(pattern, t)]

drivers = sorted(os.listdir(os.path.join(W, "bin", "cmds", "main")))
out = []
out.append("# Porting record of the hegel workload\n")
out.append("This file is generated from `porting/*.md` by `porting/gen_porting.py` (one file per task; edit those, not this "
           "concatenation). Each family file maps every old assertion it inherits to "
           "`verbatim | renamed | oracle (literal) | new | dropped (why)`, lists its new properties, "
           "its must-hit reachability and its requests to task 99.\n")
out.append("## Summary\n")
inscope = [d for d in drivers if d not in K8S]
out.append(f"- **{len(drivers)} main drivers** (`workload/bin/cmds/main/`): {len(inscope)} in scope, "
           f"{len(drivers) - len(inscope)} out of scope (k8s operator, DESIGN §1 item 7).\n"
           "- **The model template** (`workload/bin/cmds/model/singleton_driver_model/`): lifted into the "
           "oracle service (`workload/oraclecheck/**`, `workload/bin/oracle/`).\n"
           "- **26 scenario blocks** (`singleton_driver_scenario_blocks`): `driver/src/rules/blocks.rs` "
           "through the client's `/block` (task 17).\n")
out.append("### Main drivers\n")
out.append("| Old driver | New home (porting file) |")
out.append("|---|---|")
for d in drivers:
    if d in K8S:
        extra = homes(r"\b" + d + r"\b")
        note = "out of scope: k8s operator (DESIGN §1 item 7)"
        if extra:
            note += "; the non-operator part is ported in " + ", ".join(f"`{h}`" for h in extra)
        out.append(f"| `{d}` | {note} |")
        continue
    h = homes(r"\b" + d + r"\b")
    cell = ", ".join(f"`{x}`" for x in h)
    if d in SPECIAL:
        cell = SPECIAL[d] + (", " + cell if cell else "")
    out.append(f"| `{d}` | {cell or '**missing**'} |")
out.append("")
out.append("### Model template (`M/` files)\n")
out.append("| `M/` file | New home (porting file) |")
out.append("|---|---|")
mdir = os.path.join(W, "bin", "cmds", "model", "singleton_driver_model")
for f in sorted(os.listdir(mdir)):
    if not f.endswith(".go") or f.endswith("_test.go"):
        continue
    h = homes(r"M/" + re.escape(f))
    note = {"generation.go": "dropped: its one assertion (`generation engine failed`) guards the old random engine, which hegel draws replace"}.get(f, "helper code without assertions (lifted as code where a family needed it)")
    out.append(f"| `{f}` | {', '.join(f'`{x}`' for x in h) or note} |")
out.append("")
out.append("### Scenario blocks\n")
blocks = re.findall(r'"([^"]+)" => "block ', open(os.path.join(HG, "driver", "src", "rules", "blocks.rs"), encoding="utf-8").read())
out.append(f"{len(blocks)} blocks, each with `block <name> succeeded` / `block <name> failed` in `rules/blocks.rs`: "
           + ", ".join(f"`{b}`" for b in blocks) + ".\n")
if appendix:
    out.append(appendix.rstrip() + "\n")
out.append("---\n")
for n, t in texts.items():
    out.append(f"<a id=\"{n}\"></a>\n")
    out.append(re.sub(r"^# ", "## ", t.strip(), count=1, flags=re.M).replace("\n# ", "\n## "))
    out.append("\n---\n")
sys.stdout.write("\n".join(out))
