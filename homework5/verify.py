"""Independent set-based oracle; never substitutes for Hadoop output."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent

def records(name):
    rows = []
    for line in (ROOT / "input" / name).read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        index, stock = line.split()
        rows.append((int(index), stock))
    return rows

def expected():
    a, b = set(records("A.txt")), set(records("B.txt"))
    relations, totals = [], {}
    for index, stock in sorted(a | b):
        tag = "BOTH" if (index, stock) in a & b else "A_ONLY" if (index, stock) in a else "B_ONLY"
        relations.append(f"{index}\t{stock}\t{tag}")
        counts = totals.setdefault(index, [0, 0, 0, 0])
        counts[{"BOTH": 0, "A_ONLY": 1, "B_ONLY": 2}[tag]] += 1
        counts[3] += 1
    stats = [str(index) + "\t" + "\t".join(map(str, counts)) for index, counts in sorted(totals.items())]
    return relations, stats

if __name__ == "__main__":
    ref1, ref2 = expected()
    if len(sys.argv) == 1:
        print("EXPECTED ONLY - not Hadoop execution evidence")
        print("index\tboth\ta_only\tb_only\tunion")
        print("\n".join(ref2))
    elif len(sys.argv) == 3:
        for filename, ref in zip(sys.argv[1:], [ref1, ref2]):
            actual = Path(filename).read_text(encoding="utf-8").splitlines()
            if actual != ref:
                raise SystemExit(f"VERIFY_FAILED: {filename}; expected {len(ref)} rows, got {len(actual)}. Check contents/order.")
        print(f"VERIFY_OK: {len(ref1)} relations and {len(ref2)} index summaries")
    else:
        raise SystemExit("Usage: python3 verify.py [task1-part-file task2-part-file]")
