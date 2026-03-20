import json
import subprocess
from pathlib import Path


def load_version(spec: str) -> dict:
    raw = subprocess.check_output(["git", "show", spec], text=True)
    return json.loads(raw)


def main() -> None:
    upstream = load_version(":2:state.json")
    current = load_version(":3:state.json")

    merged_urls = list(
        dict.fromkeys(upstream.get("posted_urls", []) + current.get("posted_urls", []))
    )[-500:]

    candidates = [value for value in [upstream.get("last_run"), current.get("last_run")] if value]
    last_run = max(candidates) if candidates else None

    merged = {
        "posted_urls": merged_urls,
        "last_run": last_run,
    }

    Path("state.json").write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
