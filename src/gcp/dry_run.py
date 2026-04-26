from __future__ import annotations

import argparse
import json

from src.gcp.assets import run_gcp_dry_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Pub/Sub and BigQuery dry-run assets")
    parser.add_argument("--output-dir", default=None, help="Directory where the dry-run assets are written")
    parser.add_argument("--num-users", type=int, default=None, help="Number of synthetic users to generate")
    parser.add_argument(
        "--events-per-user",
        type=int,
        default=None,
        help="Number of events per synthetic user",
    )
    args = parser.parse_args()

    summary = run_gcp_dry_run(
        output_dir=args.output_dir,
        num_users=args.num_users,
        events_per_user=args.events_per_user,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
