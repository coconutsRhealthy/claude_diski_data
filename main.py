import argparse
from pathlib import Path

from discount_finder import config
from discount_finder.pipeline import run


# Market used when no --market flag is passed (e.g. when hitting Run in
# IntelliJ without parameters). Change this to switch between runs.
MARKET = "germany"


def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Extract discount codes from Instagram posts.")
    parser.add_argument(
        "--market",
        default=MARKET,
        choices=config.MARKETS,
        help=f"Which market to process. Defaults to MARKET in main.py ({MARKET!r}).",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Local Apify export JSON file. Defaults to the most recent dataset_*.json in inputs/<market>/.",
    )
    parser.add_argument(
        "--apify-dataset",
        help="Apify dataset id to fetch instead of a local file. Defaults to env var APIFY_DATASET_ID_<MARKET>.",
    )
    parser.add_argument(
        "--apify-run",
        action="store_true",
        help="Trigger the Instagram scraper actor for this market using inputs/<market>/influencers.txt and process the resulting dataset.",
    )
    parser.add_argument("--output", type=Path, help="Override the full-output JSON path.")
    parser.add_argument("--max-age-days", type=int, default=config.MAX_AGE_DAYS)
    parser.add_argument("--batch-size", type=int, default=config.BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Skip the Claude API calls; print counts only.")
    args = parser.parse_args()

    run(
        market=args.market,
        input_path=args.input,
        apify_dataset_id=args.apify_dataset,
        apify_run=args.apify_run,
        output_path=args.output,
        max_age_days=args.max_age_days,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
