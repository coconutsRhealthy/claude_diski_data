import argparse
from pathlib import Path

from discount_finder import config
from discount_finder.pipeline import run


def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Extract discount codes from Instagram posts.")
    parser.add_argument("--input", type=Path, help="Local Apify export JSON file.")
    parser.add_argument("--apify-dataset", help="Apify dataset id to fetch instead of a local file.")
    parser.add_argument("--output", type=Path, default=config.OUTPUT_PATH)
    parser.add_argument("--max-age-days", type=int, default=config.MAX_AGE_DAYS)
    parser.add_argument("--batch-size", type=int, default=config.BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Skip the Claude API calls; print counts only.")
    args = parser.parse_args()

    run(
        input_path=args.input,
        apify_dataset_id=args.apify_dataset,
        output_path=args.output,
        max_age_days=args.max_age_days,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
