"""Best-effort upload of per-run social artifacts to Cloudflare R2.

Mirrors the boto3-against-R2 pattern used in image_openai
(R2_ACCOUNT_ID + R2_ACCESS_KEY + R2_SECRET_KEY env vars, endpoint
constructed from the account id, put_object with explicit Body and
ContentType). The destination bucket is ``config.CAROUSEL_BUCKET``.

Designed to be a soft addition: if any of the required env vars are
missing the function logs a single skip line and returns 0 — the
pipeline keeps going and the local cache at
``output/<market>/social/<date>/`` is still written.

Object keys mirror the local layout:
    <market>/<YYYY-MM-DD>/<filename>
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from . import config


_REQUIRED_ENV = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY", "R2_SECRET_KEY")

# Small fixed map — we only ever upload these from this project. Avoids
# pulling in mimetypes and stays explicit (matches the image_openai style
# of hardcoding ContentType per call).
_CONTENT_TYPES = {
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".txt":  "text/plain; charset=utf-8",
}


def _get_r2_client():
    import boto3

    endpoint = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY"],
        aws_secret_access_key=os.environ["R2_SECRET_KEY"],
    )


def upload_dir(local_dir: Path, *, market: str, run_date: date) -> int:
    """Upload every file under ``local_dir`` to R2. Returns the count uploaded.

    Object keys take the form ``<market>/<YYYY-MM-DD>/<filename>`` so a
    browser of the bucket can scan by market then date. Returns 0 (and
    logs) on misconfiguration or any upload error — never raises.
    """
    if not local_dir.exists():
        return 0

    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        print(
            f"R2 upload skipped: missing env var(s) {', '.join(missing)}. "
            f"Files remain at {local_dir}.",
            flush=True,
        )
        return 0

    files = sorted(p for p in local_dir.iterdir() if p.is_file())
    if not files:
        return 0

    try:
        r2 = _get_r2_client()
    except ImportError as e:
        print(f"R2 upload skipped: boto3 not installed ({e}).", flush=True)
        return 0

    bucket = config.CAROUSEL_BUCKET
    prefix = f"{market}/{run_date.isoformat()}"

    uploaded = 0
    for f in files:
        key = f"{prefix}/{f.name}"
        content_type = _CONTENT_TYPES.get(f.suffix.lower(), "application/octet-stream")
        try:
            with f.open("rb") as fp:
                r2.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=fp,
                    ContentType=content_type,
                )
            uploaded += 1
        except Exception as e:
            print(f"R2 upload failed for {key}: {e}", flush=True)
            # keep going — other files in the batch may still succeed

    print(
        f"R2: uploaded {uploaded}/{len(files)} file(s) to s3://{bucket}/{prefix}/",
        flush=True,
    )
    return uploaded
