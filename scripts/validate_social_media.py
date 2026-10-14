#!/usr/bin/env python3
"""Validate social media posts (JSON) and write a tab-separated file for Hive."""

import argparse
import json
import sys
from datetime import datetime

REQUIRED = ["post_id", "user", "text", "likes", "comments", "shares", "timestamp"]
COUNTERS = ["likes", "comments", "shares"]
HEADER = ["post_id", "user_name", "post_text", "likes", "comments",
          "shares", "post_timestamp", "media_file"]


class ValidationError(Exception):
    pass


def clean(value):
    """Keep every record on one line with no tabs, so TSV columns stay aligned."""
    return " ".join(str(value).split())


def validate_posts(data):
    if not isinstance(data, dict) or not isinstance(data.get("posts"), list):
        raise ValidationError("JSON must be an object with a top-level 'posts' array")
    if not data["posts"]:
        raise ValidationError("'posts' array is empty")

    errors, rows, seen = [], [], set()
    for i, post in enumerate(data["posts"], start=1):
        where = f"post #{i}"
        if not isinstance(post, dict):
            errors.append(f"{where}: must be an object")
            continue
        missing = [f for f in REQUIRED if f not in post or post[f] in (None, "")]
        if missing:
            errors.append(f"{where}: missing fields {missing}")
            continue
        if post["post_id"] in seen:
            errors.append(f"{where}: duplicate post_id {post['post_id']}")
            continue
        seen.add(post["post_id"])
        bad = False
        for f in COUNTERS:
            v = post[f]
            if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                errors.append(f"{where}: '{f}' must be a non-negative integer")
                bad = True
        try:
            datetime.fromisoformat(str(post["timestamp"]).replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"{where}: 'timestamp' is not ISO format")
            bad = True
        if bad:
            continue
        rows.append([
            clean(post["post_id"]), clean(post["user"]).lower(), clean(post["text"]),
            post["likes"], post["comments"], post["shares"],
            clean(post["timestamp"]), clean(post.get("media_file") or ""),
        ])
    if errors:
        raise ValidationError("\n".join(errors))
    return rows


def run(input_path, output_path):
    try:
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Cannot read JSON: {exc}")
    rows = validate_posts(data)
    with open(output_path, "w", encoding="utf-8") as out:
        out.write("\t".join(HEADER) + "\n")
        for row in rows:
            out.write("\t".join(str(c) for c in row) + "\n")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        count = run(args.input, args.output)
    except ValidationError as exc:
        print(f"VALIDATION FAILED:\n{exc}", file=sys.stderr)
        sys.exit(1)
    print(f"OK: {count} posts validated and written to {args.output}")


if __name__ == "__main__":
    main()
