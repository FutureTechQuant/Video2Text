import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--limit", type=int, default=100000)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for old in output_dir.glob("merged_*.txt"):
        old.unlink()

    files = sorted(input_dir.glob("*.txt"))
    if not files:
        raise RuntimeError("No raw subtitle txt files found")

    bucket = []
    bucket_len = 0
    part = 1

    def flush():
        nonlocal bucket, bucket_len, part
        if not bucket:
            return
        merged_text = ("\n\n" + "=" * 40 + "\n\n").join(bucket).strip() + "\n"
        (output_dir / f"merged_{part:03d}.txt").write_text(merged_text, encoding="utf-8")
        part += 1
        bucket = []
        bucket_len = 0

    for file in files:
        text = file.read_text(encoding="utf-8", errors="ignore")
        size = len(text)

        if size > args.limit:
            flush()
            (output_dir / f"merged_{part:03d}.txt").write_text(text, encoding="utf-8")
            part += 1
            continue

        if bucket_len + size > args.limit:
            flush()

        bucket.append(text)
        bucket_len += size

    flush()

if __name__ == "__main__":
    main()
