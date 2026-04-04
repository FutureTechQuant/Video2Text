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

    files = sorted(input_dir.glob("*.txt"))
    bucket = []
    bucket_len = 0
    part = 1

    def flush():
        nonlocal bucket, bucket_len, part
        if not bucket:
            return
        merged_text = "\n\n" + ("\n\n" + ("=" * 40) + "\n\n").join(bucket)
        out = output_dir / f"merged_{part:03d}.txt"
        out.write_text(merged_text.strip() + "\n", encoding="utf-8")
        part += 1
        bucket = []
        bucket_len = 0

    for file in files:
        text = file.read_text(encoding="utf-8", errors="ignore")
        text_len = len(text)

        if text_len > args.limit:
            flush()
            out = output_dir / f"merged_{part:03d}.txt"
            out.write_text(text, encoding="utf-8")
            part += 1
            continue

        if bucket_len + text_len > args.limit:
            flush()

        bucket.append(text)
        bucket_len += text_len

    flush()

if __name__ == "__main__":
    main()
