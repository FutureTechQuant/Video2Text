import argparse
import json
import os
import re
import subprocess
from pathlib import Path

def safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:120] if name else "untitled"

def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "command failed")
    return result.stdout

def get_info(url, cookies):
    cmd = [
        "yt-dlp",
        "--cookies", cookies,
        "--dump-single-json",
        "--skip-download",
        url
    ]
    data = run_cmd(cmd)
    return json.loads(data)

def get_subtitle_content(url, cookies, lang_candidates=None):
    if lang_candidates is None:
        lang_candidates = ["zh-CN", "zh-Hans", "zh", "ai-zh", "en"]

    tmp_dir = Path("tmp_subs")
    tmp_dir.mkdir(exist_ok=True)

    base = tmp_dir / "sub"
    for ext in ["vtt", "srt", "json3", "ass"]:
        f = tmp_dir / f"sub.{ext}"
        if f.exists():
            f.unlink()

    cmd = [
        "yt-dlp",
        "--cookies", cookies,
        "--skip-download",
        "--write-sub",
        "--sub-langs", ",".join(lang_candidates),
        "--convert-subs", "srt",
        "-o", str(base) + ".%(ext)s",
        url
    ]
    subprocess.run(cmd, capture_output=True, text=True)

    candidates = list(tmp_dir.glob("sub*.srt")) + list(tmp_dir.glob("sub*.vtt")) + list(tmp_dir.glob("sub*.ass"))
    if not candidates:
        return None

    content = candidates[0].read_text(encoding="utf-8", errors="ignore")
    for f in tmp_dir.iterdir():
        f.unlink()
    return content

def clean_subtitle(text: str) -> str:
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if re.match(r"^\d+$", s):
            continue
        if "-->" in s:
            continue
        if re.match(r"^\d{2}:\d{2}:\d{2}", s):
            continue
        lines.append(s)
    return "\n".join(lines).strip()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cookies", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    input_file = Path(args.input)
    output_dir = Path(args.output)
    manifest_file = Path(args.manifest)

    output_dir.mkdir(parents=True, exist_ok=True)

    urls = []
    if input_file.exists():
        urls = [x.strip() for x in input_file.read_text(encoding="utf-8").splitlines() if x.strip()]

    manifest = []
    index = 1

    for url in urls:
        try:
            info = get_info(url, args.cookies)
            title = info.get("title") or f"video_{index:04d}"
            clean_title = safe_name(title)
            sub = get_subtitle_content(url, args.cookies)
            if not sub:
                manifest.append({
                    "index": index,
                    "url": url,
                    "title": title,
                    "status": "no_subtitle"
                })
                index += 1
                continue

            text = clean_subtitle(sub)
            if not text:
                manifest.append({
                    "index": index,
                    "url": url,
                    "title": title,
                    "status": "empty_after_clean"
                })
                index += 1
                continue

            filename = f"{index:04d}_{clean_title}.txt"
            body = f"标题：{title}\n链接：{url}\n\n{text}\n"
            (output_dir / filename).write_text(body, encoding="utf-8")

            manifest.append({
                "index": index,
                "url": url,
                "title": title,
                "file": filename,
                "chars": len(body),
                "status": "ok"
            })
        except Exception as e:
            manifest.append({
                "index": index,
                "url": url,
                "status": "error",
                "error": str(e)
            })
        index += 1

    manifest_file.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
