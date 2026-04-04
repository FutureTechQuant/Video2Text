import argparse
import json
import re
import subprocess
from pathlib import Path

TEXT_EXTS = {".srt", ".vtt", ".ass", ".ssa", ".lrc", ".txt"}
JSON_EXTS = {".json", ".bcc"}

def safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120] if name else "untitled"

def run_cmd(cmd, check=True):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(msg)
    return result

def read_cookie_input(path: str):
    text = Path(path).read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        raise RuntimeError("cookie input is empty")

    lines = [x for x in text.splitlines() if x.strip()]
    first = lines[0].strip() if lines else ""

    if first in ("# Netscape HTTP Cookie File", "# HTTP Cookie File"):
        return ["--cookies", path]

    if "\t" in text and len(lines) >= 1:
        return ["--cookies", path]

    one_line = "; ".join(x.strip() for x in text.splitlines() if x.strip())
    return ["--add-header", f"Cookie: {one_line}"]

def load_urls(path: str):
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"input file not found: {path}")
    urls = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        urls.append(s)
    if not urls:
        raise RuntimeError("urls.txt is empty")
    return urls

def get_info(url: str, auth_args):
    cmd = ["yt-dlp", *auth_args, "--dump-single-json", "--skip-download", url]
    result = run_cmd(cmd)
    return json.loads(result.stdout)

def list_subs(url: str, auth_args):
    cmd = ["yt-dlp", *auth_args, "--list-subs", url]
    result = run_cmd(cmd, check=False)
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "list-subs failed")

def clean_text_subtitle(text: str):
    cleaned = []
    for raw in text.splitlines():
        s = raw.strip()
        if not s:
            continue
        if s in {"WEBVTT", "NOTE"}:
            continue
        if re.match(r"^\d+$", s):
            continue
        if "-->" in s:
            continue
        if re.match(r"^\d{2}:\d{2}:\d{2}[.,]\d{1,3}$", s):
            continue
        if re.match(r"^Dialogue:", s):
            parts = s.split(",", 9)
            s = parts[-1].strip() if len(parts) >= 10 else s
        if s.startswith(("Style:", "Format:", "[Script Info]", "[V4+ Styles]", "[Events]")):
            continue
        s = re.sub(r"<[^>]+>", "", s)
        s = re.sub(r"\{[^}]+\}", "", s)
        s = re.sub(r"\\N", "\n", s)
        s = s.strip()
        if s:
            cleaned.append(s)
    return "\n".join(cleaned).strip()

def extract_from_json_obj(obj):
    chunks = []

    def walk(x):
        if isinstance(x, dict):
            for key in ("content", "text", "utf8", "caption", "sentence", "transcript"):
                val = x.get(key)
                if isinstance(val, str) and val.strip():
                    chunks.append(val.strip())
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)

    dedup = []
    seen = set()
    for item in chunks:
        if item not in seen:
            seen.add(item)
            dedup.append(item)
    return "\n".join(dedup).strip()

def parse_subtitle_file(path: Path):
    ext = path.suffix.lower()
    text = path.read_text(encoding="utf-8", errors="ignore")

    if ext in TEXT_EXTS:
        return clean_text_subtitle(text)

    if ext in JSON_EXTS:
        try:
            obj = json.loads(text)
            extracted = extract_from_json_obj(obj)
            if extracted:
                return extracted
        except Exception:
            pass
        return text.strip()

    return clean_text_subtitle(text)

def find_sub_files(tmp_dir: Path):
    exts = [".srt", ".vtt", ".json", ".bcc", ".ass", ".ssa", ".lrc", ".txt"]
    files = []
    for ext in exts:
        files.extend(tmp_dir.glob(f"*{ext}"))
    return sorted(files)

def choose_best_file(files):
    priority = {
        ".srt": 1,
        ".vtt": 2,
        ".json": 3,
        ".bcc": 4,
        ".ass": 5,
        ".ssa": 6,
        ".lrc": 7,
        ".txt": 8,
    }
    return sorted(files, key=lambda p: priority.get(p.suffix.lower(), 99))[0]

def download_subs(url: str, auth_args, tmp_dir: Path):
    for f in tmp_dir.glob("*"):
        f.unlink()

    base_cmd = [
        "yt-dlp",
        *auth_args,
        "--skip-download",
        "--sub-langs", "all",
        "--sub-format", "srt/vtt/ass/json3/best",
        "-o", str(tmp_dir / "%(id)s.%(ext)s"),
        url
    ]

    regular = run_cmd([*base_cmd, "--write-subs"], check=False)
    files = find_sub_files(tmp_dir)
    if files:
        return files, regular.stdout, regular.stderr, "written"

    auto = run_cmd([*base_cmd, "--write-auto-subs"], check=False)
    files = find_sub_files(tmp_dir)
    if files:
        return files, auto.stdout, auto.stderr, "auto"

    msg = "\n".join([
        "regular stderr:",
        regular.stderr.strip(),
        "regular stdout:",
        regular.stdout.strip(),
        "auto stderr:",
        auto.stderr.strip(),
        "auto stdout:",
        auto.stdout.strip(),
    ]).strip()
    raise RuntimeError(msg or "No subtitle files downloaded")

def batch_mode(args):
    auth_args = read_cookie_input(args.cookie_input)
    urls = load_urls(args.input)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)

    tmp_dir = Path("tmp_subs")
    tmp_dir.mkdir(exist_ok=True)

    manifest = []

    for idx, url in enumerate(urls, start=1):
        row = {"index": idx, "url": url}
        try:
            info = get_info(url, auth_args)
            title = info.get("title") or f"video_{idx:04d}"
            row["title"] = title
            row["subtitle_langs"] = sorted(list((info.get("subtitles") or {}).keys()))
            row["auto_caption_langs"] = sorted(list((info.get("automatic_captions") or {}).keys()))

            files, stdout, stderr, source = download_subs(url, auth_args, tmp_dir)
            picked = choose_best_file(files)
            text = parse_subtitle_file(picked)

            if not text.strip():
                raise RuntimeError(f"subtitle file exists but cleaned text is empty: {picked.name}")

            filename = f"{idx:04d}_{safe_name(title)}.txt"
            body = f"标题：{title}\n链接：{url}\n来源：{source}\n文件：{picked.name}\n\n{text.strip()}\n"
            (out_dir / filename).write_text(body, encoding="utf-8")

            row["status"] = "ok"
            row["source"] = source
            row["picked_file"] = picked.name
            row["output_file"] = filename
            row["chars"] = len(body)
            row["stderr_tail"] = (stderr or "")[-1000:]
            row["stdout_tail"] = (stdout or "")[-1000:]
        except Exception as e:
            row["status"] = "error"
            row["error"] = str(e)

        manifest.append(row)

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    ok_count = sum(1 for x in manifest if x["status"] == "ok")
    if ok_count == 0:
        raise RuntimeError("All subtitle downloads failed. Check manifest.json and first-url list-subs output.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["list", "batch"], required=True)
    parser.add_argument("--url")
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--cookie-input", required=True)
    parser.add_argument("--manifest")
    args = parser.parse_args()

    auth_args = read_cookie_input(args.cookie_input)

    if args.mode == "list":
        if not args.url:
            raise RuntimeError("--url is required when mode=list")
        list_subs(args.url, auth_args)
        return

    if not args.input or not args.output or not args.manifest:
        raise RuntimeError("--input --output --manifest are required when mode=batch")

    batch_mode(args)

if __name__ == "__main__":
    main()
