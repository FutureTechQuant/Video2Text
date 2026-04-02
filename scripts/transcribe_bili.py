import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

SPACE_URL = "https://space.bilibili.com/28152637/video"
ROOT = Path(".")
STATE_DIR = ROOT / "state"
TRANS_DIR = ROOT / "transcripts"
TMP_DIR = ROOT / "tmp_audio"
COOKIES = ROOT / "cookies.txt"

QUEUE_FILE = STATE_DIR / "queue.json"
DONE_FILE = STATE_DIR / "done.txt"
CURSOR_FILE = STATE_DIR / "cursor.json"
CONTINUE_FLAG = STATE_DIR / "continue.flag"

MAX_SECONDS = 330 * 60  # 主动在 5.5 小时左右停，避免被 6 小时硬切
MODEL_NAME = "small"

STATE_DIR.mkdir(exist_ok=True)
TRANS_DIR.mkdir(exist_ok=True)
TMP_DIR.mkdir(exist_ok=True)

def run(cmd, capture=False, check=True):
    if capture:
        return subprocess.run(cmd, check=check, text=True, capture_output=True)
    return subprocess.run(cmd, check=check)

def load_done():
    if not DONE_FILE.exists():
        return set()
    return {line.strip() for line in DONE_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}

def save_done(bvid):
    with DONE_FILE.open("a", encoding="utf-8") as f:
        f.write(bvid + "\n")

def save_cursor(index):
    CURSOR_FILE.write_text(json.dumps({"index": index}, ensure_ascii=False, indent=2), encoding="utf-8")

def load_cursor():
    if not CURSOR_FILE.exists():
        return 0
    try:
        return int(json.loads(CURSOR_FILE.read_text(encoding="utf-8")).get("index", 0))
    except Exception:
        return 0

def build_queue():
    cmd = [
        "yt-dlp",
        "--cookies", str(COOKIES),
        "--flat-playlist",
        "--dump-single-json",
        SPACE_URL
    ]
    res = run(cmd, capture=True)
    data = json.loads(res.stdout)
    entries = data.get("entries", [])
    queue = []
    for e in entries:
        vid = e.get("id")
        url = e.get("url") or (f"https://www.bilibili.com/video/{vid}" if vid else None)
        title = e.get("title") or vid
        if vid and url:
            queue.append({"id": vid, "url": url, "title": title})
    QUEUE_FILE.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
    return queue

def load_queue():
    if QUEUE_FILE.exists():
        return json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
    return build_queue()

def sanitize(name: str):
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name.strip()

def download_audio(url, vid):
    outtmpl = str(TMP_DIR / f"{vid}.%(ext)s")
    cmd = [
        "yt-dlp",
        "--cookies", str(COOKIES),
        "-f", "ba/bestaudio",
        "-x",
        "--audio-format", "mp3",
        "--audio-quality", "7",
        "-o", outtmpl,
        url
    ]
    run(cmd)
    files = list(TMP_DIR.glob(f"{vid}.*"))
    if not files:
        raise RuntimeError(f"audio not found for {vid}")
    return files[0]

def transcribe(audio_file: Path):
    from faster_whisper import WhisperModel
    model = WhisperModel(MODEL_NAME, device="cpu", compute_type="int8")
    segments, info = model.transcribe(str(audio_file), language="zh", vad_filter=True, beam_size=1)
    parts = []
    for seg in segments:
        start = f"{int(seg.start//3600):02d}:{int((seg.start%3600)//60):02d}:{int(seg.start%60):02d}"
        end = f"{int(seg.end//3600):02d}:{int((seg.end%3600)//60):02d}:{int(seg.end%60):02d}"
        text = seg.text.strip()
        if text:
            parts.append(f"[{start} - {end}] {text}")
    return "\n".join(parts)

def git_commit_one(vid):
    run(["git", "add", "transcripts", "state"], check=False)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode != 0:
        run(["git", "commit", "-m", f"transcript: {vid}"], check=False)
        run(["git", "push"], check=False)

def main():
    start = time.time()
    CONTINUE_FLAG.unlink(missing_ok=True)

    queue = load_queue()
    done = load_done()
    cursor = load_cursor()

    for i in range(cursor, len(queue)):
        if time.time() - start > MAX_SECONDS:
            CONTINUE_FLAG.write_text("1", encoding="utf-8")
            save_cursor(i)
            return

        item = queue[i]
        vid = item["id"]
        url = item["url"]
        title = sanitize(item["title"])

        if vid in done or (TRANS_DIR / f"{vid}.txt").exists():
            save_cursor(i + 1)
            continue

        audio = None
        try:
            audio = download_audio(url, vid)
            text = transcribe(audio)
            out_file = TRANS_DIR / f"{vid}.txt"
            out_file.write_text(f"标题：{title}\n链接：{url}\n\n{text}\n", encoding="utf-8")
            save_done(vid)
            save_cursor(i + 1)
            git_commit_one(vid)
        except Exception as e:
            err = TRANS_DIR / f"{vid}.error.txt"
            err.write_text(f"{url}\n{repr(e)}\n", encoding="utf-8")
            save_cursor(i + 1)
            git_commit_one(vid)
        finally:
            if audio and Path(audio).exists():
                Path(audio).unlink(missing_ok=True)

    CONTINUE_FLAG.unlink(missing_ok=True)

if __name__ == "__main__":
    main()
