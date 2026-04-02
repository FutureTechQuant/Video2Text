import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from faster_whisper import WhisperModel

SPACE_URL = os.getenv("BILIBILI_SPACE_URL", "https://space.bilibili.com/28152637/video")
COOKIES_FILE = Path(os.getenv("BILIBILI_COOKIES_FILE", "cookies.txt"))

STATE_DIR = Path("state")
TRANSCRIPTS_DIR = Path("transcripts")
TMP_DIR = Path("tmp_audio")
ERRORS_DIR = STATE_DIR / "errors"

QUEUE_FILE = STATE_DIR / "queue.json"
DONE_FILE = STATE_DIR / "done.txt"
FAILED_FILE = STATE_DIR / "failed.txt"
PROGRESS_FILE = STATE_DIR / "progress.json"
CONTINUE_FLAG = STATE_DIR / "continue.flag"

MODEL_NAME = os.getenv("WHISPER_MODEL", "small")
DEVICE = os.getenv("WHISPER_DEVICE", "cpu")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")
LANGUAGE = os.getenv("WHISPER_LANGUAGE", "zh")
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "7")
BEAM_SIZE = int(os.getenv("BEAM_SIZE", "1"))
VAD_FILTER = os.getenv("VAD_FILTER", "1") == "1"

def log(msg: str):
    print(msg, flush=True)

def ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)

def atomic_write_text(path: Path, content: str, encoding: str = "utf-8"):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding=encoding)
    tmp.replace(path)

def append_line(path: Path, line: str):
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data):
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))

def load_set(path: Path) -> set:
    if not path.exists():
        return set()
    return {x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()}

def seconds_to_hms(sec: float) -> str:
    sec = max(0, int(sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def sanitize_filename(name: str, max_len: int = 200) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip(" .") or "untitled"

def run(cmd: List[str], capture: bool = False, check: bool = True):
    log("[cmd] " + " ".join(cmd))
    return subprocess.run(cmd, text=True, capture_output=capture, check=check)

def format_video_url(entry: Dict) -> Optional[str]:
    url = entry.get("url") or entry.get("webpage_url") or ""
    vid = (entry.get("id") or "").strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if vid.startswith("BV"):
        return f"https://www.bilibili.com/video/{vid}"
    if url.startswith("BV"):
        return f"https://www.bilibili.com/video/{url}"
    return None

def save_progress(status: str, note: str = "", current: Optional[Dict] = None, queue_total: int = 0, queue_index: int = 0):
    payload = {
        "status": status,
        "note": note,
        "space_url": SPACE_URL,
        "queue_total": queue_total,
        "queue_index": queue_index,
        "updated_at": int(time.time()),
        "updated_at_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    if current:
        payload["current_bvid"] = current.get("id", "")
        payload["current_title"] = current.get("title", "")
        payload["current_url"] = current.get("url", "")
    save_json(PROGRESS_FILE, payload)

def load_existing_done() -> set:
    done = load_set(DONE_FILE)
    for p in TRANSCRIPTS_DIR.glob("BV*.txt"):
        done.add(p.stem)
    return done

def record_done(bvid: str):
    done = load_set(DONE_FILE)
    if bvid not in done:
        append_line(DONE_FILE, bvid)

def record_failed(bvid: str):
    failed = load_set(FAILED_FILE)
    if bvid not in failed:
        append_line(FAILED_FILE, bvid)

def write_error_file(bvid: str, title: str, url: str, err: Exception):
    path = ERRORS_DIR / f"{bvid}.txt"
    body = (
        f"bvid: {bvid}\n"
        f"title: {title}\n"
        f"url: {url}\n"
        f"time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n"
        f"error: {repr(err)}\n"
    )
    atomic_write_text(path, body)

def extract_queue_from_space() -> List[Dict]:
    if not COOKIES_FILE.exists():
        raise FileNotFoundError(f"cookies file not found: {COOKIES_FILE}")

    result = run([
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--flat-playlist",
        "--dump-single-json",
        SPACE_URL
    ], capture=True)

    raw = result.stdout.strip()
    if not raw:
        raise RuntimeError("empty playlist json")

    data = json.loads(raw)
    entries = data.get("entries") or []
    queue = []

    for i, e in enumerate(entries):
        bvid = (e.get("id") or "").strip()
        title = (e.get("title") or bvid or f"item_{i}").strip()
        url = format_video_url(e)
        if not bvid or not url:
            continue
        queue.append({
            "id": bvid,
            "title": title,
            "url": url
        })

    if not queue:
        raise RuntimeError("queue is empty")
    return queue

def load_or_build_queue() -> List[Dict]:
    if QUEUE_FILE.exists():
        queue = load_json(QUEUE_FILE, [])
        if queue:
            log(f"[info] loaded queue from state: {len(queue)} items")
            return queue

    log("[info] building queue from bilibili space")
    queue = extract_queue_from_space()
    save_json(QUEUE_FILE, queue)
    log(f"[info] queue saved: {len(queue)} items")
    return queue

def find_next_item(queue: List[Dict], done: set) -> Optional[Dict]:
    for item in queue:
        bvid = item["id"]
        transcript_file = TRANSCRIPTS_DIR / f"{bvid}.txt"
        if bvid not in done and not transcript_file.exists():
            return item
    return None

def has_more_pending(queue: List[Dict], done: set) -> bool:
    for item in queue:
        bvid = item["id"]
        transcript_file = TRANSCRIPTS_DIR / f"{bvid}.txt"
        if bvid not in done and not transcript_file.exists():
            return True
    return False

def download_audio(video_url: str, bvid: str) -> Path:
    outtmpl = str(TMP_DIR / f"{bvid}.%(ext)s")
    run([
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--no-playlist",
        "-f", "ba/bestaudio",
        "-x",
        "--audio-format", AUDIO_FORMAT,
        "--audio-quality", AUDIO_QUALITY,
        "-o", outtmpl,
        video_url
    ])

    files = [p for p in TMP_DIR.glob(f"{bvid}.*") if p.is_file() and not p.name.endswith(".part")]
    if not files:
        raise RuntimeError(f"audio file not found for {bvid}")
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]

def load_model() -> WhisperModel:
    log(f"[info] loading model: {MODEL_NAME}, device={DEVICE}, compute_type={COMPUTE_TYPE}")
    return WhisperModel(MODEL_NAME, device=DEVICE, compute_type=COMPUTE_TYPE)

def transcribe_audio(model: WhisperModel, audio_path: Path) -> Dict:
    segments, info = model.transcribe(
        str(audio_path),
        language=LANGUAGE if LANGUAGE else None,
        beam_size=BEAM_SIZE,
        vad_filter=VAD_FILTER,
        condition_on_previous_text=False,
    )

    lines = []
    seg_count = 0
    for seg in segments:
        seg_count += 1
        text = (seg.text or "").strip()
        if not text:
            continue
        lines.append(f"[{seconds_to_hms(seg.start)} - {seconds_to_hms(seg.end)}] {text}")

    return {
        "language": getattr(info, "language", ""),
        "language_probability": getattr(info, "language_probability", ""),
        "segments": seg_count,
        "text": "\n".join(lines).strip(),
    }

def write_transcript(item: Dict, result: Dict):
    bvid = item["id"]
    title = sanitize_filename(item["title"], 300)
    url = item["url"]

    out = TRANSCRIPTS_DIR / f"{bvid}.txt"
    body = (
        f"BV号：{bvid}\n"
        f"标题：{title}\n"
        f"链接：{url}\n"
        f"识别语言：{result.get('language', '')}\n"
        f"语言置信度：{result.get('language_probability', '')}\n"
        f"分段数：{result.get('segments', 0)}\n"
        f"生成时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n"
        f"\n"
        f"{result.get('text', '').strip()}\n"
    )
    atomic_write_text(out, body)

def cleanup_temp_file(path: Optional[Path]):
    try:
        if path and path.exists():
            path.unlink(missing_ok=True)
    except Exception as e:
        log(f"[warn] cleanup failed: {e}")

def touch_continue():
    CONTINUE_FLAG.write_text("1\n", encoding="utf-8")

def clear_continue():
    CONTINUE_FLAG.unlink(missing_ok=True)

def main():
    ensure_dirs()
    clear_continue()

    queue = load_or_build_queue()
    done = load_existing_done()

    next_item = find_next_item(queue, done)
    if not next_item:
        save_progress("finished", note="all items completed", queue_total=len(queue), queue_index=len(done))
        log("[info] all items completed")
        return

    model = load_model()
    audio_path = None

    try:
        idx = next((i for i, x in enumerate(queue) if x["id"] == next_item["id"]), 0)
        save_progress("downloading_audio", current=next_item, queue_total=len(queue), queue_index=idx)
        audio_path = download_audio(next_item["url"], next_item["id"])

        save_progress("transcribing", current=next_item, queue_total=len(queue), queue_index=idx)
        result = transcribe_audio(model, audio_path)

        save_progress("writing_transcript", current=next_item, queue_total=len(queue), queue_index=idx)
        write_transcript(next_item, result)

        record_done(next_item["id"])
        done.add(next_item["id"])

        save_progress("done_one", note="one video processed", current=next_item, queue_total=len(queue), queue_index=idx + 1)
        log(f"[ok] completed: {next_item['id']} {next_item['title']}")

    except Exception as e:
        record_failed(next_item["id"])
        write_error_file(next_item["id"], next_item["title"], next_item["url"], e)
        save_progress("error", note=repr(e), current=next_item, queue_total=len(queue), queue_index=idx)
        log(f"[error] {next_item['id']}: {e}")

    finally:
        cleanup_temp_file(audio_path)

    if has_more_pending(queue, done):
        touch_continue()
        save_progress("queued_next_run", note="continue.flag created for next video", queue_total=len(queue), queue_index=len(done))
        log("[info] more pending items exist, continue.flag created")
    else:
        clear_continue()
        save_progress("finished", note="all items completed", queue_total=len(queue), queue_index=len(done))
        log("[info] no more pending items")

if __name__ == "__main__":
    main()
