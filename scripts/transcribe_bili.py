import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    from faster_whisper import WhisperModel
except Exception as e:
    print(f"[fatal] failed to import faster_whisper: {e}", file=sys.stderr)
    raise

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
LOCK_FILE = STATE_DIR / "run.lock"

MODEL_NAME = os.getenv("WHISPER_MODEL", "small")
DEVICE = os.getenv("WHISPER_DEVICE", "cpu")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "int8")
LANGUAGE = os.getenv("WHISPER_LANGUAGE", "zh")

MAX_SECONDS = int(os.getenv("MAX_SECONDS", str(330 * 60)))
AUDIO_FORMAT = os.getenv("AUDIO_FORMAT", "mp3")
AUDIO_QUALITY = os.getenv("AUDIO_QUALITY", "7")
SLEEP_BETWEEN_ITEMS = float(os.getenv("SLEEP_BETWEEN_ITEMS", "2"))
BEAM_SIZE = int(os.getenv("BEAM_SIZE", "1"))
VAD_FILTER = os.getenv("VAD_FILTER", "1") == "1"

START_TS = time.time()


def now_ts() -> int:
    return int(time.time())


def ensure_dirs():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    ERRORS_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    print(msg, flush=True)


def run(cmd: List[str], capture: bool = False, check: bool = True, timeout: Optional[int] = None):
    log("[cmd] " + " ".join(shlex_quote(x) for x in cmd))
    return subprocess.run(
        cmd,
        text=True,
        capture_output=capture,
        check=check,
        timeout=timeout,
    )


def shlex_quote(s: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9._:/\\=-]+", s or ""):
        return s
    return '"' + s.replace('"', '\\"') + '"'


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
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_set(path: Path) -> set:
    if not path.exists():
        return set()
    return {x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()}


def sanitize_filename(name: str, max_len: int = 120) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:max_len].rstrip(" .") or "untitled"


def seconds_to_hms(sec: float) -> str:
    sec = max(0, int(sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def should_pause() -> bool:
    return (time.time() - START_TS) >= MAX_SECONDS


def save_progress(
    *,
    status: str,
    queue_total: int,
    queue_index: int,
    current_bvid: str = "",
    current_title: str = "",
    note: str = "",
):
    data = {
        "status": status,
        "space_url": SPACE_URL,
        "queue_total": queue_total,
        "queue_index": queue_index,
        "current_bvid": current_bvid,
        "current_title": current_title,
        "elapsed_seconds": int(time.time() - START_TS),
        "elapsed_hms": seconds_to_hms(time.time() - START_TS),
        "updated_at": now_ts(),
        "updated_at_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "note": note,
    }
    save_json(PROGRESS_FILE, data)


def acquire_lock():
    if LOCK_FILE.exists():
        old = load_json(LOCK_FILE, {})
        pid = old.get("pid")
        ts = old.get("ts")
        log(f"[warn] existing lock found, overwrite it. old_pid={pid}, old_ts={ts}")
    save_json(LOCK_FILE, {"pid": os.getpid(), "ts": now_ts()})


def release_lock():
    if LOCK_FILE.exists():
        LOCK_FILE.unlink(missing_ok=True)


def mark_continue(flag: bool):
    if flag:
        CONTINUE_FLAG.write_text("1\n", encoding="utf-8")
    else:
        CONTINUE_FLAG.unlink(missing_ok=True)


def format_video_url(entry: Dict) -> Optional[str]:
    url = entry.get("url") or entry.get("webpage_url") or ""
    vid = entry.get("id") or ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if vid.startswith("BV"):
        return f"https://www.bilibili.com/video/{vid}"
    if url.startswith("BV"):
        return f"https://www.bilibili.com/video/{url}"
    return None


def extract_queue_from_space() -> List[Dict]:
    if not COOKIES_FILE.exists():
        raise FileNotFoundError(f"cookies file not found: {COOKIES_FILE}")

    cmd = [
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--flat-playlist",
        "--dump-single-json",
        SPACE_URL,
    ]
    result = run(cmd, capture=True, check=True)
    raw = result.stdout.strip()
    if not raw:
        raise RuntimeError("yt-dlp returned empty playlist json")

    data = json.loads(raw)
    entries = data.get("entries") or []
    queue = []

    for idx, e in enumerate(entries):
        bvid = (e.get("id") or "").strip()
        title = (e.get("title") or bvid or f"item_{idx}").strip()
        url = format_video_url(e)
        if not bvid or not url:
            continue
        queue.append({
            "id": bvid,
            "title": title,
            "url": url,
        })

    if not queue:
        raise RuntimeError("queue is empty after extracting playlist")
    return queue


def load_or_build_queue() -> List[Dict]:
    if QUEUE_FILE.exists():
        queue = load_json(QUEUE_FILE, [])
        if queue:
            log(f"[info] loaded existing queue: {len(queue)} items")
            return queue

    log("[info] building queue from bilibili space")
    queue = extract_queue_from_space()
    save_json(QUEUE_FILE, queue)
    log(f"[info] queue saved: {len(queue)} items")
    return queue


def find_resume_index(queue: List[Dict], done: set) -> int:
    for i, item in enumerate(queue):
        bvid = item["id"]
        transcript_file = TRANSCRIPTS_DIR / f"{bvid}.txt"
        if bvid not in done and not transcript_file.exists():
            return i
    return len(queue)


def write_error_file(bvid: str, title: str, url: str, err: Exception):
    error_path = ERRORS_DIR / f"{bvid}.txt"
    content = (
        f"bvid: {bvid}\n"
        f"title: {title}\n"
        f"url: {url}\n"
        f"time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n"
        f"error: {repr(err)}\n"
    )
    atomic_write_text(error_path, content)


def load_model() -> WhisperModel:
    log(f"[info] loading model: {MODEL_NAME}, device={DEVICE}, compute_type={COMPUTE_TYPE}")
    model = WhisperModel(MODEL_NAME, device=DEVICE, compute_type=COMPUTE_TYPE)
    return model


def download_audio(video_url: str, bvid: str) -> Path:
    outtmpl = str(TMP_DIR / f"{bvid}.%(ext)s")
    cmd = [
        "yt-dlp",
        "--cookies", str(COOKIES_FILE),
        "--no-playlist",
        "-f", "ba/bestaudio",
        "-x",
        "--audio-format", AUDIO_FORMAT,
        "--audio-quality", AUDIO_QUALITY,
        "-o", outtmpl,
        video_url,
    ]
    run(cmd, capture=False, check=True)

    candidates = sorted(TMP_DIR.glob(f"{bvid}.*"))
    candidates = [p for p in candidates if p.is_file() and not p.name.endswith(".part")]
    if not candidates:
        raise RuntimeError(f"audio file not found for {bvid}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def transcribe_audio(model: WhisperModel, audio_path: Path) -> Dict:
    segments, info = model.transcribe(
        str(audio_path),
        language=LANGUAGE if LANGUAGE else None,
        beam_size=BEAM_SIZE,
        vad_filter=VAD_FILTER,
        condition_on_previous_text=False,
    )

    texts = []
    seg_count = 0
    for seg in segments:
        seg_count += 1
        text = (seg.text or "").strip()
        if not text:
            continue
        start = seconds_to_hms(seg.start)
        end = seconds_to_hms(seg.end)
        texts.append(f"[{start} - {end}] {text}")

    return {
        "language": getattr(info, "language", ""),
        "language_probability": getattr(info, "language_probability", None),
        "segments": seg_count,
        "text": "\n".join(texts).strip(),
    }


def write_transcript(bvid: str, title: str, video_url: str, data: Dict):
    out = TRANSCRIPTS_DIR / f"{bvid}.txt"
    body = (
        f"BV号：{bvid}\n"
        f"标题：{title}\n"
        f"链接：{video_url}\n"
        f"识别语言：{data.get('language', '')}\n"
        f"语言置信度：{data.get('language_probability', '')}\n"
        f"分段数：{data.get('segments', 0)}\n"
        f"生成时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n"
        f"\n"
        f"{data.get('text', '').strip()}\n"
    )
    atomic_write_text(out, body)


def cleanup_temp_file(path: Optional[Path]):
    try:
        if path and path.exists():
            path.unlink(missing_ok=True)
    except Exception as e:
        log(f"[warn] temp cleanup failed: {e}")


def record_done(bvid: str):
    done = load_set(DONE_FILE)
    if bvid not in done:
        append_line(DONE_FILE, bvid)


def record_failed(bvid: str):
    failed = load_set(FAILED_FILE)
    if bvid not in failed:
        append_line(FAILED_FILE, bvid)


def load_existing_done() -> set:
    done = load_set(DONE_FILE)
    for p in TRANSCRIPTS_DIR.glob("BV*.txt"):
        done.add(p.stem)
    return done


def main():
    ensure_dirs()
    acquire_lock()
    mark_continue(False)

    try:
        queue = load_or_build_queue()
        done = load_existing_done()
        total = len(queue)
        resume_index = find_resume_index(queue, done)

        if resume_index >= total:
            save_progress(
                status="finished",
                queue_total=total,
                queue_index=total,
                note="all queue items already processed",
            )
            log("[info] all items already processed")
            return

        model = load_model()

        for i in range(resume_index, total):
            item = queue[i]
            bvid = item["id"]
            title = item.get("title", bvid)
            video_url = item["url"]

            transcript_file = TRANSCRIPTS_DIR / f"{bvid}.txt"
            if bvid in done or transcript_file.exists():
                done.add(bvid)
                save_progress(
                    status="skipped_done",
                    queue_total=total,
                    queue_index=i + 1,
                    current_bvid=bvid,
                    current_title=title,
                    note="already done, skip",
                )
                continue

            if should_pause():
                mark_continue(True)
                save_progress(
                    status="paused_for_next_run",
                    queue_total=total,
                    queue_index=i,
                    current_bvid=bvid,
                    current_title=title,
                    note="time budget reached before starting next item",
                )
                log("[info] time budget reached, pause for next run")
                return

            audio_path = None
            try:
                save_progress(
                    status="downloading_audio",
                    queue_total=total,
                    queue_index=i,
                    current_bvid=bvid,
                    current_title=title,
                )
                audio_path = download_audio(video_url, bvid)

                if should_pause():
                    mark_continue(True)
                    save_progress(
                        status="paused_for_next_run",
                        queue_total=total,
                        queue_index=i,
                        current_bvid=bvid,
                        current_title=title,
                        note="time budget reached after download",
                    )
                    log("[info] time budget reached after download, pause for next run")
                    return

                save_progress(
                    status="transcribing",
                    queue_total=total,
                    queue_index=i,
                    current_bvid=bvid,
                    current_title=title,
                )
                result = transcribe_audio(model, audio_path)

                save_progress(
                    status="writing_transcript",
                    queue_total=total,
                    queue_index=i,
                    current_bvid=bvid,
                    current_title=title,
                )
                write_transcript(
                    bvid=bvid,
                    title=sanitize_filename(title, 300),
                    video_url=video_url,
                    data=result,
                )

                record_done(bvid)
                done.add(bvid)

                save_progress(
                    status="done_one",
                    queue_total=total,
                    queue_index=i + 1,
                    current_bvid=bvid,
                    current_title=title,
                    note="one transcript saved",
                )
                log(f"[ok] done: {bvid} {title}")

            except Exception as e:
                record_failed(bvid)
                write_error_file(bvid, title, video_url, e)
                save_progress(
                    status="error",
                    queue_total=total,
                    queue_index=i + 1,
                    current_bvid=bvid,
                    current_title=title,
                    note=repr(e),
                )
                log(f"[error] {bvid}: {e}")

            finally:
                cleanup_temp_file(audio_path)
                if SLEEP_BETWEEN_ITEMS > 0:
                    time.sleep(SLEEP_BETWEEN_ITEMS)

        mark_continue(False)
        save_progress(
            status="finished",
            queue_total=total,
            queue_index=total,
            note="queue finished",
        )
        log("[info] all items processed")

    finally:
        release_lock()


if __name__ == "__main__":
    main()
