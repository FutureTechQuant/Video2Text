import json
import os
import re
import time
from pathlib import Path
from http.cookiejar import MozillaCookieJar

import requests

BVID = os.getenv("BVID", "BV1TC1jYmEve")
COOKIE_FILE = os.getenv("BILIBILI_COOKIE_FILE", "cookies.txt")

OUT_DIR = Path("subtitles") / BVID
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": f"https://www.bilibili.com/video/{BVID}",
}


def safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\n\r\t]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120].strip(" ._") or "untitled"


def sec_to_srt_time(sec: float) -> str:
    ms = int(round(sec * 1000))
    h = ms // 3600000
    ms %= 3600000
    m = ms // 60000
    ms %= 60000
    s = ms // 1000
    ms %= 1000
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def json_to_srt(body):
    lines = []
    idx = 1
    for item in body:
        content = str(item.get("content", "")).strip()
        if not content:
            continue
        start = sec_to_srt_time(float(item["from"]))
        end = sec_to_srt_time(float(item["to"]))
        lines.append(f"{idx}\n{start} --> {end}\n{content}\n")
        idx += 1
    return "\n".join(lines)


def build_session() -> requests.Session:
    cookie_path = Path(COOKIE_FILE)
    if not cookie_path.exists():
        raise RuntimeError(f"cookie file not found: {COOKIE_FILE}")

    text = cookie_path.read_text(encoding="utf-8", errors="ignore")
    lines = [line for line in text.splitlines() if line.strip()]

    if not lines:
        raise RuntimeError("cookie file is empty")

    first = lines[0].strip()
    if first not in ("# Netscape HTTP Cookie File", "# HTTP Cookie File"):
        raise RuntimeError(f"cookie file is not Netscape/Mozilla format, first line: {first!r}")

    jar = MozillaCookieJar(str(cookie_path))
    jar.load(ignore_discard=True, ignore_expires=True)

    all_cookie_names = {c.name for c in jar}
    if "SESSDATA" not in all_cookie_names:
        raise RuntimeError("cookies.txt missing SESSDATA")

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies = jar
    return session


session = build_session()


def get_json(url: str, params=None):
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("code") not in (None, 0):
        raise RuntimeError(f"api failed: {data}")
    return data


def get_video_info(bvid: str):
    url = "https://api.bilibili.com/x/web-interface/view"
    data = get_json(url, {"bvid": bvid})
    return data["data"]


def get_pages(bvid: str):
    url = "https://api.bilibili.com/x/player/pagelist"
    data = get_json(url, {"bvid": bvid})
    return data["data"]


def get_subtitles_meta(bvid: str, cid: int):
    url = "https://api.bilibili.com/x/player/v2"
    data = get_json(url, {"bvid": bvid, "cid": cid})
    subtitle = data.get("data", {}).get("subtitle", {})
    return subtitle.get("subtitles", [])


def pick_subtitle(subtitles):
    if not subtitles:
        return None

    preferred = [
        "zh-CN",
        "zh-Hans",
        "zh",
        "ai-zh",
        "zh-TW",
    ]
    for lan in preferred:
        for item in subtitles:
            if item.get("lan") == lan:
                return item
    return subtitles[0]


def download_subtitle_json(subtitle_url: str):
    if subtitle_url.startswith("//"):
        subtitle_url = "https:" + subtitle_url
    r = session.get(subtitle_url, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    info = get_video_info(BVID)
    title = info["title"]
    pages = get_pages(BVID)

    meta = {
        "bvid": BVID,
        "title": title,
        "video_count": len(pages),
        "pages": []
    }

    for page in pages:
        p = page["page"]
        cid = page["cid"]
        part = page["part"]
        base = f"{p:03d}-{safe_name(part)}"

        try:
            subtitles = get_subtitles_meta(BVID, cid)
            chosen = pick_subtitle(subtitles)

            if not chosen:
                print(f"[skip] p{p} {part} -> no subtitle")
                meta["pages"].append({
                    "page": p,
                    "cid": cid,
                    "part": part,
                    "subtitle": None
                })
                time.sleep(1)
                continue

            sub_json = download_subtitle_json(chosen["subtitle_url"])
            body = sub_json.get("body", [])

            json_path = OUT_DIR / f"{base}.json"
            srt_path = OUT_DIR / f"{base}.srt"

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(sub_json, f, ensure_ascii=False, indent=2)

            with open(srt_path, "w", encoding="utf-8") as f:
                f.write(json_to_srt(body))

            meta["pages"].append({
                "page": p,
                "cid": cid,
                "part": part,
                "subtitle": {
                    "lan": chosen.get("lan"),
                    "lan_doc": chosen.get("lan_doc"),
                    "subtitle_url": chosen.get("subtitle_url"),
                    "json": str(json_path).replace("\\", "/"),
                    "srt": str(srt_path).replace("\\", "/"),
                }
            })

            print(f"[ok] p{p} {part}")
            time.sleep(1)

        except Exception as e:
            print(f"[error] p{p} {part}: {e}")
            meta["pages"].append({
                "page": p,
                "cid": cid,
                "part": part,
                "error": str(e)
            })
            time.sleep(1)

    with open(OUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
