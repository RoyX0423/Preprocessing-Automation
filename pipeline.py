#!/usr/bin/env python3
"""
preprocessing-automation 流水线核心
S1 批量转换(海康私有封装→标准MP4) -> S2/S3 抽帧+质量过滤 -> S5 差分去重 -> S4 标准化
-> round 打包(每轮≤50张, 未满先补, 满了开新轮) -> manifest
执行顺序: 抽帧 -> 质量过滤(黑帧/模糊/过曝) -> 去重 -> 标准化 -> 打包
"""
import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
from PIL import Image

def _find_ffmpeg():
    """跨平台定位 ffmpeg: 环境变量 FFMPEG > 本机 ffmpeg-static > 系统 PATH"""
    env = os.environ.get("FFMPEG")
    if env and os.path.exists(env):
        return env
    is_win = sys.platform.startswith("win")
    exe = "ffmpeg.exe" if is_win else "ffmpeg"
    cands = [
        str(Path.home() / "node_modules" / "ffmpeg-static" / exe),          # ~/node_modules
        str(Path(__file__).resolve().parent / "ffmpeg" / exe),              # 项目内 ffmpeg/
        str(Path(__file__).resolve().parent / "ffmpeg-static" / exe),       # 项目内 ffmpeg-static/
    ]
    for c in cands:
        if os.path.exists(c):
            return c
    w = shutil.which("ffmpeg")
    if w:
        return w
    return None


FFMPEG = _find_ffmpeg()
VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".flv", ".ts", ".mpg", ".mpeg", ".wmv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp"}
STATE_FILE = "processed_videos.json"

# 质量过滤默认阈值(黑帧/过曝为绝对阈值, 模糊用按视频自适应的相对阈值)
BLACK_MEAN_MAX = 8.0        # 灰度均值低于此值判为黑帧
OVEREXPOSED_MEAN_MIN = 250  # 灰度均值高于此值判为过曝


@dataclass
class Config:
    interval: float = 1.0        # 抽帧间隔(秒), 静态场景1s, 动态场景建议2s
    dedup_sim: float = 97.0      # 去重阈值: 两帧 dHash 相似度≥该值即判重复丢弃(97%≈汉明≤2bit, 只并几乎相同的帧). 值越低合并越激进→输出越少; 越接近100%越接近原样保留
    blur_sens: float = 10.0      # 模糊过滤灵敏度(0-50): 该视频帧清晰度分位, 0=关闭模糊过滤
    img_blur_min: float = 400.0  # 图片模糊绝对阈值(符号保留的Laplacian方差): 单图无帧分布, 低于此值判模糊; 0=关闭; 需按实际抓拍图标定
    long_edge: int = 1280        # 长边像素
    max_kb: int = 400            # 单张上限KB
    quality_filter: bool = True  # 是否启用质量过滤(黑帧/过曝)
    jpg_quality: int = 85
    per_round: int = 50          # 每轮张数
    force: bool = False          # 强制重新处理所有文件(忽略增量记录)


@dataclass
class Stats:
    videos_total: int = 0
    videos_ok: int = 0
    videos_failed: int = 0
    videos_skipped: int = 0      # 增量跳过
    frames_raw: int = 0
    frames_filtered: int = 0     # 质量过滤掉
    frames_dedup: int = 0        # 去重吞并
    images_out: int = 0
    converted: int = 0           # S1 转换数
    copied: int = 0              # 仅转换模式: 标准 MP4 直接复制的数量
    rounds_created: int = 0
    failures: list = field(default_factory=list)  # [(file, reason)]


class Logger:
    def __init__(self, cb=None):
        self.cb = cb
        self.lines = []

    def log(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        self.lines.append(line)
        if self.cb:
            self.cb({"type": "log", "msg": line})


def _run(cmd, timeout=600):
    if cmd[0] == FFMPEG and not FFMPEG:
        raise RuntimeError("未找到 ffmpeg: 请设置环境变量 FFMPEG, 或安装 ffmpeg-static/ffmpeg 并加入 PATH")
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def is_hikvision(path):
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"IMKH"
    except OSError:
        return False


def probe_info(path):
    """返回 (duration秒, 是否含视频流); 解析 ffmpeg -i 输出"""
    r = _run([FFMPEG, "-hide_banner", "-i", str(path)], timeout=60)
    out = (r.stdout or "") + (r.stderr or "")
    m = re.search(r"Duration:\s*(\d+):(\d+):([\d.]+)", out)
    dur = 0.0
    if m:
        dur = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
    has_video = "Video:" in out
    return dur, has_video


def probe_video_codec(path):
    """返回视频编码名(小写, 如 h264/hevc/mpeg4), 解析失败返回空串"""
    r = _run([FFMPEG, "-hide_banner", "-i", str(path)], timeout=60)
    out = (r.stdout or "") + (r.stderr or "")
    m = re.search(r"Video:\s*([a-zA-Z0-9_]+)", out)
    return m.group(1).lower() if m else ""


def is_standard_h264_mp4(path):
    """已是「标准可播放 H.264 MP4」(非海康 IMKH 私有封装) → 无需重编码, 仅转换模式直接复制"""
    p = Path(path)
    if p.suffix.lower() != ".mp4" or is_hikvision(p):
        return False
    return probe_video_codec(p) == "h264"


def convert_to_mp4(src, dst, logger):
    """S1: 海康私有封装 → 标准 H.264+AAC MP4"""
    cmd = [FFMPEG, "-hide_banner", "-y", "-i", str(src),
           "-map", "0:v:0", "-map", "0:a:0?",
           "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
           "-pix_fmt", "yuv420p",
           "-c:a", "aac", "-b:a", "64k", "-ar", "8000", "-ac", "1",
           "-movflags", "+faststart", str(dst)]
    r = _run(cmd, timeout=900)
    if r.returncode != 0 or not os.path.exists(dst) or os.path.getsize(dst) < 1024:
        raise RuntimeError(f"ffmpeg转换失败: {(r.stderr or '')[-300:]}")
    d, hv = probe_info(dst)
    if d <= 0 or not hv:
        raise RuntimeError("转换后校验失败(无时长或无视频流)")
    return d


def extract_frames(video, out_dir, interval, logger):
    """S3: 按间隔抽帧为JPG"""
    fps = 1.0 / interval if interval > 0 else 1.0
    pattern = os.path.join(out_dir, "frm_%05d.jpg")
    cmd = [FFMPEG, "-hide_banner", "-y", "-i", str(video),
           "-vf", f"fps={fps:.6f}", "-q:v", "2", pattern]
    r = _run(cmd, timeout=900)
    frames = sorted(Path(out_dir).glob("frm_*.jpg"))
    if r.returncode != 0 and not frames:
        raise RuntimeError(f"抽帧失败: {(r.stderr or '')[-300:]}")
    return frames


def frame_metrics(path):
    """返回 (灰度均值, Laplacian 方差清晰度分).
    灰度均值用于黑帧/过曝判定; Laplacian 方差用于模糊判定(值越大越清晰).

    实现说明: 与 OpenCV 标准模糊检测算子 cv2.Laplacian(gray, CV_64F).var()
    (Pech-Pacheco et al., ICPR 2000 提出的对焦度量) 等价 —— 采用**符号保留**
    的拉普拉斯响应求方差. 旧实现用 PIL 8bit 滤波会把负响应截断为 0, 导致
    清晰度分系统性偏低且与内容不成比例(实测清晰帧偏低约 50%+), 已修正.
    """
    im = Image.open(path).convert("L")
    w, h = im.size
    if w > 160:
        im = im.resize((160, max(1, int(h * 160 / w))))
    a = np.asarray(im, dtype=np.float64)
    mean = float(a.mean())
    # 拉普拉斯 = 4 邻域和 - 4*中心(内部有效区域, 避免边界填充偏差, 与 cv2 近似)
    lap = (a[:-2, 1:-1] + a[2:, 1:-1] + a[1:-1, :-2] + a[1:-1, 2:]
           - 4.0 * a[1:-1, 1:-1])
    return mean, float(lap.var())


def dhash(path, size=(9, 8)):
    """dHash 感知哈希: 9x8 灰度图比较相邻像素亮度差, 返回64位整数.
    帧相似度 = 1 - 汉明距离/64, 行业标准做法(对压缩噪声/微光变化鲁棒)."""
    im = Image.open(path).convert("L").resize(size)
    px = im.load()
    bits = 0
    for y in range(size[1]):
        for x in range(size[0] - 1):
            bits = (bits << 1) | (1 if px[x, y] > px[x + 1, y] else 0)
    return bits


def hamming(a, b):
    return bin(a ^ b).count("1")


def standardize(src, dst, cfg):
    """S4: 长边统一 + JPG + 大小控制, 返回 (宽,高,大小KB)"""
    im = Image.open(src).convert("RGB")
    w, h = im.size
    if max(w, h) > cfg.long_edge:
        if w >= h:
            im = im.resize((cfg.long_edge, max(1, int(h * cfg.long_edge / w))), Image.LANCZOS)
        else:
            im = im.resize((max(1, int(w * cfg.long_edge / h)), cfg.long_edge), Image.LANCZOS)
    q = cfg.jpg_quality
    while q >= 60:
        im.save(dst, "JPEG", quality=q, optimize=True)
        if os.path.getsize(dst) <= cfg.max_kb * 1024:
            break
        q -= 10
    w2, h2 = im.size
    return w2, h2, round(os.path.getsize(dst) / 1024, 1)


def is_alias(path):
    """macOS Alias 快捷方式: 头部 book\0\0\0\0mark, 通常 988 字节"""
    try:
        with open(path, "rb") as f:
            head = f.read(16)
        return head[:4] == b"book" and b"mark" in head
    except OSError:
        return False


def _resolve_osascript(path):
    """用 Finder 解析 Alias 目标(本机一般可用; 沙箱内可能失败返回 None)"""
    try:
        r = subprocess.run(
            ["osascript", "-e",
             f'tell application "Finder" to get POSIX path of (original item of (alias file (POSIX file "{path}") as alias))'],
            capture_output=True, text=True, timeout=8)
        out = r.stdout.strip()
        if r.returncode == 0 and out.startswith("/") and os.path.exists(out):
            return out
    except Exception:
        pass
    return None


def _ivms_roots():
    """iVMS-4200 录像目录候选(跨平台)"""
    if sys.platform.startswith("win"):
        roots = [
            r"C:\Program Files\HIKVISION\iVMS-4200\Contents\MacOS\File\video",
            r"C:\Program Files (x86)\HIKVISION\iVMS-4200\Contents\MacOS\File\video",
            r"C:\Program Files\HIKVISION\iVMS-4200\File\video",
            str(Path.home() / "Documents" / "iVMS-4200" / "File" / "video"),
        ]
    else:
        roots = ["/Applications/iVMS-4200.app/Contents/MacOS/File/video"]
    return [r for r in roots if os.path.isdir(r)]


def _find_in_ivms(fname):
    """按文件名在 iVMS-4200 录像目录中搜索(本项目常见情形, 跨平台)"""
    for root in _ivms_roots():
        try:
            for p in Path(root).rglob(fname):
                if p.is_file() and not is_alias(p):
                    return str(p)
        except OSError:
            continue
    return None


def _resolve_windows_lnk(path):
    """Windows .lnk 快捷方式 → 目标路径 (PowerShell WScript.Shell)"""
    if not sys.platform.startswith("win"):
        return None
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command",
             f"$s=(New-Object -ComObject WScript.Shell).CreateShortcut('{path}'); $s.TargetPath"],
            capture_output=True, text=True, timeout=10)
        out = r.stdout.strip()
        if r.returncode == 0 and out and os.path.exists(out):
            return out
    except Exception:
        pass
    return None


def _resolve_from_bytes(path):
    """字节级兜底: 从 Alias 内嵌路径组件拼出候选, 要求存在且不是快捷方式"""
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    fname = path.name.encode("utf-8")
    i = raw.find(fname)
    if i < 0:
        return None
    runs = re.findall(rb"[\x20-\x7e\xc0-\xff][\x20-\x7e\xc0-\xff]*", raw[:i])
    parts = []
    for r in runs[-10:]:
        try:
            s = r.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if s.strip() and not s.startswith("0"):
            parts.append(s)
    for start in range(len(parts)):
        cand = Path("/" + "/".join(parts[start:]))
        if cand.is_file() and not is_alias(cand) and cand.name == path.name:
            return str(cand)
    return None


def resolve_alias(path):
    """解析快捷方式 → 真实文件路径; 非快捷方式原样返回; 失败返回 None.
    支持: macOS Alias(book/mark) 与 Windows .lnk"""
    p = Path(path)
    if not is_alias(p) and not p.suffix.lower() == ".lnk":
        return str(path)
    if is_alias(p):
        for fn, arg in ((_resolve_osascript, p), (_find_in_ivms, p.name), (_resolve_from_bytes, p)):
            target = fn(arg)
            if target:
                return target
        return None
    return _resolve_windows_lnk(p)


def find_inputs(root):
    """返回 [(真实路径, rel_dir, 展示名)], 含视频/图片/Alias, 跳过隐藏文件.
    Alias 会被解析为真实文件; 解析失败时真实路径为 None(交给 process_one 报错)"""
    root = Path(root)
    items = []
    for p in sorted(root.rglob("*")):
        if p.is_dir():
            continue
        if any(part.startswith(".") for part in p.parts):
            continue
        ext = p.suffix.lower()
        if ext not in VIDEO_EXTS and ext not in IMAGE_EXTS:
            continue
        rel_dir = str(p.parent.relative_to(root))
        if rel_dir == ".":
            rel_dir = ""
        real = resolve_alias(p)
        items.append((real, rel_dir, p.name))
    return items


def load_state(output_root, fname=STATE_FILE):
    p = Path(output_root) / fname
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(output_root, state, fname=STATE_FILE):
    Path(output_root).mkdir(parents=True, exist_ok=True)
    p = Path(output_root) / fname
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(str(tmp), str(p))


def round_no_of(dirname):
    try:
        return int(dirname.split("_")[1])
    except (IndexError, ValueError):
        return 0


def _seq_of(name):
    m = re.search(r"_(\d+)\.jpg$", name)
    return int(m.group(1)) if m else 0


def pack_rounds(output_root, per_round=50, date_str=None, stats=None):
    """把各子目录的图片按每轮 per_round 打包:
    未满的 round 先补(编号从小到大), 补满再开新 round(编号=max+1).
    文件名统一 ppe_日期_轮次_序号.jpg, .meta.json 旁车文件跟随移动.
    返回 {旧文件名: 新相对路径}"""
    date_str = date_str or time.strftime("%Y%m%d")
    mapping = {}
    # 打包单位: 输出根目录本身 + 各子目录(排除 round_* 防嵌套, 排除隐藏)
    roots = [Path(output_root)]
    roots += sorted(p for p in Path(output_root).iterdir()
                    if p.is_dir() and not p.name.startswith("round_") and not p.name.startswith("."))
    for sub in roots:
        images = sorted(p for p in sub.iterdir()
                        if p.is_file() and p.suffix.lower() in IMAGE_EXTS)
        if not images:
            continue
        rounds = sorted((p for p in sub.iterdir() if p.is_dir() and p.name.startswith("round_")),
                        key=lambda p: round_no_of(p.name))
        slots = []  # (目标round目录, 下个序号)
        for r in rounds:
            exist = sorted(r.glob("*.jpg"))
            if len(exist) < per_round:
                seq = max([_seq_of(f.name) for f in exist] or [0]) + 1
                slots.append((r, seq))
        next_no = max([round_no_of(r.name) for r in rounds] or [0]) + 1
        idx = 0
        while idx < len(images):
            if slots:
                r, seq = slots.pop(0)
            else:
                r = sub / f"round_{next_no:02d}_{date_str}"
                r.mkdir(exist_ok=True)
                seq = 1
                next_no += 1
                if stats is not None:
                    stats.rounds_created += 1
            have = len(list(r.glob("*.jpg")))
            rno = round_no_of(r.name)
            rdate = r.name.split("_")[2] if len(r.name.split("_")) > 2 else date_str
            while idx < len(images) and have < per_round:
                src = images[idx]
                new_name = f"ppe_{rdate}_{rno:02d}_{seq:04d}.jpg"
                dst = r / new_name
                if src.resolve() != dst.resolve():
                    shutil.move(str(src), str(dst))
                    meta_src = Path(str(src) + ".meta.json")
                    if meta_src.exists():
                        shutil.move(str(meta_src), str(dst) + ".meta.json")
                mapping[str(src.name)] = str(dst.relative_to(output_root))
                have += 1
                seq += 1
                idx += 1
    return mapping


def rebuild_manifest(output_root):
    """从产物树 + .meta.json 重建 manifest(不依赖内存状态, 可增量/崩溃恢复)"""
    rows = []
    for p in sorted(Path(output_root).rglob("*.jpg")):
        meta_p = Path(str(p) + ".meta.json")
        if not meta_p.exists():
            continue
        try:
            meta = json.loads(meta_p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        rows.append({
            "图片": str(p.relative_to(output_root)),
            "来源视频": meta.get("来源视频", ""),
            "子目录": meta.get("子目录", ""),
            "轮次": p.parent.name,
            "帧序号": meta.get("帧序号", ""),
            "宽": meta.get("宽", ""), "高": meta.get("高", ""),
            "大小KB": meta.get("大小KB", ""), "清晰度分": meta.get("清晰度分", ""),
        })
    with open(Path(output_root) / "manifest.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["图片", "来源视频", "子目录", "轮次", "帧序号", "宽", "高", "大小KB", "清晰度分"])
        w.writeheader()
        w.writerows(rows)
    return rows


def process_one(src_path, rel_dir, display_name, out_root, work_root, cfg, stats, logger, progress_cb, img_hashes):
    """处理单个输入文件(视频/图片/Alias解析后的真实文件).
    src_path=None 表示 Alias 解析失败. img_hashes: 已保留图片的 dHash 列表(图片全局去重用).
    返回 True=成功"""
    name = display_name
    base = Path(display_name).stem
    if src_path is None:
        stats.videos_failed += 1
        msg = "快捷方式(Alias)无法解析到真实文件, 请确认 iVMS-4200 录像仍存在"
        stats.failures.append((str(Path(rel_dir) / name) if rel_dir else name, msg))
        logger.log(f"失败 {name}: {msg}")
        return False
    src_path = Path(src_path)
    out_dir = Path(out_root) / rel_dir if rel_dir else Path(out_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    work = Path(work_root)
    work.mkdir(parents=True, exist_ok=True)
    tmp_video = work / f"{base}.std.mp4"
    tmp_frames = work / f"{base}_frames"
    tmp_frames.mkdir(exist_ok=True)

    rows = []
    try:
        # ---- S1 转换(仅海康私有封装) ----
        is_img = src_path.suffix.lower() in IMAGE_EXTS
        video_path = src_path
        if not is_img:
            if is_hikvision(src_path):
                progress_cb({"type": "stage", "msg": f"S1 转换 {name}"})
                convert_to_mp4(src_path, tmp_video, logger)
                video_path = tmp_video
                stats.converted += 1
                logger.log(f"S1 转换完成: {name}")
            else:
                d, hv = probe_info(src_path)
                if not hv:
                    raise RuntimeError("未检测到视频流")

        # ---- S3 抽帧 ----
        if is_img:
            frames = [src_path]
        else:
            progress_cb({"type": "stage", "msg": f"S3 抽帧 {name}"})
            frames = extract_frames(video_path, tmp_frames, cfg.interval, logger)
        stats.frames_raw += len(frames)

        # ---- 质量过滤: 视频模糊用按视频自适应的相对阈值; 图片用绝对阈值(无帧分布可依赖) ----
        feats = [(fp, *frame_metrics(fp), dhash(fp)) for fp in frames]  # (fp, 均值, 清晰度分, dHash)
        blur_min = 0.0
        if (not is_img) and cfg.quality_filter and cfg.blur_sens > 0 and feats:
            # 相对阈值: 取本片内清晰度分的 blur_sens% 分位, 低于它的判为片内最糊的一批帧
            vs = sorted(v for _, _, v, _ in feats)
            idx = min(len(vs) - 1, max(0, int(len(vs) * cfg.blur_sens / 100)))
            blur_min = vs[idx]
        kept = []
        for fp, mean, lv, h in feats:
            if cfg.quality_filter:
                if mean < BLACK_MEAN_MAX or mean > OVEREXPOSED_MEAN_MIN:
                    stats.frames_filtered += 1
                    continue
                if is_img:
                    # 图片: 绝对阈值(符号保留尺度, 需按真实抓拍图标定; 0=关闭)
                    if cfg.img_blur_min > 0 and lv < cfg.img_blur_min:
                        stats.frames_filtered += 1
                        continue
                elif cfg.blur_sens > 0 and lv < blur_min:
                    stats.frames_filtered += 1
                    continue
            kept.append((fp, round(lv, 1), h))

        # ---- S5 dHash 感知哈希去重(视频: 相邻保留帧; 图片: 与已保留集合全局比较) ----
        # 映射: 相似度% → 汉明距离 max_hd = round(64*(1-sim/100)); 92%≈≤5bit.
        # 注意方向: sim 越低 → max_hd 越大 → 合并越激进 → 输出越少; sim 越高越接近原样保留.
        progress_cb({"type": "stage", "msg": f"S5 去重 {name}"})
        max_hd = max(0, round(64 * (1 - cfg.dedup_sim / 100)))
        final = []
        prev_h = None
        for fp, score, h in kept:
            if is_img:
                if any(hamming(hh, h) <= max_hd for hh in img_hashes):
                    stats.frames_dedup += 1
                    continue
                img_hashes.append(h)
            elif prev_h is not None and hamming(prev_h, h) <= max_hd:
                stats.frames_dedup += 1
                continue
            prev_h = h
            final.append((fp, score))

        # ---- S4 标准化输出(暂存子目录根, 之后统一打包进 round) ----
        seq_start = len(list(out_dir.glob(f"{base}_f*.jpg")))
        for i, (fp, score) in enumerate(final, 1):
            out_name = f"{base}_f{seq_start + i:04d}.jpg"
            w, h, kb = standardize(fp, out_dir / out_name, cfg)
            stats.images_out += 1
            # 旁车文件: 记录溯源元数据, manifest 由它重建(可增量/崩溃恢复)
            meta = {"来源视频": name, "子目录": rel_dir, "帧序号": seq_start + i,
                    "宽": w, "高": h, "大小KB": kb, "清晰度分": score}
            (out_dir / (out_name + ".meta.json")).write_text(
                json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        stats.videos_ok += 1
        logger.log(f"完成 {name}: 抽帧{len(frames)} 过滤后保留{len(final)} 输出{len(final)}")
        return True
    except Exception as e:
        stats.videos_failed += 1
        stats.failures.append((str(Path(rel_dir) / name) if rel_dir else name, str(e)))
        logger.log(f"失败 {name}: {e}")
        return False


def run_pipeline(input_root, output_root, cfg, progress_cb=None):
    """主入口. progress_cb(dict) 接收进度事件. 返回 stats"""
    progress_cb = progress_cb or (lambda e: None)
    logger = Logger(progress_cb)
    stats = Stats()
    input_root = Path(input_root)
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    # 中间产物放系统临时目录, 避免在个人目录批量删文件; 结束后整体清理
    work_root = Path(tempfile.mkdtemp(prefix="ppa_"))

    items = find_inputs(input_root)
    # 增量: 跳过已成功处理的(rel_dir, 展示名, size)
    state = load_state(output_root) if not cfg.force else {}
    todo = []
    for real, rel_dir, name in items:
        size = 0
        if real is not None:
            try:
                size = Path(real).stat().st_size
            except OSError:
                pass
        if state.get(rel_dir, {}).get(name) == size:
            stats.videos_skipped += 1
            continue
        todo.append((real, rel_dir, name))
    stats.videos_total = len(todo)
    progress_cb({"type": "init", "total": len(todo)})
    logger.log(f"输入 {len(items)} 个, 本次待处理 {len(todo)} 个(增量跳过 {stats.videos_skipped}), "
               f"参数: 间隔{cfg.interval}s 去重相似度{cfg.dedup_sim}% 模糊灵敏度{cfg.blur_sens} "
               f"长边{cfg.long_edge}px 每轮{cfg.per_round}张")

    kept_img_hashes = []  # 已保留图片的 dHash 集合(图片全局去重, 与视频相邻帧去重互不影响)
    for idx, (real, rel_dir, name) in enumerate(todo, 1):
        progress_cb({"type": "file", "idx": idx, "total": len(todo), "name": name})
        ok = process_one(real, rel_dir, name, output_root, work_root, cfg, stats, logger, progress_cb,
                         kept_img_hashes)
        if ok:
            size = Path(real).stat().st_size
            state.setdefault(rel_dir, {})[name] = size
            save_state(output_root, state)

    # ---- round 打包(多退少补) ----
    progress_cb({"type": "stage", "msg": "打包 round(每轮≤50张, 未满先补)"})
    stats.rounds_created = 0
    pack_rounds(output_root, per_round=cfg.per_round, stats=stats)
    logger.log(f"round 打包完成: 新开 {stats.rounds_created} 个 round 目录")

    # ---- manifest 从产物树重建(增量/崩溃安全) ----
    rows = rebuild_manifest(output_root)
    logger.log(f"manifest 重建完成: {len(rows)} 条记录")

    logger.log(f"全部完成: 待处理{stats.videos_ok}/{stats.videos_total} 转换{stats.converted} "
               f"原始帧{stats.frames_raw} 质量过滤{stats.frames_filtered} 去重吞并{stats.frames_dedup} "
               f"输出图片{stats.images_out} 失败{stats.videos_failed}")
    if stats.failures:
        logger.log("失败清单: " + "; ".join(f"{n}({r})" for n, r in stats.failures))
    with open(output_root / "pipeline.log", "w", encoding="utf-8") as f:
        f.write("\n".join(logger.lines))

    shutil.rmtree(work_root, ignore_errors=True)
    progress_cb({"type": "done", "stats": {
        "videos_total": stats.videos_total, "videos_ok": stats.videos_ok,
        "videos_failed": stats.videos_failed, "videos_skipped": stats.videos_skipped,
        "converted": stats.converted, "frames_raw": stats.frames_raw,
        "frames_filtered": stats.frames_filtered, "frames_dedup": stats.frames_dedup,
        "images_out": stats.images_out, "rounds_created": stats.rounds_created,
        "failures": stats.failures,
    }})
    return stats


CONVERT_STATE_FILE = "converted_videos.json"


def run_convert(input_root, output_root, force=False, progress_cb=None):
    """仅转换模式: 输入视频 → 标准 H.264+AAC MP4(保留子目录结构), 不做抽帧/去重/打包.

    - 海康 IMKH 私有封装 / 非标准视频: ffmpeg 重编码为标准 H.264+AAC MP4
    - 已是标准 H.264 MP4: 直接复制(不重转, 无损且秒完成)
    - 增量: converted_videos.json 按 (子目录, 文件名, 大小) 记录, 重跑自动跳过
    - 图片输入自动忽略; Alias 快捷方式自动解析(逻辑与完整流水线一致)"""
    progress_cb = progress_cb or (lambda e: None)
    logger = Logger(progress_cb)
    stats = Stats()
    input_root = Path(input_root)
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    items = find_inputs(input_root)
    # 仅转换模式只处理视频, 图片忽略
    items = [it for it in items if Path(it[2]).suffix.lower() in VIDEO_EXTS]

    # 增量: 跳过已成功处理的(rel_dir, 展示名, size)
    state = load_state(output_root, CONVERT_STATE_FILE) if not force else {}
    todo = []
    for real, rel_dir, name in items:
        size = 0
        if real is not None:
            try:
                size = Path(real).stat().st_size
            except OSError:
                pass
        if state.get(rel_dir, {}).get(name) == size:
            stats.videos_skipped += 1
            continue
        todo.append((real, rel_dir, name))
    stats.videos_total = len(todo)
    progress_cb({"type": "init", "total": len(todo)})
    logger.log(f"输入视频 {len(items)} 个, 本次待处理 {len(todo)} 个(增量跳过 {stats.videos_skipped})")

    for idx, (real, rel_dir, name) in enumerate(todo, 1):
        progress_cb({"type": "file", "idx": idx, "total": len(todo), "name": name})
        rel_out = Path(output_root) / rel_dir if rel_dir else Path(output_root)
        rel_out.mkdir(parents=True, exist_ok=True)
        dst = rel_out / (Path(name).stem + ".mp4")
        try:
            if real is None:
                raise RuntimeError("快捷方式(Alias)无法解析到真实文件, 请确认 iVMS-4200 录像仍存在")
            src = Path(real)
            if is_standard_h264_mp4(src):
                # 已是标准 H.264 MP4: 直接复制, 避免无谓重编码的时间与画质损失
                progress_cb({"type": "stage", "msg": f"复制标准 MP4 {name}"})
                shutil.copy2(str(src), str(dst))
                stats.copied += 1
                logger.log(f"复制标准 MP4: {name}")
            else:
                # 海康私有封装(IMKH) 或 其它非标准封装 → 重编码为标准 H.264+AAC MP4
                progress_cb({"type": "stage", "msg": f"S1 转换 {name}"})
                tmp = dst.with_name(dst.stem + ".conv.tmp.mp4")
                if tmp.exists():
                    tmp.unlink()
                try:
                    convert_to_mp4(src, tmp, logger)
                    shutil.move(str(tmp), str(dst))
                finally:
                    if tmp.exists():
                        tmp.unlink()
                stats.converted += 1
                logger.log(f"转换完成: {name}")
            stats.videos_ok += 1
            state.setdefault(rel_dir, {})[name] = Path(src).stat().st_size
            save_state(output_root, state, CONVERT_STATE_FILE)
        except Exception as e:
            stats.videos_failed += 1
            stats.failures.append((str(Path(rel_dir) / name) if rel_dir else name, str(e)))
            logger.log(f"失败 {name}: {e}")

    logger.log(f"全部完成: 成功{stats.videos_ok}/{stats.videos_total} 私有转码{stats.converted} "
               f"标准复制{stats.copied} 增量跳过{stats.videos_skipped} 失败{stats.videos_failed}")
    if stats.failures:
        logger.log("失败清单: " + "; ".join(f"{n}({r})" for n, r in stats.failures))
    with open(output_root / "pipeline.log", "w", encoding="utf-8") as f:
        f.write("\n".join(logger.lines))

    progress_cb({"type": "done", "stats": {
        "videos_total": stats.videos_total, "videos_ok": stats.videos_ok,
        "videos_failed": stats.videos_failed, "videos_skipped": stats.videos_skipped,
        "converted": stats.converted, "copied": stats.copied,
        "failures": stats.failures,
    }})
    return stats


def main():
    ap = argparse.ArgumentParser(description="海康监控视频 -> 预处理图片数据集(round打包)")
    ap.add_argument("--input", required=True, help="输入文件夹(可含子目录)")
    ap.add_argument("--output", required=True, help="输出文件夹")
    ap.add_argument("--convert-only", action="store_true",
                    help="仅转换视频为标准 H.264+AAC MP4(海康私有转码/标准MP4直接复制), 不做抽帧处理")
    ap.add_argument("--interval", type=float, default=1.0, help="抽帧间隔秒, 默认1")
    ap.add_argument("--dedup-sim", type=float, default=97.0,
                    help="去重阈值%(0-100): 相似度≥该值即判重复; 默认97(≈汉明≤2bit, 只并几乎相同帧; 值越低并越多)")
    ap.add_argument("--blur-sens", type=float, default=10.0, help="模糊过滤灵敏度%(0-50), 0=关闭, 默认10")
    ap.add_argument("--img-blur-min", type=float, default=400.0,
                    help="图片模糊绝对阈值(符号保留Laplacian方差), 0=关闭, 默认400(需按抓拍图标定)")
    ap.add_argument("--size", type=int, default=1280, help="长边像素, 默认1280")
    ap.add_argument("--per-round", type=int, default=50, help="每轮张数, 默认50")
    ap.add_argument("--no-filter", action="store_true", help="关闭质量过滤(黑帧/过曝)")
    ap.add_argument("--force", action="store_true", help="忽略增量记录, 重新处理所有文件")
    args = ap.parse_args()

    cfg = Config(interval=args.interval, dedup_sim=args.dedup_sim, blur_sens=args.blur_sens,
                 img_blur_min=args.img_blur_min,
                 long_edge=args.size, quality_filter=not args.no_filter,
                 per_round=args.per_round, force=args.force)
    if args.convert_only:
        run_convert(args.input, args.output, force=args.force,
                    progress_cb=lambda e: print(e.get("msg") or e) if e.get("type") in ("log", "done") else None)
    else:
        run_pipeline(args.input, args.output, cfg,
                     progress_cb=lambda e: print(e.get("msg") or e) if e.get("type") in ("log", "done") else None)


if __name__ == "__main__":
    main()
