#!/usr/bin/env python3
"""
preprocessing-automation Web 后端
拖拽文件夹上传 或 指定本地路径 -> 后台跑 pipeline -> 进度轮询 -> 打包下载
历史任务持久化到 output/jobs.json, 重启不丢, 可随时追溯/下载
"""
import csv
import json
import os
import shutil
import threading
import time
import uuid
from pathlib import Path

import requests
from flask import Flask, jsonify, request, send_file, send_from_directory

from pipeline import IMAGE_EXTS, Config, run_pipeline, run_convert

BASE = Path(__file__).resolve().parent
UPLOAD_DIR = BASE / "uploads"
OUTPUT_DIR = BASE / "output"
HISTORY_FILE = OUTPUT_DIR / "jobs.json"
LS_CONFIG_FILE = BASE / "ls_config.json"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

app = Flask(__name__, static_folder="static", static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = None

JOBS = {}
LOCK = threading.Lock()


def _load_history():
    try:
        return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_history():
    HISTORY_FILE.write_text(json.dumps(JOBS, ensure_ascii=False, indent=1), encoding="utf-8")


# 启动时恢复历史任务(仅保留概要, 状态置为历史完成/错误)
for _jid, _rec in _load_history().items():
    if _rec.get("status") in ("done", "error"):
        _rec["status"] = _rec["status"]
    JOBS[_jid] = _rec


def new_job(mode, input_path="", output_dir=None):
    jid = uuid.uuid4().hex[:8]
    with LOCK:
        JOBS[jid] = {
            "id": jid, "mode": mode, "status": "running",
            "total": 0, "idx": 0, "current": "",
            "stage": "", "logs": [], "stats": None,
            "input": input_path, "output": str(output_dir) if output_dir else "",
            "created": time.time(),
        }
    _save_history()
    return jid


def make_cb(jid):
    def cb(e):
        with LOCK:
            j = JOBS.get(jid)
            if not j:
                return
            t = e.get("type")
            if t == "init":
                j["total"] = e.get("total", 0)
            elif t == "file":
                j["idx"] = e.get("idx", 0)
                j["current"] = e.get("name", "")
            elif t == "stage":
                j["stage"] = e.get("msg", "")
            elif t == "log":
                j["logs"].append(e.get("msg", ""))
                j["logs"] = j["logs"][-200:]
            elif t == "done":
                j["stats"] = e.get("stats")
                j["status"] = "done"
                _save_history()
    return cb


def run_job(jid, input_root, output_root, cfg):
    try:
        run_pipeline(input_root, output_root, cfg, progress_cb=make_cb(jid))
    except Exception as e:
        with LOCK:
            JOBS[jid]["status"] = "error"
            JOBS[jid]["logs"].append(f"流水线异常: {e}")
            _save_history()


def run_convert_job(jid, input_root, output_root, force=False):
    """仅转换模式的后台线程入口(事件格式与完整流水线一致, 复用 make_cb)"""
    try:
        run_convert(input_root, output_root, force=force, progress_cb=make_cb(jid))
    except Exception as e:
        with LOCK:
            JOBS[jid]["status"] = "error"
            JOBS[jid]["logs"].append(f"转换异常: {e}")
            _save_history()


def parse_cfg(data):
    def fnum(key, default, cast=float):
        try:
            return cast(data.get(key, default))
        except (TypeError, ValueError):
            return default
    return Config(
        interval=max(0.2, fnum("interval", 1.0)),
        dedup_sim=min(100.0, max(0.0, fnum("dedup_sim", 97.0))),
        blur_sens=min(50.0, max(0.0, fnum("blur_sens", 10.0))),
        img_blur_min=min(2000.0, max(0.0, fnum("img_blur_min", 400.0))),
        long_edge=max(320, fnum("size", 1280, int)),
        quality_filter=data.get("filter", "on") != "off",
        per_round=max(1, fnum("per_round", 50, int)),
    )


def job_out_root(jid):
    j = JOBS.get(jid, {})
    if j.get("mode") == "merge":
        # 合并筛选会话: 无实体图片, 输出目录只用于存放 .review 筛选状态
        d = OUTPUT_DIR / ".merge" / jid
        d.mkdir(parents=True, exist_ok=True)
        return d
    out = j.get("output")
    if out and os.path.isdir(out):
        return Path(out)
    fallback = OUTPUT_DIR / jid
    return fallback if fallback.is_dir() else None


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    with LOCK:
        items = sorted(JOBS.values(), key=lambda j: j.get("created", 0), reverse=True)
        brief = [{
            "id": j["id"], "mode": j["mode"], "status": j["status"],
            "created": j.get("created"), "input": j.get("input", ""),
            "output": j.get("output", ""), "stage": j.get("stage", ""),
            "current": j.get("current", ""),
            "stats": j.get("stats"),
        } for j in items]
    return jsonify(items=brief)


@app.route("/api/jobs", methods=["DELETE"])
def clear_jobs():
    """一键清空历史任务列表(仅清 jobs.json 记录, 不动磁盘上的输出文件)"""
    with LOCK:
        JOBS.clear()
    _save_history()
    return jsonify({"ok": True, "cleared": True})


@app.route("/api/jobs", methods=["POST"])
def create_upload_job():
    files = request.files.getlist("files")
    relpaths = request.form.get("relpaths", "[]")
    try:
        relpaths = json.loads(relpaths)
    except ValueError:
        relpaths = []
    if not files:
        return jsonify({"error": "没有收到文件"}), 400

    jid = new_job("upload")
    src_root = UPLOAD_DIR / jid
    for i, f in enumerate(files):
        rel = relpaths[i] if i < len(relpaths) else f.filename
        rel = str(rel).replace("\\", "/").lstrip("/")
        rel = "/".join(p for p in rel.split("/") if p not in ("", ".", ".."))
        if not rel:
            rel = f.filename
        dst = src_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        f.save(dst)

    cfg = parse_cfg(request.form)
    out_root = OUTPUT_DIR / jid
    with LOCK:
        JOBS[jid]["output"] = str(out_root)
        _save_history()
    threading.Thread(target=run_job, args=(jid, src_root, out_root, cfg), daemon=True).start()
    return jsonify({"job_id": jid})


@app.route("/api/jobs/images", methods=["POST"])
def create_image_job():
    """上传图片: 保存到 uploads/<jid>/ 后跑完整预处理流水线,
    自动获得 黑帧/过曝过滤 + 标准化(长边/压缩) + round打包 + manifest/meta 溯源,
    交付结构与视频任务完全一致; 复用现有下载/查看/筛选接口"""
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "没有收到图片"}), 400

    jid = new_job("image")
    src_root = UPLOAD_DIR / jid
    src_root.mkdir(parents=True, exist_ok=True)
    saved, seen = 0, set()
    for f in files:
        name = Path(f.filename or "").name  # 只取文件名, 防路径穿越
        ext = Path(name).suffix.lower()
        if not name or ext not in IMAGE_EXTS:
            continue
        if name in seen:  # 重名自动加序号, 避免覆盖
            stem, suffix = Path(name).stem, Path(name).suffix
            i = 2
            while f"{stem}_{i}{suffix}" in seen:
                i += 1
            name = f"{stem}_{i}{suffix}"
        seen.add(name)
        f.save(src_root / name)
        saved += 1
    if not saved:
        return jsonify({"error": "没有有效的图片文件(支持 jpg/jpeg/png/bmp)"}), 400

    cfg = parse_cfg(request.form)
    out_root = OUTPUT_DIR / jid
    with LOCK:
        JOBS[jid]["output"] = str(out_root)
        _save_history()
    threading.Thread(target=run_job, args=(jid, src_root, out_root, cfg), daemon=True).start()
    return jsonify({"job_id": jid})


@app.route("/api/jobs/local", methods=["POST"])
def create_local_job():
    data = request.get_json(force=True) or {}
    path = str(data.get("path", "")).strip()
    if not path or not os.path.isdir(path):
        return jsonify({"error": f"路径不存在或不是文件夹: {path}"}), 400
    # 输出建在输入母文件夹旁: <输入名>_预处理结果, 增量补round
    src = Path(path)
    out_root = src.parent / (src.name + "_预处理结果")
    jid = new_job("local", input_path=path, output_dir=out_root)
    cfg = parse_cfg(data)
    threading.Thread(target=run_job, args=(jid, path, out_root, cfg), daemon=True).start()
    return jsonify({"job_id": jid})


# ============================================================
# 仅转换模式(标准 MP4): 海康私有封装重编码 / 标准H.264 MP4直接复制
# mode = 'convert'(拖拽上传) | 'convert_local'(本机路径)
# ============================================================

@app.route("/api/jobs/convert", methods=["POST"])
def create_convert_upload_job():
    """仅转换模式(拖拽上传): 保存视频到 uploads/<jid>, 输出转好的 MP4 到 output/<jid>"""
    files = request.files.getlist("files")
    relpaths = request.form.get("relpaths", "[]")
    try:
        relpaths = json.loads(relpaths)
    except ValueError:
        relpaths = []
    if not files:
        return jsonify({"error": "没有收到文件"}), 400

    jid = new_job("convert")
    src_root = UPLOAD_DIR / jid
    for i, f in enumerate(files):
        rel = relpaths[i] if i < len(relpaths) else f.filename
        rel = str(rel).replace("\\", "/").lstrip("/")
        rel = "/".join(p for p in rel.split("/") if p not in ("", ".", ".."))
        if not rel:
            rel = f.filename
        dst = src_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        f.save(dst)

    out_root = OUTPUT_DIR / jid
    with LOCK:
        JOBS[jid]["output"] = str(out_root)
        _save_history()
    force = request.form.get("force", "off") == "on"
    threading.Thread(target=run_convert_job, args=(jid, src_root, out_root, force), daemon=True).start()
    return jsonify({"job_id": jid})


@app.route("/api/jobs/convert-local", methods=["POST"])
def create_convert_local_job():
    """仅转换模式(本机路径): 输出建在输入母文件夹旁: <输入名>_转码MP4"""
    data = request.get_json(force=True) or {}
    path = str(data.get("path", "")).strip()
    if not path or not os.path.isdir(path):
        return jsonify({"error": f"路径不存在或不是文件夹: {path}"}), 400
    src = Path(path)
    out_root = src.parent / (src.name + "_转码MP4")
    jid = new_job("convert_local", input_path=path, output_dir=out_root)
    threading.Thread(target=run_convert_job,
                     args=(jid, path, out_root, bool(data.get("force"))), daemon=True).start()
    return jsonify({"job_id": jid})


# ============================================================
# 合并筛选会话: 把多个历史任务的保留图片合到一起统一人工筛选
# 不复制图片(只记录引用与筛选标记), 状态存 output/.merge/<jid>/.review/
# 图片 key 统一为 '<子任务jid>/<输出内相对路径>', 保证跨任务不重名
# ============================================================

def merge_children(j):
    """该 merge 任务的有效子任务 [(child_jid, child_out_root)], 跳过已删除/无输出目录的"""
    out = []
    for cid in (j.get("children") or []):
        c = JOBS.get(cid)
        if not c or c.get("mode") == "merge":
            continue
        croot = job_out_root(cid)
        if croot and croot.is_dir():
            out.append((cid, croot))
    return out


def merge_pool_keys(j):
    """合并图片池(带状态的 key 列表): 各子任务保留池(排除子任务已 drop)的相对路径并集"""
    keys = []
    for cid, croot in merge_children(j):
        dropped = {k for k, v in _review_state(croot).items() if v == "drop"}
        for p in sorted(croot.rglob("*.jpg")):
            rel = str(p.relative_to(croot))
            if rel not in dropped:
                keys.append(f"{cid}/{rel}")
    return keys


def _read_meta_json(meta_p):
    try:
        return json.loads(meta_p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


@app.route("/api/jobs/merge", methods=["POST"])
def create_merge_job():
    """从多个已完成的 history 任务创建合并筛选会话(立即完成, 无后台任务).
    子任务中已人工剔除(drop)的图片不参与合并; 合并会话本身的筛选标记独立保存."""
    data = request.get_json(force=True) or {}
    job_ids = [str(x).strip() for x in (data.get("job_ids") or []) if str(x).strip()]
    if not job_ids:
        return jsonify({"error": "请至少选择一个任务"}), 400
    if len(job_ids) < 2:
        return jsonify({"error": "合并筛选至少需要 2 个任务"}), 400
    with LOCK:
        seen, bad = [], []
        for cid in job_ids:
            j = JOBS.get(cid)
            if not j:
                bad.append(f"{cid}(不存在)")
            elif j.get("mode") == "merge":
                bad.append(f"{cid}(已是合并会话)")
            elif j.get("status") != "done" and j.get("status") != "error":
                bad.append(f"{cid}(未完成)")
            else:
                seen.append(cid)
    if bad:
        return jsonify({"error": "存在不可合并的任务: " + "; ".join(bad)}), 400
    if len(seen) < 2:
        return jsonify({"error": "可合并的已完成任务不足 2 个"}), 400

    jid = uuid.uuid4().hex[:8]
    pseudo = {"mode": "merge", "children": seen}
    n_img = len(merge_pool_keys(pseudo))
    with LOCK:
        JOBS[jid] = {
            "id": jid, "mode": "merge", "status": "done",
            "total": 0, "idx": 0, "current": "", "stage": "",
            "logs": [f"[{time.strftime('%H:%M:%S')}] 合并筛选会话创建: {len(seen)} 个任务, 图片池 {n_img} 张(子任务已剔除的不参与)"],
            "stats": {"images_out": n_img, "children": seen},
            "input": "合并筛选: " + " + ".join(seen),
            "output": "", "created": time.time(), "children": seen,
        }
        _save_history()
    return jsonify({"job_id": jid, "images": n_img, "children": seen})


@app.route("/api/jobs/<jid>")
def job_status(jid):
    with LOCK:
        j = JOBS.get(jid)
        if not j:
            return jsonify({"error": "任务不存在"}), 404
        return jsonify(j)


def _dir_latest_mtime(root):
    """目录内最新文件 mtime, 用于判断 zip 是否需要重新打包"""
    mt = 0.0
    for dp, _, fns in os.walk(root):
        for fn in fns:
            try:
                mt = max(mt, os.path.getmtime(os.path.join(dp, fn)))
            except OSError:
                pass
    return mt


def _make_clean_zip(out_root, zip_path):
    """打包输出目录为下载 zip, 只含「可交付内容」, 排除内部过程文件:
    - 隐藏/状态目录(.review .merge 等, 筛选标记/合并会话)
    - 每张图的 *.meta.json 旁车溯源(其信息已并入 manifest.csv, 交付不需要)
    - processed_videos.json / converted_videos.json(增量记录)
    - pipeline.log(内部日志)
    zip 内保留: 图片(jpg)/视频(mp4) + manifest.csv"""
    import zipfile
    out_root = Path(out_root)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for dp, dns, fns in os.walk(out_root):
            dns[:] = [d for d in dns if not d.startswith(".")]
            for fn in sorted(fns):
                if fn.endswith(".meta.json"):
                    continue
                if fn in ("processed_videos.json", "converted_videos.json", "pipeline.log"):
                    continue
                fp = Path(dp) / fn
                zf.write(str(fp), fp.relative_to(out_root).as_posix())


@app.route("/api/jobs/<jid>/download")
def job_download(jid):
    j = JOBS.get(jid, {})
    if j.get("mode") == "merge":
        return jsonify({"error": "合并筛选任务请使用筛选视图里的「导出保留集 ZIP」"}), 400
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在(可能已被清理), 无法打包下载"}), 404
    zip_path = OUTPUT_DIR / f"{jid}.zip"
    try:
        # 不存在 或 输出有新变化(增量补round后) → 重新打包
        need = not zip_path.exists()
        if not need:
            try:
                need = zip_path.stat().st_mtime < _dir_latest_mtime(out_root)
            except OSError:
                need = True
        if need:
            _make_clean_zip(out_root, zip_path)
    except Exception as e:
        return jsonify({"error": f"打包失败: {e}"}), 500
    dname = (f"转码MP4_{jid}.zip" if j.get("mode") in ("convert", "convert_local")
             else f"预处理结果_{jid}.zip")
    return send_file(zip_path, as_attachment=True, download_name=dname)


@app.route("/api/jobs/<jid>/files")
def job_files(jid):
    j = JOBS.get(jid, {})
    if j.get("mode") == "merge":
        return jsonify({"files": [], "count": 0})
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"files": [], "count": 0})
    exts = (".mp4",) if j.get("mode") in ("convert", "convert_local") else (".jpg",)
    files = []
    for p in sorted(out_root.rglob("*")):
        if p.is_file() and p.suffix.lower() in exts:
            files.append(str(p.relative_to(out_root)))
    return jsonify({"files": files, "count": len(files)})


# ============================================================
# 筛选(Review) + 上传 Label Studio
# 状态持久化在输出目录 .review/state.json, 不动源 jpg/meta 文件
# ============================================================

def _review_state(out_root):
    sf = Path(out_root) / ".review" / "state.json"
    try:
        return json.loads(sf.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _ls_read_config():
    try:
        return json.loads(LS_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _update_ls_token(new_token):
    """把新 token 写回本地配置(防御性).
    LS 1.23 的 /api/token/refresh 默认不返回新 refresh token, 此函数通常不会被触发;
    仅当未来版本响应里真带 refresh 字段(启用轮换)时才保存, 避免旧 token 作废后 config 留存脏值."""
    if not new_token:
        return
    cfg = _ls_read_config()
    if cfg.get("token") != new_token:
        cfg["token"] = new_token
        LS_CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")


def _ls_exchange_pat(url, token):
    """PAT(JWT refresh token) → 短期 access token (约5分钟有效):
    POST /api/token/refresh {"refresh": <PAT>} → {"access": <token>}
    LS 1.23 该端点是标准 simplejwt 实现: 不轮换、不黑名单旧 refresh, 同一 PAT 可反复换 access.
    (LS 的轮换/黑名单走 /api/token/rotate/, 需登录会话, 外部工具用不到)
    若未来版本响应额外带 refresh 字段, 仍写回配置(防御性, 无副作用).
    返回 (access 或 None, 失败详情)"""
    try:
        r = requests.post(f"{url}/api/token/refresh", timeout=10,
                          headers={"Content-Type": "application/json"},
                          json={"refresh": token})
        if r.status_code < 400:
            data = r.json() or {}
            acc = data.get("access")
            new_refresh = data.get("refresh")
            if new_refresh:
                _update_ls_token(new_refresh)  # 防御: 仅当 LS 真返回新 refresh 才保存
            if acc:
                return acc, None
            return None, f"refresh 返回成功但无 access 字段: {r.text[:200]}"
        return None, f"refresh 失败(HTTP {r.status_code}): {r.text[:200]}"
    except requests.RequestException as e:
        return None, f"refresh 连接失败: {e}"


def _ls_auth(url, token):
    """探测可用鉴权方式, 返回 (headers, mode, None) 或 (None, None, 错误信息)
    mode: 'legacy' → Authorization: Token <token> 直接可用
          'pat'    → PAT 是 JWT refresh token, 必须先换短期 access token 再用 Bearer"""
    conn_err = None
    # 1) Legacy token: 直接可用
    try:
        r = requests.get(f"{url}/api/projects/", timeout=10,
                         headers={"Authorization": f"Token {token}"})
        if r.status_code < 400:
            return {"Authorization": f"Token {token}"}, "legacy", None
        if r.status_code >= 400 and r.status_code not in (401, 403):
            return None, None, f"Label Studio 返回异常状态(HTTP {r.status_code}), 请检查服务地址是否正确"
    except requests.RequestException as e:
        conn_err = e
    # 2) PAT: 换 access token 后再 Bearer
    access, pat_detail = _ls_exchange_pat(url, token)
    if access:
        return {"Authorization": f"Bearer {access}"}, "pat", None
    # 3) 都不行
    if conn_err is not None:
        return None, None, f"无法连接 Label Studio: {conn_err}"
    return None, None, _ls_token_hint() + (f" | PAT refresh 详情: {pat_detail}" if pat_detail else "")


def _ls_token_hint():
    return ("Token 无效(HTTP 401). LS 1.23 的 API Token 列表只显示「截断版」(header.payload, 缺签名段), "
            "复制这种 token 用不了. 请改: ① Account & Settings → Personal Access Token → Create New Token, "
            "在弹出的创建成功框里一次性完整复制(仅显示这一次); "
            "② 若组织设置里启用了 Legacy API Token(默认关闭), 复制它最省事(不走 JWT). "
            "已生效的 token 本工具会自动记住复用, 无需反复新建 —— 只有你在 LS 界面 revoke/重建后旧 token 才会立即失效.")


# 新建项目时的基础标注配置(图片框选), 上传后可在 LS 界面调整标签类别
_LS_LABEL_CONFIG = (
    '<View>'
    '<Image name="image" value="$image"/>'
    '<RectangleLabels name="label" toName="image">'
    '<Label value="object" background="#FF0000"/>'
    '</RectangleLabels>'
    '</View>'
)


def _save_review_state(out_root, state):
    d = Path(out_root) / ".review"
    d.mkdir(exist_ok=True)
    (d / "state.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


@app.route("/api/ls/config", methods=["GET"])
def ls_config_get():
    try:
        return jsonify(json.loads(LS_CONFIG_FILE.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return jsonify({})


@app.route("/api/ls/config", methods=["POST"])
def ls_config_save():
    """保存/更新 Label Studio 配置(部分更新):
    只覆盖请求中「非空」字段, 空/缺失字段保留原值.
    doUpload 全量写 url/token/project; lsLoadProjects 只带 url/token,
    不应因此清掉已保存的 project_id/project_name."""
    data = request.get_json(force=True) or {}
    cfg = _ls_read_config()
    for k in ("url", "token", "project_id", "project_name"):
        v = data.get(k)
        if isinstance(v, str):
            v = v.strip()
        if v not in (None, ""):
            cfg[k] = str(v)
    LS_CONFIG_FILE.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    return jsonify({"ok": True})


@app.route("/api/ls/projects", methods=["POST"])
def ls_projects():
    """后端代理列出 Label Studio 项目(避免前端跨域)"""
    data = request.get_json(force=True) or {}
    url = (data.get("url") or "").strip().rstrip("/")
    token = (data.get("token") or "").strip()
    if not url or not token:
        return jsonify({"error": "缺少 URL 或 Token"}), 400
    headers, _mode, err = _ls_auth(url, token)
    if not headers:
        return jsonify({"error": err}), 401
    try:
        r = requests.get(f"{url}/api/projects/", timeout=20, headers=headers)
        if r.status_code >= 400:
            return jsonify({"error": f"拉取项目列表失败(HTTP {r.status_code}): {r.text[:300]}"}), 502
        data = r.json() or []
        # LS 1.23 返回 DRF 分页格式: {"count","next","previous","results":[...]}
        items = data.get("results") if isinstance(data, dict) else data
        projects = [{"id": p.get("id"), "title": p.get("title")}
                    for p in items if isinstance(p, dict)]
        resp = {"projects": projects}
        # 注意: LS 1.23 的 /api/token/refresh 不轮换、不返回新 refresh token(轮换走的是需登录会话的
        # /api/token/rotate/, 外部工具用不了). 因此不再回传 config 里的 token —— 前端若用它覆盖
        # 输入框, 会把用户刚粘贴的有效 token 冲成 config 里的旧值, 导致下次连接必 401.
        return jsonify(resp)
    except requests.RequestException as e:
        return jsonify({"error": f"连接 Label Studio 失败: {e}"}), 502


@app.route("/api/jobs/<jid>/review")
def job_review(jid):
    """所有 jpg + meta(清晰度分/大小) + 所属round + 筛选状态.
    merge 任务: 各子任务保留池图片的并集, key = '<子任务jid>/<相对路径>',
    子任务中已人工剔除(drop)的图片不出现; 本会话已有标记带入 status."""
    j = JOBS.get(jid, {})
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在(可能已被清理)"}), 404
    if j.get("mode") == "merge":
        state = _review_state(out_root)
        items = []
        for cid, croot in merge_children(j):
            dropped = {k for k, v in _review_state(croot).items() if v == "drop"}
            for p in sorted(croot.rglob("*.jpg")):
                rel = str(p.relative_to(croot))
                if rel in dropped:
                    continue
                key = f"{cid}/{rel}"
                meta = _read_meta_json(Path(str(p) + ".meta.json"))
                items.append({
                    "file": key, "source": cid, "round": p.parent.name,
                    "score": meta.get("清晰度分", ""), "size": meta.get("大小KB", ""),
                    "status": state.get(key, ""),
                })
        children = [cid for cid, _ in merge_children(j)]
        return jsonify({"items": items, "count": len(items),
                        "output": f"合并筛选 {jid} · {len(children)} 个任务",
                        "merged": True, "children": children})
    state = _review_state(out_root)
    items = []
    for p in sorted(Path(out_root).rglob("*.jpg")):
        rel = str(p.relative_to(out_root))
        meta = {}
        meta_p = Path(str(p) + ".meta.json")
        if meta_p.exists():
            try:
                meta = json.loads(meta_p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                meta = {}
        items.append({
            "file": rel,
            "round": p.parent.name,
            "score": meta.get("清晰度分", ""),
            "size": meta.get("大小KB", ""),
            "status": state.get(rel, ""),
        })
    return jsonify({"items": items, "count": len(items), "output": str(out_root)})


@app.route("/api/jobs/<jid>/review", methods=["POST"])
def job_review_save(jid):
    """保存筛选标记 {相对路径: keep|drop}
    merge 任务: key 必须是 '<子任务jid>/<相对路径>' 且子任务属于本会话"""
    j = JOBS.get(jid, {})
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在"}), 404
    data = request.get_json(force=True) or {}
    decisions = data.get("decisions") or {}
    if j.get("mode") == "merge":
        children_ids = set(j.get("children") or [])
        clean = {}
        for k, v in decisions.items():
            if v not in ("keep", "drop"):
                continue
            head, sep, rest = k.partition("/")
            if sep and head in children_ids and rest and not rest.startswith(("/", ".")) \
                    and ".." not in rest.split("/") and rest.lower().endswith(".jpg"):
                clean[k] = v
        _save_review_state(out_root, clean)
        return jsonify({"ok": True, "saved": len(clean)})
    valid = {str(p.relative_to(out_root)) for p in Path(out_root).rglob("*.jpg")}
    clean = {k: v for k, v in decisions.items() if k in valid and v in ("keep", "drop")}
    _save_review_state(out_root, clean)
    return jsonify({"ok": True, "saved": len(clean)})


@app.route("/api/jobs/<jid>/img/<path:rel>")
def job_img(jid, rel):
    """安全读取输出目录内的 jpg(限目录+扩展名, 防路径穿越).
    merge 任务: rel 形如 '<子任务jid>/<相对路径>', 转发到子任务输出目录读取"""
    j = JOBS.get(jid, {})
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在"}), 404
    if j.get("mode") == "merge":
        head, sep, rest = rel.partition("/")
        if not sep or head not in (j.get("children") or []):
            return jsonify({"error": "非法路径"}), 403
        base_root = job_out_root(head)
        if not base_root:
            return jsonify({"error": "源任务输出目录不存在"}), 404
        rel = rest
    else:
        base_root = out_root
    base = Path(base_root).resolve()
    p = (base / rel).resolve()
    if not str(p).startswith(str(base)) or p.suffix.lower() != ".jpg":
        return jsonify({"error": "非法路径"}), 403
    if not p.exists():
        return jsonify({"error": "文件不存在"}), 404
    return send_file(p)


def _export_merged(jid, j):
    """合并任务导出: 保留图(各子任务保留池 − 本会话剔除)按每轮≤50张**混排统一打包**,
    zip 顶层直接是 round_XX_日期/ppe_...jpg(不分任务来源); manifest 增加 源任务/原始路径 溯源.
    返回 (zip_path 或 None, 错误信息或 None)"""
    out_root = job_out_root(jid)
    state = _review_state(out_root)
    dropped = {k for k, v in state.items() if v == "drop"}
    keep = [k for k in merge_pool_keys(j) if k not in dropped]
    if not keep:
        return None, "没有可导出的图片(全部被剔除了)"
    export_dir = OUTPUT_DIR / f"{jid}_已筛选_{time.strftime('%Y%m%d_%H%M%S')}"
    export_dir.mkdir(parents=True)
    date_str = time.strftime("%Y%m%d")
    rows = []
    round_no, seq = 1, 1
    cur_dir = export_dir / f"round_{round_no:02d}_{date_str}"
    cur_dir.mkdir(parents=True, exist_ok=True)
    for key in keep:
        if seq > 50:
            round_no += 1
            seq = 1
            cur_dir = export_dir / f"round_{round_no:02d}_{date_str}"
            cur_dir.mkdir(exist_ok=True)
        cid, _, rel = key.partition("/")
        croot = job_out_root(cid)
        if not croot:
            continue
        src = (Path(croot) / rel).resolve()
        if not src.exists() or src.suffix.lower() != ".jpg":
            continue
        new_name = f"ppe_{date_str}_{round_no:02d}_{seq:04d}.jpg"
        dst = cur_dir / new_name
        shutil.copy2(src, dst)
        meta = _read_meta_json(Path(str(src) + ".meta.json"))
        rows.append({
            "图片": str(dst.relative_to(export_dir)),
            "源任务": cid,
            "原始路径": rel,
            "来源视频": meta.get("来源视频", ""),
            "轮次": cur_dir.name,
            "帧序号": meta.get("帧序号", ""),
            "宽": meta.get("宽", ""), "高": meta.get("高", ""),
            "大小KB": meta.get("大小KB", ""), "清晰度分": meta.get("清晰度分", ""),
        })
        seq += 1
    manifest_p = export_dir / "manifest.csv"
    with open(manifest_p, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        w.writerows(rows)
    zip_path = OUTPUT_DIR / f"{jid}_已筛选.zip"
    if zip_path.exists():
        zip_path.unlink()
    shutil.make_archive(str(zip_path)[:-4], "zip", export_dir)
    return zip_path, None


@app.route("/api/jobs/<jid>/review/export", methods=["POST"])
def job_review_export(jid):
    """导出保留集: 按场景每轮≤50张重新打包(去 meta.json 过程文件), 重建 manifest.csv, 打包 zip 返回
    保留集 = 全部图片 − 被剔除的(未审=默认保留); zip 内仅 jpg + manifest.csv"""
    j = JOBS.get(jid, {})
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在"}), 404
    if j.get("mode") == "merge":
        zip_path, err = _export_merged(jid, j)
        if err:
            return jsonify({"error": err}), 400
        return send_file(zip_path, as_attachment=True,
                         download_name=f"已筛选图片_{jid}.zip")
    state = _review_state(out_root)
    dropped = {k for k, v in state.items() if v == "drop"}
    all_imgs = sorted(str(p.relative_to(out_root)) for p in Path(out_root).rglob("*.jpg"))
    keep = [f for f in all_imgs if f not in dropped]
    if not keep:
        return jsonify({"error": "没有可导出的图片(全部被剔除了)"}), 400

    export_dir = OUTPUT_DIR / f"{jid}_已筛选_{time.strftime('%Y%m%d_%H%M%S')}"
    export_dir.mkdir(parents=True)

    # 按原场景(第一层目录)分组, 场景内每轮 ≤50 重新打包, 重命名 ppe_日期_轮次_序号.jpg
    # 兼容两种结构: <场景>/round_XX/... 和 直接 round_XX/... (无场景层则全部归"默认场景")
    # zip 结构: <场景>/round_XX_日期/ppe_...jpg + manifest.csv(场景隔离, 轮次互不冲突)
    date_str = time.strftime("%Y%m%d")
    groups = {}
    for rel in keep:
        parts = Path(rel).parts
        if len(parts) > 1 and not parts[0].startswith("round_"):
            scene = parts[0]
        else:
            scene = "默认场景"
        groups.setdefault(scene, []).append(rel)

    rows = []
    for scene, rels in groups.items():
        scene_dir = export_dir / scene
        round_no, seq = 1, 1
        cur_dir = scene_dir / f"round_{round_no:02d}_{date_str}"
        cur_dir.mkdir(parents=True, exist_ok=True)
        for rel in rels:
            if seq > 50:
                round_no += 1
                seq = 1
                cur_dir = scene_dir / f"round_{round_no:02d}_{date_str}"
                cur_dir.mkdir(exist_ok=True)
            src = (Path(out_root) / rel).resolve()
            if not src.exists() or src.suffix.lower() != ".jpg":
                continue
            new_name = f"ppe_{date_str}_{round_no:02d}_{seq:04d}.jpg"
            dst = cur_dir / new_name
            shutil.copy2(src, dst)
            # 溯源从原 meta.json 读(meta 本身不进入交付包)
            meta = {}
            meta_src = Path(str(src) + ".meta.json")
            if meta_src.exists():
                try:
                    meta = json.loads(meta_src.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    meta = {}
            rows.append({
                "图片": str(dst.relative_to(export_dir)),
                "来源视频": meta.get("来源视频", ""),
                "子目录": scene,
                "轮次": cur_dir.name,
                "帧序号": meta.get("帧序号", ""),
                "宽": meta.get("宽", ""), "高": meta.get("高", ""),
                "大小KB": meta.get("大小KB", ""), "清晰度分": meta.get("清晰度分", ""),
            })
            seq += 1

    manifest_p = export_dir / "manifest.csv"
    with open(manifest_p, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        w.writerows(rows)

    zip_path = OUTPUT_DIR / f"{jid}_已筛选.zip"
    if zip_path.exists():
        zip_path.unlink()
    shutil.make_archive(str(zip_path)[:-4], "zip", export_dir)
    return send_file(zip_path, as_attachment=True,
                     download_name=f"已筛选图片_{jid}.zip")


@app.route("/api/jobs/<jid>/review/upload", methods=["POST"])
def job_review_upload(jid):
    """把 keep 图片批量上传到 Label Studio(每批50张, 自动创建task)"""
    out_root = job_out_root(jid)
    if not out_root:
        return jsonify({"error": "结果目录不存在"}), 404
    data = request.get_json(force=True) or {}
    url = (data.get("url") or "").strip().rstrip("/")
    token = (data.get("token") or "").strip()
    project_id = data.get("project_id")
    project_name = (data.get("project_name") or "").strip()
    if not url or not token:
        return jsonify({"error": "缺少 Label Studio URL 或 Token"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "URL 需以 http:// 或 https:// 开头"}), 400

    headers, mode, auth_err = _ls_auth(url, token)
    if not headers:
        return jsonify({"error": auth_err}), 401
    try:
        if project_id:
            pid = int(project_id)
        elif project_name:
            r = requests.post(f"{url}/api/projects/", timeout=20,
                              headers={**headers, "Content-Type": "application/json"},
                              json={"title": project_name, "label_config": _LS_LABEL_CONFIG})
            if r.status_code in (401, 403):
                return jsonify({"error": f"创建项目被拒绝(HTTP {r.status_code}): "
                                        f"token 无效或当前账号无创建权限. 详情: {r.text[:200]}"}), 502
            if r.status_code >= 400:
                return jsonify({"error": f"创建项目失败(HTTP {r.status_code}): {r.text[:300]}"}), 502
            pid = r.json().get("id")
        else:
            return jsonify({"error": "请提供 project_id 或 project_name"}), 400

        # 收集待上传文件的绝对路径 plan(单任务或合并会话统一处理)
        j = JOBS.get(jid, {})
        if j.get("mode") == "merge":
            sess_state = _review_state(out_root)
            sess_dropped = {k for k, v in sess_state.items() if v == "drop"}
            keep_keys = [k for k in merge_pool_keys(j) if k not in sess_dropped]
            plan = []
            for key in keep_keys:
                cid, _, rel = key.partition("/")
                croot = job_out_root(cid)
                if croot:
                    plan.append(str((Path(croot) / rel).resolve()))
            total = len(plan)
            if not plan:
                return jsonify({"error": "没有可上传的图片(全部被剔除了)"}), 400
        else:
            state = _review_state(out_root)
            dropped = {k for k, v in state.items() if v == "drop"}
            all_imgs = sorted(str(p.relative_to(out_root)) for p in Path(out_root).rglob("*.jpg"))
            keep = [f for f in all_imgs if f not in dropped]
            if not keep:
                return jsonify({"error": "没有可上传的图片(全部被剔除了)"}), 400
            plan = [str((Path(out_root) / f).resolve()) for f in keep]
            total = len(keep)

        ok, fail, batch = [], [], 50
        for i in range(0, len(plan), batch):
            chunk = plan[i:i + batch]
            # PAT 的 access token 约5分钟过期, 每批上传前刷新一次.
            # LS 1.23 refresh 端点不轮换不黑名单: 直接用请求里的 token(=输入框当前有效 PAT)反复换 access 即可.
            # 不要改从 config 读 token —— config 可能存着旧值, 反而把有效 PAT 换掉导致 401.
            if mode == "pat":
                access, _d = _ls_exchange_pat(url, token)
                if not access:
                    fail.extend([(Path(p).name, "access token 刷新失败(请重新复制 PAT)") for p in chunk])
                    continue
                headers = {"Authorization": f"Bearer {access}"}
            opened = []
            for _i, p in enumerate(chunk):
                pp = Path(p)
                # 字段名必须不同(file_0,file_1...), 同名 file 字段会被 LS 合并成1个task
                if pp.exists() and pp.suffix.lower() == ".jpg":
                    opened.append((f"file_{_i}", (pp.name, open(pp, "rb"), "image/jpeg")))
            if not opened:
                fail.extend([(Path(p).name, "文件缺失") for p in chunk])
                continue
            try:
                rr = requests.post(f"{url}/api/projects/{pid}/import",
                                   headers=headers, files=opened, timeout=120)
                if rr.status_code < 400:
                    ok.extend([Path(p).name for p in chunk])
                else:
                    fail.extend([(Path(p).name, rr.status_code) for p in chunk])
            except requests.RequestException as e:
                fail.extend([(Path(p).name, str(e)) for p in chunk])
            finally:
                for _, (_, fobj, _) in opened:
                    fobj.close()

        resp = {
            "ok": True, "project_id": pid, "success": len(ok), "total": total,
            "failed": [{"file": f, "detail": str(s)} for f, s in fail],
            "project_url": f"{url}/projects/{pid}",
        }
        # 同 ls_projects: LS 1.23 refresh 不轮换, 不回传 config token, 避免前端用旧值覆盖输入框
        return jsonify(resp)
    except requests.RequestException as e:
        return jsonify({"error": f"连接 Label Studio 失败: {e}"}), 502
    except ValueError:
        return jsonify({"error": "project_id 必须是数字"}), 400


if __name__ == "__main__":
    print("服务启动: http://127.0.0.1:8050")
    app.run(host="127.0.0.1", port=8050, threaded=True)
