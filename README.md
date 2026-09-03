# preprocessing-automation

监控视频 -> 可标注图片数据集的预处理流水线。

把海康 iVMS-4200 导出的私有封装 MP4（或普通视频/图片）批量转为标准图片数据集：转码 -> 抽帧 -> 质量过滤 -> 感知哈希去重 -> 标准化 -> 按轮打包（每轮 <= 50 张），并提供人工筛选与上传 Label Studio 的配套界面。

## 功能

- S1 批量转码：海康私有封装（IMKH + MPEG-PS）-> 标准 H.264 + AAC MP4
- S2/S3 抽帧 + 质量过滤：按间隔抽帧，过滤黑帧 / 过曝 / 模糊帧
- S5 dHash 去重：视频与上保留帧比较；图片与本次保留集全局比较
- S4 标准化：长边统一像素、JPG 压缩、单张限大小
- round 打包：每轮 <= 50 张，文件名 ppe_日期_轮次_序号.jpg，附 manifest.csv 溯源
- 增量处理：已处理视频自动跳过，新视频补进未满的 round
- 历史任务面板、多任务合并筛选、导出保留集 ZIP、上传 Label Studio
- 输出方式：拖拽上传模式输出在应用内（可 ZIP 下载）；本机路径模式输出在源文件夹旁

## 环境要求

- Python 3.10+
- ffmpeg（查找顺序：环境变量 FFMPEG -> 本机 node_modules/ffmpeg-static -> 项目内 ffmpeg/ -> PATH；Windows 建议直接放 PATH 或用 ffmpeg-static）
- pip 包：flask、pillow、numpy、requests

## 快速开始

macOS 与 Windows 通用（任选其一）：

```bash
python -m pip install flask pillow numpy requests
python app.py
```

浏览器打开 http://127.0.0.1:8050

也可使用一键脚本：

- macOS / Linux：`./start.sh`（如报权限错误先执行 `chmod +x start.sh`）
- Windows：双击 `start.bat`（首次会自动安装缺失依赖）

注意：仓库内的 start.sh 原为个人开发环境路径，如不能直接用请以 `python app.py` 启动。

## 使用

1. 打开页面后，把含视频的文件夹拖到中间，或切换到"本机路径"标签输入绝对路径（程序只读源文件，不改动）
2. 选择处理模式：
   - 完整预处理：转码 -> 抽帧 -> 过滤 -> 去重 -> 标准化 -> round 打包（输出图片数据集）
   - 仅转码 MP4：只输出可播放的标准 MP4（已标准的直接复制不重转）
3. 可选参数：抽帧间隔（秒）、去重差分阈值、长边像素、是否质量过滤、每轮张数
4. 处理完显示统计并下载 ZIP；"历史任务"面板可回看、重下、合并筛选

人工筛选与上传 Label Studio：在历史任务中进入"人工筛选"，用空格/双击剔除，K 标记保留，完成后可导出保留集 ZIP 或直接上传 Label Studio。合并筛选可把多个已完成任务的保留图合到一起统一筛选。

## 输出结构

```
<输入文件夹>_预处理结果/
├── 场景子目录（可选，与输入一致）
│   └── round_01_20260903/
│       ├── ppe_20260903_01_0001.jpg
│       └── ppe_20260903_01_0001.jpg.meta.json   # 旁车溯源文件
├── manifest.csv        # 图片 / 来源视频 / 轮次 / 帧序号 / 清晰度等
└── pipeline.log
```

下载 ZIP 只含交付内容（图片 + manifest.csv），不含内部状态文件。

## CLI（无界面）

```bash
python pipeline.py --input /path/to/输入目录 --output /path/to/输出目录 \
    --interval 1 --dedup-sim 97 --size 1280 --per-round 50
python pipeline.py --input /path/to/输入目录 --output /path/to/输出目录 --convert-only   # 仅转码
python pipeline.py --input ... --output ... --force                                   # 忽略增量全量重跑
```

## 目录结构（入库内容）

| 路径 | 作用 |
|---|---|
| app.py | Flask Web 后端（端口 8050），历史任务持久化 |
| pipeline.py | 流水线核心，可独立 CLI 运行 |
| static/index.html | 前端页面 |
| start.sh / start.bat | 一键启动脚本 |

运行期自动生成（不入库，参见 .gitignore）：output/、uploads/、ls_config.json、server.log、__pycache__/。

## 常见问题

- 端口占用：8050 被占用时修改 app.py 末尾的 `app.run(port=8050)`。
- 找不到 ffmpeg：装好 ffmpeg 并确保 `ffmpeg -version` 可用，或设置环境变量 FFMPEG 指向可执行文件。
- Label Studio 上传提示 Token 无效：Label Studio（1.23+）请在 Account & Settings -> Personal Access Token -> Create New Token，在创建成功弹窗中一次性完整复制（token 列表显示的是截断版，缺签名不可用）。有效 token 会被工具记住，无需反复新建。
