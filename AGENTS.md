# AGENTS.md

## 1. 仓库定位
- 项目类型: Calibre Interface Action 插件（Send Raw to KOReader）
- 源码目录: `send_raw/`
- 文档目录: `doc/`
- 本地产物目录: `dist/`
- 远程仓库: `origin`（默认分支 `main`）

## 2. 规范入口（单一事实源）
- 打包与安装规范: `doc/calibre-plugin-packaging.md`
- 本文件只保留执行摘要；细则以 `doc/calibre-plugin-packaging.md` 为准。

## 3. 开发与提交流程（摘要）
1. 修改源码（主要是 `send_raw/*.py`）。
2. 语法检查:
   - `python3 -m py_compile send_raw/__init__.py send_raw/config.py send_raw/ui.py`
3. 更新版本号:
   - `send_raw/__init__.py` 中 `version = (...)`
4. 按文档打包并校验:
   - 参见 `doc/calibre-plugin-packaging.md`
5. 在 Calibre 里安装 zip 验证。
6. 提交并推送到 `origin/main`。

## 4. Git 规则
- `dist/` 只作为本地打包产物，不提交到 Git。
- `.superset/` 为本地目录，不提交到 Git。
- 常规提交范围: `send_raw/*.py`、`doc/*.md`、`AGENTS.md`、`.gitignore`。

## 5. 发布规则（摘要）
1. 确认 `main` 已包含目标提交。
2. 创建并推送 tag（例如 `v1.1.2`）。
3. 创建 GitHub Release（标题同 tag）。
4. 上传 `dist/` 下对应 zip 作为 Release 资产。
5. Release 备注使用中文 Markdown，简述修复点与资产名。
