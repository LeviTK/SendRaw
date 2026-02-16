# AGENTS.md

## 项目说明
- 项目类型: Calibre Interface Action 插件
- 当前源码目录: `send_raw/`
- 打包输出目录: `dist/`
- 远程仓库: `origin` (`main` 分支)

## Calibre 插件打包规范
- 安装包必须是 `.zip`。
- `zip` 根目录必须直接包含 `__init__.py`。
- 不能把源码目录名再包一层（错误示例: `send_raw/__init__.py` 在 zip 内作为首层路径）。
- 不要把 `__pycache__/`、`*.pyc` 打进安装包。
- 建议包含文件:
  - `__init__.py`
  - `ui.py`
  - `config.py`
  - `plugin-import-name-send_raw.txt`

## 标准打包命令（本项目）
```bash
mkdir -p dist
find send_raw -type d -name __pycache__ -prune -exec rm -rf {} +
cd send_raw && zip -r ../dist/send_raw_v1.1.2.zip __init__.py config.py ui.py plugin-import-name-send_raw.txt -x "*/__pycache__/*" "*.pyc"
```

## 打包校验
- 使用以下命令确认 zip 根目录结构正确:
```bash
unzip -l dist/send_raw_v1.1.2.zip
```
- 校验通过标准:
  - 列表中是 `__init__.py`、`ui.py` 等顶层文件
  - 不出现 `send_raw/__init__.py`
  - 不出现 `__pycache__` 或 `*.pyc`

## 发布前检查清单
- 更新 `send_raw/__init__.py` 中 `version`。
- 重新打包并执行 `unzip -l` 校验。
- 在 Calibre 中通过“从文件加载插件”安装测试。

## Git 同步规范
- `dist/` 目录仅作为本地打包产物，不提交到 Git，不同步到远程。
- `.superset/` 为本地目录，不提交到 Git，不同步到远程。
- 远程同步默认只提交源码与文档（如 `send_raw/*.py`、`AGENTS.md`、`.gitignore`）。
