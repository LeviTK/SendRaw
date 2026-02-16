# Calibre 插件打包规范

## 目标
- 产出可直接在 Calibre 中安装的插件 zip 包。
- 避免目录结构错误导致 `InvalidPlugin`。

## 目录与文件要求
- 安装包必须是 `.zip`。
- `zip` 根目录必须直接包含 `__init__.py`。
- 不允许再包一层源码目录（错误示例：`send_raw/__init__.py`）。
- 建议包含文件：
  - `__init__.py`
  - `ui.py`
  - `config.py`
  - `plugin-import-name-send_raw.txt`

## 排除规则
- 不要把 `__pycache__/` 打进安装包。
- 不要把 `*.pyc` 打进安装包。
- `dist/` 仅作为本地打包产物，不提交到 Git。

## 标准打包命令（当前项目）
```bash
mkdir -p dist
find send_raw -type d -name __pycache__ -prune -exec rm -rf {} +
cd send_raw && zip -r ../dist/send_raw_v1.1.2.zip __init__.py config.py ui.py plugin-import-name-send_raw.txt -x "*/__pycache__/*" "*.pyc"
```

## 校验命令
```bash
unzip -l dist/send_raw_v1.1.2.zip
```

## 校验通过标准
- 列表中显示顶层文件：`__init__.py`、`ui.py`、`config.py`、`plugin-import-name-send_raw.txt`。
- 不出现 `send_raw/__init__.py`。
- 不出现 `__pycache__` 或 `*.pyc`。

## 发布前检查
- 更新 `send_raw/__init__.py` 中版本号。
- 重新打包并执行 `unzip -l` 校验。
- 在 Calibre 中执行“从文件加载插件”安装验证。
