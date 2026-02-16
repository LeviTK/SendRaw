#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Send Raw to KOReader - Configuration
"""

from qt.core import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QGroupBox,
    QCheckBox,
)

from calibre.utils.config import JSONConfig

prefs = JSONConfig("plugins/send_raw")

prefs.defaults["preferred_formats"] = ["EPUB", "AZW3", "MOBI", "PDF", "CBZ", "CBR"]
prefs.defaults["filename_template"] = "{author} - {title}"
prefs.defaults["verify_md5"] = False


class ConfigWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.load_settings()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 格式优先级
        fmt_group = QGroupBox("格式优先级（从上到下）")
        fmt_layout = QVBoxLayout(fmt_group)

        self.format_list = QListWidget()
        self.format_list.setDragDropMode(QListWidget.DragDropMode.InternalMove)
        self.format_list.setMaximumHeight(120)
        fmt_layout.addWidget(self.format_list)

        btn_layout = QHBoxLayout()
        self.add_format_btn = QPushButton("添加")
        self.remove_format_btn = QPushButton("删除")
        self.move_up_btn = QPushButton("上移")
        self.move_down_btn = QPushButton("下移")

        self.add_format_btn.clicked.connect(self.add_format)
        self.remove_format_btn.clicked.connect(self.remove_format)
        self.move_up_btn.clicked.connect(self.move_up)
        self.move_down_btn.clicked.connect(self.move_down)

        btn_layout.addWidget(self.add_format_btn)
        btn_layout.addWidget(self.remove_format_btn)
        btn_layout.addWidget(self.move_up_btn)
        btn_layout.addWidget(self.move_down_btn)
        fmt_layout.addLayout(btn_layout)

        layout.addWidget(fmt_group)

        # 传输设置
        transfer_group = QGroupBox("传输设置")
        transfer_layout = QVBoxLayout(transfer_group)

        # 文件名模板
        template_layout = QHBoxLayout()
        template_layout.addWidget(QLabel("文件名模板:"))
        self.template_edit = QLineEdit()
        self.template_edit.setPlaceholderText("{author} - {title}")
        template_layout.addWidget(self.template_edit)
        transfer_layout.addLayout(template_layout)

        transfer_layout.addWidget(
            QLabel("可用变量: {title}, {author}, {series}, {series_index}")
        )

        # MD5 校验
        self.verify_md5_cb = QCheckBox("启用 MD5 校验（确保文件完整性）")
        transfer_layout.addWidget(self.verify_md5_cb)

        layout.addWidget(transfer_group)

        # 说明
        info_group = QGroupBox("说明")
        info_layout = QVBoxLayout(info_group)
        info_label = QLabel(
            "此插件通过 SmartDevice 协议发送原始书籍文件到 KOReader，\n"
            "不修改文件内容，保持 MD5 哈希值不变，适用于多端同步场景。\n\n"
            "使用前请确保 KOReader 已通过无线连接到 Calibre。"
        )
        info_label.setWordWrap(True)
        info_layout.addWidget(info_label)
        layout.addWidget(info_group)

        layout.addStretch()

    def load_settings(self):
        formats = prefs.get("preferred_formats", ["EPUB", "AZW3", "MOBI", "PDF"])
        self.format_list.clear()
        self.format_list.addItems(formats)

        self.template_edit.setText(prefs.get("filename_template", "{author} - {title}"))
        self.verify_md5_cb.setChecked(prefs.get("verify_md5", False))

    def save_settings(self):
        formats = []
        for i in range(self.format_list.count()):
            formats.append(self.format_list.item(i).text())
        prefs["preferred_formats"] = formats

        prefs["filename_template"] = (
            self.template_edit.text().strip() or "{author} - {title}"
        )
        prefs["verify_md5"] = self.verify_md5_cb.isChecked()

    def add_format(self):
        from qt.core import QInputDialog

        fmt, ok = QInputDialog.getText(
            self, "添加格式", "输入格式名称 (例如: EPUB, PDF, MOBI):"
        )
        if ok and fmt:
            fmt = fmt.upper().strip()
            existing = [
                self.format_list.item(i).text() for i in range(self.format_list.count())
            ]
            if fmt not in existing:
                self.format_list.addItem(fmt)

    def remove_format(self):
        current = self.format_list.currentRow()
        if current >= 0:
            self.format_list.takeItem(current)

    def move_up(self):
        current = self.format_list.currentRow()
        if current > 0:
            item = self.format_list.takeItem(current)
            self.format_list.insertItem(current - 1, item)
            self.format_list.setCurrentRow(current - 1)

    def move_down(self):
        current = self.format_list.currentRow()
        if current >= 0 and current < self.format_list.count() - 1:
            item = self.format_list.takeItem(current)
            self.format_list.insertItem(current + 1, item)
            self.format_list.setCurrentRow(current + 1)
