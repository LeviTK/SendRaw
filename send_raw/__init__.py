#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Send Raw to KOReader - Calibre Plugin
发送原始文件到设备，不修改元数据，保持 MD5 哈希值不变
"""

from calibre.customize import InterfaceActionBase


class SendRawPlugin(InterfaceActionBase):
    name = "Send Raw to KOReader"
    description = "发送原始书籍文件到 KOReader，不注入元数据，保持 MD5 不变"
    supported_platforms = ["windows", "osx", "linux"]
    author = "C2KOReader"
    version = (1, 1, 1)
    minimum_calibre_version = (5, 0, 0)

    actual_plugin = "calibre_plugins.send_raw.ui:SendRawAction"

    def is_customizable(self):
        return True

    def config_widget(self):
        from calibre_plugins.send_raw.config import ConfigWidget

        return ConfigWidget()

    def save_settings(self, config_widget):
        config_widget.save_settings()

    def do_user_config(self, parent=None):
        from qt.core import QDialog, QVBoxLayout, QDialogButtonBox
        from calibre_plugins.send_raw.config import ConfigWidget

        dialog = QDialog(parent)
        dialog.setWindowTitle("Send Raw to KOReader - 设置")
        dialog.setMinimumWidth(450)

        layout = QVBoxLayout(dialog)
        config_widget = ConfigWidget()
        layout.addWidget(config_widget)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)

        if dialog.exec() == QDialog.DialogCode.Accepted:
            config_widget.save_settings()

        return dialog.result() == QDialog.DialogCode.Accepted
