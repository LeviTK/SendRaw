#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Send Raw to KOReader - UI Action Implementation
通过 SmartDevice 协议发送原始文件，保持 MD5 不变
"""

import os
import hashlib
import traceback
from functools import partial
from io import BytesIO
from uuid import uuid4

from qt.core import (
    QMenu, QToolButton, QThread, pyqtSignal, 
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QListWidget, QListWidgetItem, QProgressBar, QAbstractItemView,
    Qt, QMessageBox
)

from calibre.gui2.actions import InterfaceAction
from calibre.gui2 import error_dialog, info_dialog
from calibre.ebooks.metadata.book.base import Metadata


def calculate_md5(filepath):
    """计算文件 MD5"""
    hash_md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def prepare_metadata_for_device(mi, extension, file_size, file_md5=None):
    """创建带有额外设备字段的 Metadata 副本"""
    metadata = mi.deepcopy_metadata() if hasattr(mi, 'deepcopy_metadata') else mi
    metadata.extension = extension.lower()
    metadata.size = file_size
    if file_md5:
        metadata.md5 = file_md5
    # 保留原始文件名信息以便客户端生成 lpath
    metadata.device_collections = list(getattr(metadata, 'device_collections', []) or [])
    return metadata


class SmartDeviceTransport:
    """
    SmartDevice 传输后端
    """
    
    def __init__(self, device, gui):
        self.device = device
        self.gui = gui
        self._error_reports = []
    
    def send_books(self, tasks):
        """
        批量发送书籍，使用设备原生的 upload_books 方法
        """
        if not tasks:
            return [], []
        
        files = [task['path'] for task in tasks]
        names = [task['name'] for task in tasks]
        # 使用转换后的字典格式 metadata
        metadata = [task['metadata'] for task in tasks]
        
        success = []
        failed = []
        
        try:
            paths = self.device.upload_books(
                files, 
                names, 
                on_card=None, 
                end_session=True,
                metadata=metadata
            )
            
            for i, task in enumerate(tasks):
                if i < len(paths) and paths[i][1] >= 0:
                    success.append(task['title'])
                else:
                    failed.append((task['title'], '传输失败'))
            
            self._refresh_device_view()
                    
        except Exception as e:
            self._record_error('上传失败', e)
            for task in tasks:
                failed.append((task['title'], str(e)))
        
        return success, failed
    
    def _refresh_device_view(self):
        """刷新 Calibre 的设备视图"""
        try:
            if hasattr(self.gui, 'refresh_ondevice'):
                self.gui.refresh_ondevice()
        except Exception as e:
            self._record_error('刷新设备书库失败', e)

    def _record_error(self, title, exc):
        details = traceback.format_exc()
        message = str(exc)
        note = None
        if "'extension'" in message:
            note = (
                '检测到设备端返回的 metadata 缺少 extension 字段，这通常是旧版本插件遗留的记录。\n'
                '1) 请在插件菜单中使用“清除 KOReader 元数据缓存”删除主机端缓存；\n'
                '2) 然后在 KOReader -> Calibre 无线 -> 菜单 中执行“重置无线共享”，或手动在 KOReader 文件管理器中删除 /mnt/us/.metadata.calibre 文件。'
            )
        if note:
            message = f"{message}\n{note}"
            details = f"{details}\n\n{note}"
        self._error_reports.append((title, message, details))

    def pop_error_reports(self):
        reports = list(self._error_reports)
        self._error_reports.clear()
        return reports


class SendWorker(QThread):
    """后台发送线程"""
    
    progress = pyqtSignal(str)
    finished_all = pyqtSignal(list, list)
    
    def __init__(self, tasks, transport):
        super().__init__()
        self.tasks = tasks
        self.transport = transport
        self._cancelled = False
    
    def cancel(self):
        self._cancelled = True
    
    def run(self):
        if self._cancelled:
            self.finished_all.emit([], [])
            return
        
        self.progress.emit(f'正在发送 {len(self.tasks)} 本书籍...')
        
        success, failed = self.transport.send_books(self.tasks)
        
        self.finished_all.emit(success, failed)


class SendProgressDialog(QDialog):
    """发送进度对话框"""
    
    def __init__(self, parent, tasks, transport):
        super().__init__(parent)
        self.tasks = tasks
        self.transport = transport
        self.worker = None
        self._finished = False
        
        self.setWindowTitle('Send Raw to KOReader')
        self.setMinimumWidth(500)
        self.setMinimumHeight(400)
        self.init_ui()
        self.start_sending()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        total_size = sum(t['size'] for t in self.tasks) / (1024 * 1024)
        self.status_label = QLabel(f'准备发送 {len(self.tasks)} 本书籍 ({total_size:.1f} MB)...')
        layout.addWidget(self.status_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        layout.addWidget(self.progress_bar)
        
        self.book_list = QListWidget()
        self.book_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        for task in self.tasks:
            size_mb = task['size'] / (1024 * 1024)
            item = QListWidgetItem(f"○ {task['title']} ({size_mb:.1f} MB)")
            self.book_list.addItem(item)
        layout.addWidget(self.book_list)
        
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.cancel_btn = QPushButton('取消')
        self.cancel_btn.clicked.connect(self.on_cancel)
        btn_layout.addWidget(self.cancel_btn)
        
        self.close_btn = QPushButton('关闭')
        self.close_btn.clicked.connect(self.accept)
        self.close_btn.setEnabled(False)
        btn_layout.addWidget(self.close_btn)
        
        layout.addLayout(btn_layout)
    
    def start_sending(self):
        self.worker = SendWorker(self.tasks, self.transport)
        self.worker.progress.connect(self.on_progress)
        self.worker.finished_all.connect(self.on_finished)
        self.worker.start()
    
    def on_progress(self, message):
        self.status_label.setText(message)
    
    def on_finished(self, success, failed):
        self._finished = True
        self.cancel_btn.setEnabled(False)
        self.close_btn.setEnabled(True)
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        
        success_set = set(success)
        failed_dict = dict(failed)
        
        for i in range(self.book_list.count()):
            item = self.book_list.item(i)
            title = self.tasks[i]['title']
            if title in success_set:
                text = item.text()
                item.setText('✓ ' + text[2:])
                item.setForeground(Qt.GlobalColor.darkGreen)
            elif title in failed_dict:
                text = item.text()
                item.setText('✗ ' + text[2:] + f' - {failed_dict[title]}')
                item.setForeground(Qt.GlobalColor.red)
        
        if not success and not failed:
            self.status_label.setText('已取消')
        elif failed:
            self.status_label.setText(f'部分完成 - 成功 {len(success)} 本, 失败 {len(failed)} 本')
        else:
            self.status_label.setText(f'全部完成 - 成功发送 {len(success)} 本书籍 (MD5 未变)')
        
        self.success = success
        self.failed = failed
    
    def on_cancel(self):
        if self.worker and not self._finished:
            self.cancel_btn.setText('正在取消...')
            self.cancel_btn.setEnabled(False)
            self.worker.cancel()
    
    def closeEvent(self, event):
        if not self._finished and self.worker:
            self.worker.cancel()
            self.worker.wait(2000)
        event.accept()


class SendRawAction(InterfaceAction):
    name = 'Send Raw to KOReader'
    action_spec = ('Send Raw', None, '发送原始文件到设备（保持MD5不变）', 'Ctrl+Shift+R')
    action_type = 'current'
    popup_type = QToolButton.ToolButtonPopupMode.MenuButtonPopup

    def genesis(self):
        self.menu = QMenu(self.gui)
        self.qaction.setMenu(self.menu)
        self.qaction.triggered.connect(self.send_raw_selected)
        self._build_menu()

    def _build_menu(self):
        self.menu.clear()
        
        self.create_menu_action(
            self.menu, 'send_raw_epub',
            '发送 EPUB',
            triggered=partial(self.send_raw_selected, fmt='EPUB')
        )
        
        self.create_menu_action(
            self.menu, 'send_raw_pdf',
            '发送 PDF',
            triggered=partial(self.send_raw_selected, fmt='PDF')
        )
        
        self.create_menu_action(
            self.menu, 'send_raw_mobi',
            '发送 MOBI',
            triggered=partial(self.send_raw_selected, fmt='MOBI')
        )
        
        self.create_menu_action(
            self.menu, 'send_raw_azw3',
            '发送 AZW3',
            triggered=partial(self.send_raw_selected, fmt='AZW3')
        )
        
        self.menu.addSeparator()
        
        self.create_menu_action(
            self.menu, 'send_raw_auto',
            '自动选择格式',
            triggered=partial(self.send_raw_selected, fmt=None)
        )
        
        self.menu.addSeparator()
        
        self.create_menu_action(
            self.menu, 'configure',
            '设置...',
            triggered=self.show_configuration
        )

        self.create_menu_action(
            self.menu, 'cleanup_metadata',
            '清除 KOReader 元数据缓存',
            triggered=self.cleanup_metadata_cache
        )

        self.create_menu_action(
            self.menu, 'delete_metadata_remote',
            '删除设备上的 .metadata.calibre 文件',
            triggered=self.delete_remote_metadata_file
        )

    def show_configuration(self):
        self.interface_action_base_plugin.do_user_config(self.gui)

    def send_raw_selected(self, fmt=None):
        """主入口：发送选中书籍的原始文件"""
        rows = self.gui.library_view.selectionModel().selectedRows()
        if not rows:
            return error_dialog(
                self.gui, '未选择书籍',
                '请先选择要发送的书籍',
                show=True
            )
        
        book_ids = [self.gui.library_view.model().id(r) for r in rows]
        
        device = self._get_smart_device()
        if device is None:
            return error_dialog(
                self.gui, '设备未连接',
                '请先通过无线连接 KOReader 设备',
                det_msg='在 KOReader 中打开 Calibre 无线连接，然后在 Calibre 中连接设备。',
                show=True
            )
        
        tasks, failed = self._build_tasks(book_ids, fmt)
        
        if not tasks:
            msg = '没有找到可发送的书籍'
            if failed:
                msg += '\n\n' + '\n'.join(failed)
            return error_dialog(self.gui, '无法发送', msg, show=True)
        
        transport = SmartDeviceTransport(device, self.gui)
        dialog = SendProgressDialog(self.gui, tasks, transport)
        dialog.exec()
        
        self._refresh_device_books()
        
        if hasattr(dialog, 'success') and dialog.success:
            if dialog.failed:
                info_dialog(
                    self.gui, '发送完成',
                    f'成功: {len(dialog.success)} 本\n失败: {len(dialog.failed)} 本',
                    det_msg='\n'.join([f"{t}: {e}" for t, e in dialog.failed]),
                    show=True
                )

        for title, msg, detail in transport.pop_error_reports():
            error_dialog(
                self.gui, title,
                msg,
                det_msg=detail,
                show=True
            )

    def _get_smart_device(self):
        """获取已连接的 SmartDevice 设备"""
        dm = self.gui.device_manager
        if dm is None:
            return None
        
        device = getattr(dm, 'device', None)
        if device is None:
            return None
        
        if not hasattr(device, 'upload_books'):
            return None
        
        if not hasattr(device, 'device_socket'):
            return None
        
        return device
    
    def _refresh_device_books(self):
        """刷新设备书籍列表"""
        try:
            dm = self.gui.device_manager
            if dm is not None and hasattr(dm, 'device') and dm.device is not None:
                if hasattr(self.gui, 'refresh_ondevice'):
                    self.gui.refresh_ondevice()
        except Exception:
            pass

    def cleanup_metadata_cache(self):
        """允许用户清理 .metadata.calibre 缓存文件"""
        confirm = QMessageBox.question(
            self.gui,
            '清除 KOReader 元数据缓存',
            '将尝试删除 Calibre 本地缓存的 .metadata.calibre 文件。\n'
            '此操作不会删除书籍，但会强制 Calibre 重新生成设备书库。\n\n'
            '继续执行？'
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        deleted, errors = self._delete_local_metadata_files()

        if deleted:
            message = '已删除以下缓存文件:\n' + '\n'.join(deleted)
        else:
            message = '未在本地缓存中找到 .metadata.calibre 文件。'

        det_lines = []
        if deleted:
            det_lines.append('已删除:')
            det_lines.extend(deleted)
        if errors:
            det_lines.append('\n无法删除:')
            det_lines.extend([f"{path} -> {err}" for path, err in errors])

        manual_note = (
            '\n下一步：在 KOReader -> Calibre 无线连接中打开菜单，选择“重置无线共享”以删除设备上的 .metadata.calibre。'
            '\n若需手动处理，可在 KOReader 的文件管理器中删除该文件后重启无线连接。'
        )

        info_dialog(
            self.gui,
            '清理完成',
            message + manual_note,
            det_msg='\n'.join(det_lines) if det_lines else None,
            show=True
        )

        self._notify_device_cleanup()

    def _delete_local_metadata_files(self):
        """删除本地缓存下的 .metadata.calibre"""
        from calibre.devices.smart_device_app.driver import cache_dir as driver_cache_dir
        try:
            from calibre.utils.config import config_dir
        except Exception:
            config_dir = None

        candidate_dirs = set()

        try:
            cache_root = driver_cache_dir()
        except Exception:
            cache_root = None
        if cache_root and os.path.exists(cache_root):
            candidate_dirs.add(cache_root)

        if config_dir and os.path.exists(config_dir):
            candidate_dirs.add(config_dir)
            devices_path = os.path.join(config_dir, 'device_drivers')
            if os.path.exists(devices_path):
                candidate_dirs.add(devices_path)

        metadata_files = []
        for base in candidate_dirs:
            for root, _, files in os.walk(base):
                if '.metadata.calibre' in files:
                    metadata_files.append(os.path.join(root, '.metadata.calibre'))

        deleted = []
        errors = []
        for path in metadata_files:
            try:
                os.remove(path)
                deleted.append(path)
            except FileNotFoundError:
                continue
            except Exception as err:
                errors.append((path, str(err)))

        return deleted, errors

    def _notify_device_cleanup(self):
        device = self._get_smart_device()
        if not device or not hasattr(device, '_show_message'):
            return
        try:
            device._show_message(
                '请在 KOReader 的 Calibre 菜单中运行“重置无线共享”以清理 .metadata.calibre 缓存。'
            )
        except Exception:
            pass

    def delete_remote_metadata_file(self):
        """从 KOReader 设备上删除 .metadata.calibre"""
        device = self._get_smart_device()
        if device is None:
            return error_dialog(
                self.gui, '设备未连接',
                '请先连接 KOReader 无线设备，再重试删除 .metadata.calibre 文件。',
                show=True
            )

        target = self._locate_remote_metadata_file(device)
        if target is None:
            return info_dialog(
                self.gui, '未找到文件',
                '在设备收件箱目录中未检测到 metadata.calibre/.metadata.calibre 文件。',
                show=True
            )

        confirm = QMessageBox.question(
            self.gui,
            '删除设备元数据',
            f'将删除 KOReader 收件箱目录下的 {target}，用于解除损坏的缓存。\n'
            '此操作不会删除书籍，但会强制 KOReader 重新生成设备书库。\n\n'
            '继续执行？'
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            cleaned_path = self._purge_remote_metadata_file(device, target)
            device.delete_books([cleaned_path], end_session=False)
        except Exception as err:
            self._record_error('删除 .metadata.calibre 失败', err)
            return error_dialog(
                self.gui, '删除失败',
                '无法删除设备上的 .metadata.calibre，请确认 KOReader 已连接并保持唤醒。',
                det_msg=str(err),
                show=True
            )

        info_dialog(
            self.gui, '删除完成',
            f'已从设备删除 {cleaned_path}。\n请在 KOReader 中运行“重置无线共享”或重新连接，以重建元数据。',
            show=True
        )

    def _locate_remote_metadata_file(self, device):
        candidates = ['.metadata.calibre', 'metadata.calibre']
        for candidate in candidates:
            if self._remote_file_exists(device, candidate):
                return candidate
        return None

    def _remote_file_exists(self, device, lpath):
        sink = BytesIO()
        try:
            device.get_file(lpath, sink, end_session=False)
            return True
        except Exception:
            return False

    def _purge_remote_metadata_file(self, device, lpath):
        payload = b'[]'
        meta = Metadata('Calibre Metadata Reset', ['Calibre'])
        meta.uuid = str(uuid4())
        meta.lpath = lpath
        meta.size = len(payload)
        meta.tags = []
        meta.comments = 'calibre-metadata-reset'
        try:
            from calibre.utils.date import utcnow
            meta.last_modified = utcnow()
        except Exception:
            meta.last_modified = None

        wait_for_ack = getattr(device, 'can_send_ok_to_sendbook', False)

        with device.sync_lock:
            opcode, result = device._call_client(
                'SEND_BOOK',
                {
                    'lpath': lpath,
                    'length': len(payload),
                    'metadata': meta,
                    'thisBook': 0,
                    'totalBooks': 1,
                    'willStreamBooks': True,
                    'willStreamBinary': True,
                    'wantsSendOkToSendbook': wait_for_ack,
                    'canSupportLpathChanges': True,
                },
                print_debug_info=False,
                wait_for_response=wait_for_ack
            )

            if wait_for_ack:
                if opcode == 'ERROR':
                    raise RuntimeError(result.get('message', 'SEND_BOOK failed'))
                lpath = result.get('lpath', lpath)

            if payload:
                device._send_byte_string(device.device_socket, payload)

        return lpath

    def _build_tasks(self, book_ids, requested_fmt=None):
        """构建发送任务列表"""
        db = self.gui.current_db.new_api
        
        from calibre_plugins.send_raw.config import prefs
        preferred_formats = prefs.get('preferred_formats', ['EPUB', 'AZW3', 'MOBI', 'PDF'])
        template = prefs.get('filename_template', '{author} - {title}')
        verify_md5 = prefs.get('verify_md5', True)
        
        tasks = []
        failed = []
        
        for book_id in book_ids:
            formats = db.formats(book_id)
            if not formats:
                mi = db.get_metadata(book_id)
                failed.append(f"{mi.title} - 无可用格式")
                continue
            
            if requested_fmt:
                target_fmt = requested_fmt.upper() if requested_fmt.upper() in formats else None
            else:
                target_fmt = None
                for fmt in preferred_formats:
                    if fmt.upper() in formats:
                        target_fmt = fmt.upper()
                        break
                if target_fmt is None and formats:
                    target_fmt = formats[0]
            
            if target_fmt is None:
                mi = db.get_metadata(book_id)
                failed.append(f"{mi.title} - 无 {requested_fmt} 格式")
                continue
            
            raw_path = db.format_abspath(book_id, target_fmt)
            if not raw_path or not os.path.exists(raw_path):
                mi = db.get_metadata(book_id)
                failed.append(f"{mi.title} - 文件不存在")
                continue
            
            # 获取原始 Metadata 对象
            mi = db.get_metadata(book_id)
            filename = self._build_filename(mi, target_fmt, template)
            file_size = os.path.getsize(raw_path)
            file_md5 = calculate_md5(raw_path) if verify_md5 else None
            
            metadata_obj = prepare_metadata_for_device(
                mi, target_fmt, file_size, file_md5
            )
            
            tasks.append({
                'book_id': book_id,
                'path': raw_path,
                'name': filename,
                'title': mi.title,
                'metadata': metadata_obj,
                'size': file_size,
                'md5': file_md5,
                'format': target_fmt
            })
        
        return tasks, failed

    def _build_filename(self, mi, fmt, template):
        """根据模板生成文件名"""
        author = mi.authors[0] if mi.authors else 'Unknown'
        title = mi.title or 'Unknown'
        series = mi.series or ''
        series_index = str(int(mi.series_index)) if mi.series_index else ''
        
        try:
            filename = template.format(
                author=self._safe_filename(author),
                title=self._safe_filename(title),
                series=self._safe_filename(series),
                series_index=series_index
            )
        except (KeyError, ValueError):
            filename = f"{self._safe_filename(author)} - {self._safe_filename(title)}"
        
        return f"{filename}.{fmt.lower()}"

    def _safe_filename(self, name):
        """生成安全的文件名"""
        if not name:
            return 'Unknown'
        
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        name = name.strip('. ')
        
        if len(name) > 100:
            name = name[:100]
        
        return name or 'Unknown'

    def location_selected(self, loc):
        pass

    def shutting_down(self):
        pass
