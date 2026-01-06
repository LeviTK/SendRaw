#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Send Raw to KOReader - UI Action Implementation
通过 SmartDevice 协议发送原始文件，保持 MD5 不变
"""

import os
import hashlib
import traceback
import time
from functools import partial
from io import BytesIO
from uuid import uuid4

from qt.core import (
    QMenu,
    QToolButton,
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QListWidget,
    QListWidgetItem,
    QProgressBar,
    QAbstractItemView,
    Qt,
    QMessageBox,
)

from calibre.gui2.actions import InterfaceAction
from calibre.gui2 import error_dialog, info_dialog
from calibre.gui2.threaded_jobs import ThreadedJob
from calibre.ebooks.metadata.book.base import Metadata


def calculate_md5(filepath):
    """计算文件 MD5"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def prepare_metadata_for_device(mi, extension, file_size, file_md5=None):
    """
    创建带有额外设备字段的 Metadata 副本
    优化：清洗元数据，移除可能导致 metadata.calibre 序列化错误的多余字段
    """
    metadata = mi.deepcopy_metadata() if hasattr(mi, "deepcopy_metadata") else mi

    # 1. 设置核心设备字段
    metadata.extension = extension.lower()
    metadata.size = file_size
    if file_md5:
        metadata.md5 = file_md5

    # 2. 清洗可能导致冲突的格式元数据
    # Calibre 的 Metadata 对象包含 format_metadata，其中可能有其他格式（如 EPUB）的信息
    # 发送 Raw 文件时，应清除这些信息，避免设备驱动写入错误的格式记录
    if hasattr(metadata, "format_metadata"):
        metadata.format_metadata = {}

    # 3. 清理二进制数据
    # 如果 metadata 对象中残留了封面的二进制数据，会导致 JSON 序列化失败或写入乱码
    if hasattr(metadata, "cover_data"):
        metadata.cover_data = None
    if hasattr(metadata, "thumbnail"):
        metadata.thumbnail = None

    # 4. 确保 UUID 存在（这对 Calibre 识别书籍至关重要）
    if not getattr(metadata, "uuid", None):
        metadata.uuid = str(uuid4())

    # 保留设备集合/标签信息
    metadata.device_collections = list(
        getattr(metadata, "device_collections", []) or []
    )

    return metadata


class SmartDeviceTransport:
    """
    SmartDevice 传输后端
    """

    def __init__(self, device):
        self.device = device
        self._error_reports = []

    def send_single_book(self, task):
        """
        发送单本书籍
        返回: (成功标志, 错误信息, 位置信息)
        位置信息用于后续调用 add_books_to_metadata 同步设备书籍列表
        """
        try:
            # 封装为单元素列表调用 upload_books
            paths = self.device.upload_books(
                [task["path"]],
                [task["name"]],
                on_card=None,
                end_session=False,  # 保持会话，直到最后关闭
                metadata=[task["metadata"]],
            )

            # 检查返回结果
            # upload_books 返回 [(lpath, length), ...] 格式的列表
            if paths and paths[0]:
                return True, None, paths[0]
            else:
                return False, "传输未确认", None

        except Exception as e:
            self._record_error(task["title"], e)
            return False, str(e), None

    def _record_error(self, title, exc):
        details = traceback.format_exc()
        message = str(exc)
        note = None
        # 针对特定错误的提示
        if "'extension'" in message or "KeyError" in message:
            note = (
                "检测到设备端元数据可能已损坏。\n"
                "1) 请在插件菜单中使用“清除 KOReader 元数据缓存”删除主机端缓存；\n"
                "2) 并在 KOReader -> Calibre 无线 -> 菜单 中执行“重置无线共享”以重建 metadata.calibre。"
            )
        if note:
            message = f"{message}\n{note}"
            details = f"{details}\n\n{note}"
        self._error_reports.append((title, message, details))

    def get_error_reports(self):
        return self._error_reports


def send_raw_books_worker(tasks, device, abort, log, notifications):
    """
    后台任务执行函数 (ThreadedJob worker)
    :param tasks: 发送任务列表
    :param device: 设备对象
    :param abort: 用于检测是否中止的 Event
    :param log: 日志对象
    :param notifications: 用于发送进度的 Queue (frac, msg)
    :return: (success_titles, failed_list, error_reports, upload_locations, upload_metadata)
    """
    transport = SmartDeviceTransport(device)
    total = len(tasks)

    # 确保设备连接
    if not device:
        raise RuntimeError("设备未连接")

    success_titles = []
    failed_list = []
    upload_locations = []
    upload_metadata = []

    for i, task in enumerate(tasks):
        if abort.is_set():
            break

        title = task["title"]
        log.info(f"Processing: {title}")

        # 在后台线程中计算 MD5（如果需要）
        if task.get("verify_md5", False):
            notifications.put(
                (float(i) / total, f"正在计算 MD5 ({i + 1}/{total}): {title}")
            )
            try:
                file_md5 = calculate_md5(task["path"])
                task["metadata"].md5 = file_md5
            except Exception as e:
                log.error(f"MD5 calculation failed for {title}: {e}")

        notifications.put((float(i) / total, f"正在发送 ({i + 1}/{total}): {title}"))

        # 发送书籍并获取返回的位置信息
        is_ok, err_msg, location = transport.send_single_book(task)

        if is_ok:
            success_titles.append(title)
            # 收集成功上传的位置和元数据，用于后续同步
            if location:
                upload_locations.append(location)
                upload_metadata.append(task["metadata"])
        else:
            failed_list.append((title, err_msg))

    notifications.put((1.0, "发送完成"))

    return (
        success_titles,
        failed_list,
        transport.get_error_reports(),
        upload_locations,
        upload_metadata,
    )


class SendRawAction(InterfaceAction):
    name = "Send Raw to KOReader"
    action_spec = (
        "Send Raw",
        None,
        "发送原始文件到设备（保持MD5不变）",
        "Ctrl+Shift+R",
    )
    action_type = "current"
    popup_type = QToolButton.ToolButtonPopupMode.MenuButtonPopup

    def genesis(self):
        self.menu = QMenu(self.gui)
        self.qaction.setMenu(self.menu)
        self.qaction.triggered.connect(self.send_raw_selected)
        self._build_menu()

    def _build_menu(self):
        self.menu.clear()

        self.create_menu_action(
            self.menu,
            "send_raw_epub",
            "发送 EPUB",
            triggered=partial(self.send_raw_selected, fmt="EPUB"),
        )

        self.create_menu_action(
            self.menu,
            "send_raw_pdf",
            "发送 PDF",
            triggered=partial(self.send_raw_selected, fmt="PDF"),
        )

        self.create_menu_action(
            self.menu,
            "send_raw_mobi",
            "发送 MOBI",
            triggered=partial(self.send_raw_selected, fmt="MOBI"),
        )

        self.create_menu_action(
            self.menu,
            "send_raw_azw3",
            "发送 AZW3",
            triggered=partial(self.send_raw_selected, fmt="AZW3"),
        )

        self.menu.addSeparator()

        self.create_menu_action(
            self.menu,
            "send_raw_auto",
            "自动选择格式",
            triggered=partial(self.send_raw_selected, fmt=None),
        )

        self.menu.addSeparator()

        self.create_menu_action(
            self.menu, "configure", "设置...", triggered=self.show_configuration
        )

        self.create_menu_action(
            self.menu,
            "cleanup_metadata",
            "清除 KOReader 元数据缓存",
            triggered=self.cleanup_metadata_cache,
        )

        self.create_menu_action(
            self.menu,
            "delete_metadata_remote",
            "删除设备上的 .metadata.calibre 文件",
            triggered=self.delete_remote_metadata_file,
        )

    def show_configuration(self):
        self.interface_action_base_plugin.do_user_config(self.gui)

    def _device_ready_for_send(self):
        dm = getattr(self.gui, "device_manager", None)
        if dm is not None and hasattr(dm, "get_current_device_information"):
            try:
                if dm.get_current_device_information() is None:
                    info_dialog(
                        self.gui,
                        "设备正在连接",
                        "设备信息正在加载中，请稍后再发送。",
                        det_msg="等待设备列表与书库加载完成后再试。",
                        show=True,
                    )
                    return False
            except Exception:
                # 如果无法获取设备信息，继续执行后续检查
                pass

        jm = getattr(self.gui, "job_manager", None)
        if jm is not None and hasattr(jm, "has_device_jobs"):
            try:
                device_busy = jm.has_device_jobs(queued_also=True)
            except TypeError:
                device_busy = jm.has_device_jobs()
            if device_busy:
                info_dialog(
                    self.gui,
                    "设备正在同步",
                    "当前设备正在同步，请稍后再发送。",
                    det_msg="等待设备同步任务完成后再试。",
                    show=True,
                )
                return False

        return True

    def send_raw_selected(self, fmt=None):
        """主入口：发送选中书籍的原始文件"""
        rows = self.gui.library_view.selectionModel().selectedRows()
        if not rows:
            return error_dialog(
                self.gui, "未选择书籍", "请先选择要发送的书籍", show=True
            )

        book_ids = [self.gui.library_view.model().id(r) for r in rows]

        device = self._get_smart_device()
        if device is None:
            return error_dialog(
                self.gui,
                "设备未连接",
                "请先通过无线连接 KOReader 设备",
                det_msg="在 KOReader 中打开 Calibre 无线连接，然后在 Calibre 中连接设备。",
                show=True,
            )

        if not self._device_ready_for_send():
            return

        tasks, failed = self._build_tasks(book_ids, fmt)

        if failed:
            # 如果有无法构建任务的书籍，提前告知，但不阻止其他书籍发送
            # 但如果 tasks 为空，则直接报错返回
            msg = f"{len(failed)} 本书籍无法处理，将被跳过。"
            info_dialog(self.gui, "部分跳过", msg, det_msg="\n".join(failed), show=True)

        if not tasks:
            return error_dialog(
                self.gui, "无法发送", "没有找到可发送的有效书籍任务", show=True
            )

        # 使用 Calibre JobManager 运行 (使用 ThreadedJob)
        from calibre.gui2 import Dispatcher

        job = ThreadedJob(
            "Send Raw Books",
            "正在发送书籍...",
            send_raw_books_worker,
            args=(tasks, device),
            kwargs={},
            callback=Dispatcher(self.job_finished),
        )

        self.gui.job_manager.run_threaded_job(job)
        self.gui.status_bar.show_message(
            f"正在后台发送 {len(tasks)} 本书籍到 KOReader...", 3000
        )

    def job_finished(self, job):
        """任务完成回调"""
        if job.failed:
            return error_dialog(
                self.gui,
                "发送失败",
                "后台发送任务执行失败",
                det_msg=job.details,
                show=True,
            )

        if not job.result:
            return

        (
            success_titles,
            failed_list,
            error_reports,
            upload_locations,
            upload_metadata,
        ) = job.result

        # 同步设备书籍列表
        self._sync_books_to_device(upload_locations, upload_metadata)

        # 显示结果摘要
        msg_parts = []
        if success_titles:
            msg_parts.append(f"成功发送: {len(success_titles)} 本")
        if failed_list:
            msg_parts.append(f"发送失败: {len(failed_list)} 本")

        summary_msg = "\n".join(msg_parts)

        detailed_msg = ""
        if failed_list:
            detailed_msg += "--- 失败详情 ---\n"
            for title, err in failed_list:
                detailed_msg += f"{title}: {err}\n"
            detailed_msg += "\n"

        if error_reports:
            detailed_msg += "--- 详细错误报告 ---\n"
            for title, msg, detail in error_reports:
                detailed_msg += f"[{title}]\n错误: {msg}\n详情:\n{detail}\n\n"

        if failed_list or error_reports:
            error_dialog(
                self.gui,
                "发送完成 (有错误)",
                summary_msg,
                det_msg=detailed_msg,
                show=True,
            )

    def _sync_books_to_device(self, locations, metadata):
        """
        将上传的书籍同步到设备的 booklist，确保 Calibre 界面实时显示
        """
        if not locations or not metadata:
            return

        try:
            dm = self.gui.device_manager
            if dm is None or not hasattr(dm, "device") or dm.device is None:
                return

            device = dm.device

            # 获取设备的 booklists
            # booklists 是一个三元组: (main_list, carda_list, cardb_list)
            booklists = self._get_booklists()
            if booklists is None:
                return

            # 调用设备驱动的 add_books_to_metadata 方法
            # 这会将新书添加到 booklist 中
            if hasattr(device, "add_books_to_metadata"):
                device.add_books_to_metadata(locations, metadata, booklists)

            # 关键修复：必须调用 set_books_in_library 初始化新书的 in_library 属性
            # 否则 GUI 刷新时会因访问不存在的属性而崩溃
            if hasattr(self.gui, "set_books_in_library"):
                self.gui.set_books_in_library(
                    booklists, reset=True, do_device_sync=False
                )

            # 刷新界面
            self._refresh_device_views()

        except Exception as e:
            print(f"Failed to sync books to device: {e}")
            traceback.print_exc()

    def _get_booklists(self):
        """获取设备的 booklists"""
        try:
            # 从三个设备视图获取 booklist
            main_list = (
                self.gui.memory_view.model().db
                if hasattr(self.gui, "memory_view")
                else None
            )
            carda_list = (
                self.gui.card_a_view.model().db
                if hasattr(self.gui, "card_a_view")
                else None
            )
            cardb_list = (
                self.gui.card_b_view.model().db
                if hasattr(self.gui, "card_b_view")
                else None
            )

            if main_list is not None:
                return (main_list, carda_list, cardb_list)
        except Exception as e:
            print(f"Failed to get booklists: {e}")

        return None

    def _refresh_device_views(self):
        """刷新设备视图"""
        try:
            # 刷新三个设备视图
            for view in (
                self.gui.memory_view,
                self.gui.card_a_view,
                self.gui.card_b_view,
            ):
                if hasattr(view, "model") and view.model() is not None:
                    view.model().resort(reset=False)
                    view.model().research()

            # 刷新库视图的 ondevice 列
            if hasattr(self.gui, "refresh_ondevice"):
                self.gui.refresh_ondevice()
        except Exception as e:
            print(f"Failed to refresh device views: {e}")

    def _get_smart_device(self):
        """获取已连接的 SmartDevice 设备"""
        dm = self.gui.device_manager
        if dm is None:
            return None

        device = getattr(dm, "device", None)
        if device is None:
            return None

        if not hasattr(device, "upload_books"):
            return None

        if not hasattr(device, "device_socket"):
            return None

        return device

    def _refresh_device_books_full(self):
        """
        强制彻底刷新设备书籍列表。
        通过调用 load_books_on_device 触发完整的设备扫描作业。
        """
        try:
            dm = self.gui.device_manager
            if dm is not None and hasattr(dm, "device") and dm.device is not None:
                # 这是一个异步 Job，会重新读取设备上的 metadata.calibre 并刷新界面
                dm.load_books_on_device(dm.device)
        except Exception as e:
            # 降级处理
            try:
                if hasattr(self.gui, "refresh_ondevice"):
                    self.gui.refresh_ondevice()
            except Exception:
                pass
            print(f"Failed to force reload device books: {e}")

    def cleanup_metadata_cache(self):
        """允许用户清理 .metadata.calibre 缓存文件"""
        confirm = QMessageBox.question(
            self.gui,
            "清除 KOReader 元数据缓存",
            "将尝试删除 Calibre 本地缓存的 .metadata.calibre 文件。\n"
            "此操作不会删除书籍，但会强制 Calibre 重新生成设备书库。\n\n"
            "继续执行？",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        deleted, errors = self._delete_local_metadata_files()

        if deleted:
            message = "已删除以下缓存文件:\n" + "\n".join(deleted)
        else:
            message = "未在本地缓存中找到 .metadata.calibre 文件。"

        det_lines = []
        if deleted:
            det_lines.append("已删除:")
            det_lines.extend(deleted)
        if errors:
            det_lines.append("\n无法删除:")
            det_lines.extend([f"{path} -> {err}" for path, err in errors])

        manual_note = (
            "\n下一步：在 KOReader -> Calibre 无线连接中打开菜单，选择“重置无线共享”以删除设备上的 .metadata.calibre。"
            "\n若需手动处理，可在 KOReader 的文件管理器中删除该文件后重启无线连接。"
        )

        info_dialog(
            self.gui,
            "清理完成",
            message + manual_note,
            det_msg="\n".join(det_lines) if det_lines else None,
            show=True,
        )

        self._notify_device_cleanup()

    def _delete_local_metadata_files(self):
        """删除本地缓存下的 .metadata.calibre"""
        from calibre.devices.smart_device_app.driver import (
            cache_dir as driver_cache_dir,
        )

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
            devices_path = os.path.join(config_dir, "device_drivers")
            if os.path.exists(devices_path):
                candidate_dirs.add(devices_path)

        metadata_files = []
        for base in candidate_dirs:
            for root, _, files in os.walk(base):
                if ".metadata.calibre" in files:
                    metadata_files.append(os.path.join(root, ".metadata.calibre"))

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
        if not device or not hasattr(device, "_show_message"):
            return
        try:
            device._show_message(
                "请在 KOReader 的 Calibre 菜单中运行“重置无线共享”以清理 .metadata.calibre 缓存。"
            )
        except Exception:
            pass

    def delete_remote_metadata_file(self):
        """从 KOReader 设备上删除 .metadata.calibre"""
        device = self._get_smart_device()
        if device is None:
            return error_dialog(
                self.gui,
                "设备未连接",
                "请先连接 KOReader 无线设备，再重试删除 .metadata.calibre 文件。",
                show=True,
            )

        target = self._locate_remote_metadata_file(device)
        if target is None:
            return info_dialog(
                self.gui,
                "未找到文件",
                "在设备收件箱目录中未检测到 metadata.calibre/.metadata.calibre 文件。",
                show=True,
            )

        confirm = QMessageBox.question(
            self.gui,
            "删除设备元数据",
            f"将删除 KOReader 收件箱目录下的 {target}，用于解除损坏的缓存。\n"
            "此操作不会删除书籍，但会强制 KOReader 重新生成设备书库。\n\n"
            "继续执行？",
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            cleaned_path = self._purge_remote_metadata_file(device, target)
            device.delete_books([cleaned_path], end_session=False)
        except Exception as err:
            self._record_error("删除 .metadata.calibre 失败", err)
            return error_dialog(
                self.gui,
                "删除失败",
                "无法删除设备上的 .metadata.calibre，请确认 KOReader 已连接并保持唤醒。",
                det_msg=str(err),
                show=True,
            )

        info_dialog(
            self.gui,
            "删除完成",
            f"已从设备删除 {cleaned_path}。\n请在 KOReader 中运行“重置无线共享”或重新连接，以重建元数据。",
            show=True,
        )

    def _locate_remote_metadata_file(self, device):
        candidates = [".metadata.calibre", "metadata.calibre"]
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
        payload = b"[]"
        meta = Metadata("Calibre Metadata Reset", ["Calibre"])
        meta.uuid = str(uuid4())
        meta.lpath = lpath
        meta.size = len(payload)
        meta.tags = []
        meta.comments = "calibre-metadata-reset"
        try:
            from calibre.utils.date import utcnow

            meta.last_modified = utcnow()
        except Exception:
            meta.last_modified = None

        wait_for_ack = getattr(device, "can_send_ok_to_sendbook", False)

        with device.sync_lock:
            opcode, result = device._call_client(
                "SEND_BOOK",
                {
                    "lpath": lpath,
                    "length": len(payload),
                    "metadata": meta,
                    "thisBook": 0,
                    "totalBooks": 1,
                    "willStreamBooks": True,
                    "willStreamBinary": True,
                    "wantsSendOkToSendbook": wait_for_ack,
                    "canSupportLpathChanges": True,
                },
                print_debug_info=False,
                wait_for_response=wait_for_ack,
            )

            if wait_for_ack:
                if opcode == "ERROR":
                    raise RuntimeError(result.get("message", "SEND_BOOK failed"))
                lpath = result.get("lpath", lpath)

            if payload:
                device._send_byte_string(device.device_socket, payload)

        return lpath

    def _build_tasks(self, book_ids, requested_fmt=None):
        """构建发送任务列表"""
        db = self.gui.current_db.new_api

        from calibre_plugins.send_raw.config import prefs

        preferred_formats = prefs.get(
            "preferred_formats", ["EPUB", "AZW3", "MOBI", "PDF"]
        )
        template = prefs.get("filename_template", "{author} - {title}")
        verify_md5 = prefs.get("verify_md5", True)

        tasks = []
        failed = []

        for book_id in book_ids:
            formats = db.formats(book_id)
            if not formats:
                mi = db.get_metadata(book_id)
                failed.append(f"{mi.title} - 无可用格式")
                continue

            if requested_fmt:
                target_fmt = (
                    requested_fmt.upper() if requested_fmt.upper() in formats else None
                )
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

            # 注意：不在主线程计算 MD5，避免 UI 卡顿
            # MD5 将在后台线程中按需计算
            metadata_obj = prepare_metadata_for_device(mi, target_fmt, file_size, None)

            tasks.append(
                {
                    "book_id": book_id,
                    "path": raw_path,
                    "name": filename,
                    "title": mi.title,
                    "metadata": metadata_obj,
                    "size": file_size,
                    "verify_md5": verify_md5,  # 传递标志，让后台线程决定是否计算
                    "format": target_fmt,
                }
            )

        return tasks, failed

    def _build_filename(self, mi, fmt, template):
        """根据模板生成文件名"""
        author = mi.authors[0] if mi.authors else "Unknown"
        title = mi.title or "Unknown"
        series = mi.series or ""
        series_index = str(int(mi.series_index)) if mi.series_index else ""

        try:
            filename = template.format(
                author=self._safe_filename(author),
                title=self._safe_filename(title),
                series=self._safe_filename(series),
                series_index=series_index,
            )
        except (KeyError, ValueError):
            filename = f"{self._safe_filename(author)} - {self._safe_filename(title)}"

        return f"{filename}.{fmt.lower()}"

    def _safe_filename(self, name):
        """生成安全的文件名"""
        if not name:
            return "Unknown"

        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, "_")

        name = name.strip(". ")

        if len(name) > 100:
            name = name[:100]

        return name or "Unknown"

    def location_selected(self, loc):
        pass

    def shutting_down(self):
        pass
