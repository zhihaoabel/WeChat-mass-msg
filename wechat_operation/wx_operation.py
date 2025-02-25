# -*- coding: utf-8 -*-
# @Author : Frica01
# @Time   : 2022-09-11 17:16
# @Name   : wx_operation.py

"""微信群发消息"""

import os
import time
import subprocess
import uiautomation as auto
from copy import deepcopy
from typing import Iterable


class WxOperation:
    """
    微信群发消息的类。

    ...

    Attributes:
    ----------
    wx_window: auto.WindowControl
        微信控制窗口
    input_edit: wx_window.EditControl
        聊天界面输入框编辑控制窗口
    search_edit: wx_window.EditControl
        搜索输入框编辑控制窗口

    Methods:
    -------
    __goto_chat_box(name):
        跳转到 指定好友窗口
    __send_text(*msgs):
        发送文本。
    __send_file(*filepath):
        发送文件
    get_friend_list(tag, num):
        可指定tag，获取好友num页的好友数量
    send_msg(*names, msgs, file_paths)
        单个或批量发送文本和文件
    """

    def __init__(self):
        auto.SendKeys(text='{Alt}{Ctrl}w')  # 快捷键唤醒微信
        self.wx_window = auto.WindowControl(Name='微信', ClassName='WeChatMainWndForPC')
        assert self.wx_window.Exists(), "窗口不存在"
        self.input_edit = self.wx_window.EditControl(LocalizedControlType='edit')
        self.search_edit = self.wx_window.EditControl(Name='搜索')

    def __goto_chat_box(self, name: str) -> bool:
        """
        跳转到指定 name好友的聊天窗口。

        Args:
            name(str): 必选参数，好友名称

        Returns:
            None
        """
        assert name, "无法跳转到名字为空的聊天窗口"
        self.wx_window.SendKeys(text='{Ctrl}f', waitTime=0.2)
        self.wx_window.SendKeys(text='{Ctrl}a', waitTime=0.1)
        self.wx_window.SendKey(key=auto.SpecialKeyNames['DELETE'])
        auto.SetClipboardText(text=name)
        self.wx_window.SendKeys(text='{Ctrl}v', waitTime=0.1)
        group = self.wx_window.ListControl(Name="@str:IDS_FAV_SEARCH_RESULT:3780").ListItemControl(
            LocalizedControlType="列表项目")
        if group.Name.startswith('搜索'):
            return False

        self.wx_window.SendKey(key=auto.SpecialKeyNames['ENTER'], waitTime=0.2)
        return True

    def __send_text(self, *msgs) -> None:
        """
        发送文本.

        Args:
            *msgs(Iterable or str): 必选参数，为发送的文本

        Returns:
            None
        """
        for msg in msgs:
            assert msg, "发送的文本内容为空"
            self.input_edit.SendKeys(text='{Ctrl}a', waitTime=0.1)
            self.input_edit.SendKey(key=auto.SpecialKeyNames['DELETE'])
            # self.input_edit.SendKeys(text=msg, waitTime=0.1) # 一个个字符插入,不建议使用该方法
            # 设置到剪切板再黏贴到输入框
            auto.SetClipboardText(text=msg)
            self.input_edit.SendKeys(text='{Ctrl}v', waitTime=0.2)
            self.wx_window.SendKey(key=auto.SpecialKeyNames['ENTER'], waitTime=0.2)

    def __send_file(self, *file_paths) -> None:
        """
        发送文件.

        Args:
            *file_paths(Iterable or str): 必选参数，为文件的路径

        Returns:
            None
        """
        all_path = str()
        for path in file_paths:
            full_path = os.path.abspath(path=path)
            assert os.path.exists(full_path), f"{full_path} 文件路径有误"
            all_path += "'" + full_path + "',"
        args = ['powershell', f'Get-Item {all_path[:-1]} | Set-Clipboard']
        # 去除console 弹窗
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags = subprocess.CREATE_NEW_CONSOLE | subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        subprocess.Popen(args=args, startupinfo=startupinfo)
        time.sleep(0.5)
        self.input_edit.SendKeys(text='{Ctrl}v', waitTime=0.2)
        self.wx_window.SendKey(key=auto.SpecialKeyNames['ENTER'], waitTime=0.2)

    def get_friend_list(self, tag: str = None, num: int = 10) -> list:
        """
        获取微信好友名称.

        Args:
            tag(str): 可选参数，如不指定，则获取所有好友
            num(int): 可选参数，如不指定，只获取10页好友

        Returns:
            list
        """

        def click_tag():
            """点击标签"""
            contacts_management_window.ButtonControl(Name="标签").Click()

        # 点击 通讯录管理
        self.wx_window.ButtonControl(Name="通讯录").Click()
        self.wx_window.ListControl(Name="联系人").ButtonControl(Name="通讯录管理").Click()
        contacts_management_window = auto.GetForegroundControl()  # 切换到通讯录管理，相当于切换到弹出来的页面
        # contacts_management_window.ButtonControl(Name='最大化').Click()

        if tag:
            click_tag()  # 点击标签
            contacts_management_window.PaneControl(Name=tag).Click()
            time.sleep(0.3)
            click_tag()  # 关闭标签
        # 获取滑动模式
        scroll = contacts_management_window.ListControl().GetScrollPattern()
        # assert scroll, "没有可滑动对象"
        name_list = list()
        if not scroll:
            for name_node in contacts_management_window.ListControl().GetChildren():  # 获取当前页面的 列表 -> 子节点
                nick_name = name_node.TextControl().Name  # 用户名
                remark_name = name_node.ButtonControl(foundIndex=2).Name  # 用户备注名，索引1会错位，索引2是备注名，索引3是标签名
                name_list.append(remark_name if remark_name else nick_name)
        else:
            rate: int = int(float(102000 / num))  # 根据输入的num计算滑动的步长
            for pct in range(0, 102000, rate):  # range不支持float，不导入numpy库，采取迂回这的方式
                # 每次滑动一点点，-1代表不用滑动
                scroll.SetScrollPercent(horizontalPercent=-1, verticalPercent=pct / 100000)
                for name_node in contacts_management_window.ListControl().GetChildren():  # 获取当前页面的 列表 -> 子节点
                    nick_name = name_node.TextControl().Name  # 用户名
                    remark_name = name_node.ButtonControl(foundIndex=2).Name  # 用户备注名，索引1会错位，索引2是备注名，索引3是标签名
                    name_list.append(remark_name if remark_name else nick_name)
        contacts_management_window.SendKey(auto.SpecialKeyNames['ESC'])  # 结束时候关闭 "通讯录管理" 窗口
        return list(set(name_list))  # 简单去重，但是存在误判（如果存在同名的好友

    def get_group_chat_list(self) -> list:
        """获取群聊通讯录中的用户名称"""
        name_list = list()
        auto.ButtonControl(Name='聊天信息').Click()
        time.sleep(0.5)
        chat_members_win = self.wx_window.ListControl(Name='聊天成员')
        if not chat_members_win.Exists():
            return list()
        self.wx_window.ButtonControl(Name='查看更多').Click()
        for item in chat_members_win.GetChildren():
            name_list.append(item.ButtonControl().Name)
        return name_list

    def get_chat_records(self, page: int = 1) -> list:
        """
        获取聊天列表的聊天记录.

        Args:
            page(int): 可选参数，如不指定，只获取1页聊天记录

        Returns:
            list
        """
        chat_records = list()

        def extract_msg() -> None:
            all_msgs = self.wx_window.ListControl(Name="消息").GetChildren()
            for msg_node in all_msgs:
                msg = msg_node.Name
                if not msg:
                    continue
                if msg_node.PaneControl().Name:
                    chat_records.append({'type': 'Time', 'name': 'System', 'msg': msg_node.PaneControl().Name})
                    continue
                if '你已添加了' in msg and '现在可以开始聊天了' in msg:
                    chat_records.append({'type': 'System', 'name': 'System', 'msg': msg})
                    continue
                if msg in ['以下为新消息', '查看更多消息', '该类型文件可能存在安全风险，建议先检查文件安全性后再打开。']:
                    chat_records.append({'type': 'System', 'name': 'System', 'msg': msg})
                    continue
                if '撤回了一条消息' in msg or '尝试撤回上一条消息' in msg:
                    chat_records.append(
                        {'type': 'Other', 'name': ''.join(msg.split(' ')[:-1]), 'msg': msg.split(' ')[-1]})
                    continue
                if msg in ['发出红包，请在手机上查看', '收到红包，请在手机上查看',
                           '你发送了一次转账收款提醒，请在手机上查看', '你收到了一次转账收款提醒，请在手机上查看']:
                    chat_records.append({'type': 'RedEnvelope', 'name': 'System', 'msg': msg})
                    continue
                if '领取了你的红包' in msg:
                    _ = msg.split('领取了你的红包')
                    chat_records.append({'type': 'RedEnvelope', 'name': _[0], 'msg': _[1]})
                    continue
                name = msg_node.ButtonControl(foundIndex=1).Name
                if msg == '[文件]':
                    file_name = msg_node.PaneControl().TextControl(foundIndex=1).Name
                    size = msg_node.PaneControl().TextControl(foundIndex=2).Name
                    chat_records.append(
                        {'type': 'File', 'name': name, 'msg': f'size: {size}  ---  file_name: {file_name}'})
                    continue
                if msg == '微信转账':
                    operation = msg_node.PaneControl().TextControl(foundIndex=2).Name
                    amount = msg_node.PaneControl().TextControl(foundIndex=3).Name
                    chat_records.append(
                        {'type': 'RedEnvelope', 'name': name, 'msg': msg + f'    {operation}    ' + amount})
                    continue
                if '引用' in msg and '的消息' in msg:
                    chat_records.append({'type': 'Cited', 'name': name, 'msg': msg})
                    continue
                if msg == '[聊天记录]':
                    if not name:
                        name = msg_node.ButtonControl(foundIndex=2).Name
                chat_records.append({'type': 'Content', 'name': name, 'msg': msg})

        for _ in range(page):
            self.wx_window.WheelUp(wheelTimes=15)
        extract_msg()
        return chat_records

    def send_msg(self, *names, msgs, file_paths, add_remark_name=False) -> None:
        """
        发送消息，可同时发送文本和文件（至少选一项

        Args:
            *names (str or Iterable):必选参数，接收消息的好友名称，可以群发，也可以单发
            msgs (list): 可选参数，发送的文本消息
            file_paths (Iterable):可选参数，发送的文件路径
            add_remark_name(bool): 可选参数，是否添加备注名称发送

        Returns:
            None
        """
        assert names, "用户名列表为空"
        assert any([msgs, file_paths]), "没有发送任何消息"
        assert not isinstance(msgs, str), "文本必须为可迭代且非字符串类型"
        assert not isinstance(file_paths, str), "文件路径必须为可迭代且非字符串类型"
        for name in names:
            self.__goto_chat_box(name=name[0])
            if msgs:
                if add_remark_name:
                    new_msgs = deepcopy(msgs)
                    new_msgs.insert(0, name[0])
                    self.__send_text(*new_msgs)
                else:
                    self.__send_text(*msgs)
            if file_paths:
                self.__send_file(*file_paths)

    def send_msg_without_gui(self, msg_dict, file_paths, add_remark_name=False) -> None:
        """
        发送消息，可同时发送文本和文件（至少选一项）, 不带GUI。可针对不同群群发不同消息

        Args:
            msg_dict (dict): 必选参数，key为商户号，value为消息列表
            file_paths (Iterable):可选参数，发送的文件路径
            add_remark_name(bool): 可选参数，是否添加备注名称发送

        Returns:
            None
        """
        assert msg_dict, "商户号为空"
        assert not isinstance(file_paths, str), "文件路径必须为可迭代且非字符串类型"

        not_found = []

        for merchant_id in msg_dict:
            success = self.__goto_chat_box(name=merchant_id)
            if not success:
                print(f"商户号{merchant_id}没有找到")
                not_found.append(merchant_id)
                continue
            msgs = msg_dict[merchant_id]
            assert any([msgs, file_paths]), "针对商户号为 {} 的商户待发送任何消息为空"
            if msgs:
                if add_remark_name:
                    new_msgs = deepcopy(msgs)
                    new_msgs.insert(0, merchant_id)
                    self.__send_text(*new_msgs)
                else:
                    self.__send_text(*msgs)
            if file_paths:
                self.__send_file(*file_paths)
        print(f"未找到的商户号有：{not_found}")

    def get_emoji_button(self) -> auto.ButtonControl:
        """获取表情按钮"""
        return self.wx_window.ButtonControl(Name='表情')


# wx = WxOperation()
# wx.send_msg(['文件传输助手'], msgs=['群发测试'], file_paths=[], add_remark_name=False)
# wx.send_msg_without_gui({'文件传输助手': ['[囧] [汗]']}, file_paths=[],
#                         add_remark_name=False)
