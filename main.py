"""
飞书 × Claude Code Bot
通过飞书 WebSocket 长连接接收私聊/群聊消息，调用本机 claude CLI 回复，支持流式卡片输出。

启动：python main.py
"""

import asyncio
import json
import re
import sys
import os
import threading
import time
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler

# 确保项目目录在 sys.path 最前面
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lark_oapi as lark
from lark_oapi.api.im.v1.model import P2ImMessageReceiveV1
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger, P2CardActionTriggerResponse, CallBackToast,
)

import bot_config as config
from feishu_client import FeishuClient
from session_store import SessionStore, generate_summary, _write_custom_title
from commands import parse_command, handle_command
from claude_runner import run_claude
from run_control import ActiveRun, ActiveRunRegistry, stop_run

# ── 看门狗：定时重启防止 WebSocket 假死 ──────────────────────

MAX_UPTIME = 12 * 3600   # 最长运行 12 小时后主动重启
_start_time = time.time()
_last_event = time.time()


def _watchdog():
    """后台线程，定期检查进程健康。异常时退出让 launchctl 拉起。"""
    while True:
        time.sleep(300)  # 每 5 分钟检查
        uptime = time.time() - _start_time
        idle = time.time() - _last_event

        if uptime > MAX_UPTIME:
            if _active_runs.has_active_runs():
                print(f"[watchdog] 运行 {uptime/3600:.1f}h 已超，但有任务在跑，推迟重启", flush=True)
            else:
                print(f"[watchdog] 运行 {uptime/3600:.1f}h，定时重启刷新连接", flush=True)
                os._exit(0)

        print(f"[watchdog] uptime={uptime/3600:.1f}h idle={idle/60:.0f}min", flush=True)


# ── 全局单例 ──────────────────────────────────────────────────

_ws_loop = None  # WebSocket 事件循环引用，供 HTTP 回调线程调度异步任务

lark_client = lark.Client.builder() \
    .app_id(config.FEISHU_APP_ID) \
    .app_secret(config.FEISHU_APP_SECRET) \
    .log_level(lark.LogLevel.INFO) \
    .build()

feishu = FeishuClient(lark_client, app_id=config.FEISHU_APP_ID, app_secret=config.FEISHU_APP_SECRET)
store = SessionStore()
_active_runs = ActiveRunRegistry()

# per-chat 消息队列锁，保证同一群组的消息串行处理，允许不同群组并发处理
_chat_locks: dict[str, asyncio.Lock] = {}
_MAX_CHAT_LOCKS = 200  # 防止无界增长

# per-user 消息聚合缓冲：快速连续到达的消息合并为一条发给 Claude
_msg_buffers: dict[str, list] = {}  # user_id -> [msg, ...]
_msg_buffer_tasks: dict[str, asyncio.Task] = {}  # user_id -> debounce task
_MSG_DEBOUNCE_SEC = 1.0  # 等待窗口：最后一条文件/图片到达后等1秒再合并发送

# ── /btw 队列 ─────────────────────────────────────────────────
_btw_pending: dict[str, list[str]] = {}  # user_id → [content strings]


# ── 卡片 header（完成信号）────────────────────────────────────

def _card_header(state: str) -> dict | None:
    """Return CardKit 2.0 header dict for given state, or None."""
    headers = {
        "completed":     {"title": {"tag": "plain_text", "content": "✅ 完成"},       "template": "green"},
        "error":         {"title": {"tag": "plain_text", "content": "❌ 出错"},       "template": "red"},
        "interrupted":   {"title": {"tag": "plain_text", "content": "⏹ 已中断"},     "template": "orange"},
        "plan_review":   {"title": {"tag": "plain_text", "content": "📋 方案待审核"}, "template": "blue"},
        "plan_approved": {"title": {"tag": "plain_text", "content": "✅ 方案已批准"}, "template": "green"},
    }
    return headers.get(state)


# ── /stop 命令处理 ───────────────────────────────────────────

async def _announce_stopped_run(active_run: ActiveRun):
    try:
        elements = [{"tag": "markdown", "content": "⏹ 已停止当前任务"}]
        await feishu.update_card_elements(active_run.card_msg_id, elements, header=_card_header("interrupted"))
    except Exception:
        try:
            await feishu.update_card(active_run.card_msg_id, "⏹ 已停止当前任务")
        except Exception as exc:
            print(f"[warn] update stopped card failed: {exc}", flush=True)


async def _announce_interrupted(active_run: ActiveRun):
    try:
        elements = [{"tag": "markdown", "content": "⏹ 已被新消息打断"}]
        await feishu.update_card_elements(active_run.card_msg_id, elements, header=_card_header("interrupted"))
    except Exception:
        try:
            await feishu.update_card(active_run.card_msg_id, "⏹ 已被新消息打断")
        except Exception:
            pass


async def _handle_stop_command(sender_open_id: str) -> str:
    active_run = _active_runs.get_run(sender_open_id)
    if active_run is None:
        return "当前没有正在运行的任务"
    if active_run.stop_requested:
        return "正在停止当前任务，请稍候"
    stopped = await stop_run(
        _active_runs,
        sender_open_id,
        on_stopped=_announce_stopped_run,
    )
    if not stopped:
        return "当前没有正在运行的任务"
    return "已发送停止请求"


# ── 命令菜单（锁外即时响应）──────────────────────────────────

_COMMAND_MENU_GROUPS = [
    ("**会话**", [
        {"text": "🆕 新会话",      "value": {"action": "run_cmd", "cmd": "/new"}},
        {"text": "📋 新会话(规划)", "value": {"action": "run_cmd", "cmd": "/new plan"}},
        {"text": "📂 恢复会话",    "value": {"action": "run_cmd", "cmd": "/resume"}},
        {"text": "⏹ 停止任务",     "value": {"action": "run_cmd", "cmd": "/stop"}},
    ]),
    ("**配置**", [
        {"text": "🔄 切模型",      "value": {"action": "run_cmd", "cmd": "/model"}},
        {"text": "🎚 切 effort",   "value": {"action": "run_cmd", "cmd": "/effort"}},
        {"text": "⚙️ 切模式",      "value": {"action": "run_cmd", "cmd": "/mode"}},
        {"text": "📁 工作空间",    "value": {"action": "run_cmd", "cmd": "/ws"}},
    ]),
    ("**查看**", [
        {"text": "📊 状态",        "value": {"action": "run_cmd", "cmd": "/status"}},
        {"text": "📈 用量",        "value": {"action": "run_cmd", "cmd": "/usage"}},
        {"text": "🛠 Skills",      "value": {"action": "run_cmd", "cmd": "/skills"}},
        {"text": "🔌 MCP",         "value": {"action": "run_cmd", "cmd": "/mcp"}},
        {"text": "📄 目录",        "value": {"action": "run_cmd", "cmd": "/ls"}},
        {"text": "❓ 帮助",        "value": {"action": "run_cmd", "cmd": "/help"}},
    ]),
]


async def _show_command_menu(user_id: str, chat_id: str, is_group: bool, msg_id: str):
    """显示分组命令菜单（markdown 标题 + 按钮混排），不走队列锁"""
    elements = []
    for title, buttons in _COMMAND_MENU_GROUPS:
        elements.append({"tag": "markdown", "content": title})
        columns = []
        for btn in buttons:
            value = {**btn["value"], "cid": chat_id}
            columns.append({
                "tag": "column",
                "width": "auto",
                "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": btn["text"]},
                    "type": "default",
                    "size": "small",
                    "name": f"menu_{btn['value']['cmd'].replace('/', '').replace(' ', '_')}",
                    "value": value,
                    "behaviors": [{"type": "callback", "value": value}],
                }],
            })
        elements.append({"tag": "column_set", "flex_mode": "flow", "columns": columns})
    try:
        if is_group:
            card_id = await feishu.reply_card(msg_id, content="⚡ 快捷命令", loading=False)
        else:
            card_id = await feishu.send_card_to_user(user_id, content="⚡ 快捷命令", loading=False)
        await feishu.update_card_elements(card_id, elements)
    except Exception as e:
        print(f"[error] 命令菜单发送失败: {e}", flush=True)


# ── 核心消息处理（async）─────────────────────────────────────

def extract_chat_info(event: P2ImMessageReceiveV1) -> tuple[str, str, bool]:
    """
    Extract user_id, chat_id, and is_group from message event.

    Returns:
        (user_id, chat_id, is_group)
        - For private chat: chat_id = user_id
        - For group chat: chat_id = group's chat_id
    """
    sender = event.event.sender
    user_id = sender.sender_id.open_id

    message = event.event.message
    chat_type = message.chat_type
    chat_id_raw = message.chat_id

    is_group = (chat_type == "group")

    if is_group:
        chat_id = chat_id_raw
    else:
        chat_id = user_id

    return user_id, chat_id, is_group


async def handle_message_async(event: P2ImMessageReceiveV1):
    """异步处理一条飞书消息"""
    msg = event.event.message
    print(f"[收到消息] type={msg.message_type} chat={msg.chat_type}", flush=True)

    # Extract chat info (supports both private and group chats)
    user_id, chat_id, is_group = extract_chat_info(event)
    print(f"[Chat Info] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)

    # /stop 和 / 在锁外处理（不需要排队等 Claude）
    if msg.message_type == "text":
        try:
            _text = json.loads(msg.content).get("text", "").strip()
        except Exception:
            _text = ""
        # 群聊去掉 @mention
        if is_group:
            for m in (getattr(msg, 'mentions', None) or []):
                k = getattr(m, 'key', '')
                if k:
                    _text = _text.replace(k, '').strip()

        if _text.lower() in ("/stop", "/stop") or _text.strip().endswith("/stop"):
            reply = await _handle_stop_command(user_id)
            if is_group:
                await feishu.reply_card(msg.message_id, content=reply, loading=False)
            else:
                await feishu.send_card_to_user(user_id, content=reply, loading=False)
            return

        # 单独输入 / → 显示命令菜单（按钮）
        if _text == "/":
            await _show_command_menu(user_id, chat_id, is_group, msg.message_id)
            return

    # 群聊只响应 @机器人 的消息
    if is_group:
        mentions = getattr(msg, 'mentions', None) or []
        if not mentions:
            return  # 没有 @mention，忽略

    # 消息聚合策略：
    # - 文本消息：立即处理（带上缓冲区里的文件/图片）
    # - 文件/图片消息：短暂缓冲，等待同批次的其他文件
    buf_key = user_id
    is_bufferable = msg.message_type in ("file", "image", "audio")

    if is_bufferable:
        if buf_key not in _msg_buffers:
            _msg_buffers[buf_key] = []
        _msg_buffers[buf_key].append((user_id, chat_id, is_group, msg))
        print(f"[聚合] 文件/图片入缓冲，当前 {len(_msg_buffers[buf_key])} 条", flush=True)

        old_task = _msg_buffer_tasks.get(buf_key)
        if old_task and not old_task.done():
            old_task.cancel()

        async def _flush_buffer():
            await asyncio.sleep(_MSG_DEBOUNCE_SEC)
            await _dispatch_buffered(buf_key)

        _msg_buffer_tasks[buf_key] = asyncio.ensure_future(_flush_buffer())
    else:
        # 文本/富文本：立即处理，同时带走缓冲区里的文件
        buffered = _msg_buffers.pop(buf_key, [])
        old_task = _msg_buffer_tasks.pop(buf_key, None)
        if old_task and not old_task.done():
            old_task.cancel()

        all_msgs = buffered + [(user_id, chat_id, is_group, msg)]
        await _dispatch_messages(all_msgs)


_INTERNAL_PATTERNS = [
    r'<!-- internal -->.*?<!-- /internal -->',
    r'\*\*FRONTEND SKILL GATE[^*]*\*\*.*?(?=\n\n|\Z)',
    r'\*\*FEISHU PLAN GATE[^*]*\*\*.*?(?=\n\n|\Z)',
    r'\*\*FEISHU DOC FIRST[^*]*\*\*.*?(?=\n\n|\Z)',
    r'TURN-STATE GATE.*?(?=\n\n|\Z)',
    r'Same-file Edit chain.*?(?=\n\n|\Z)',
]
_INTERNAL_RE = re.compile('|'.join(f'(?:{p})' for p in _INTERNAL_PATTERNS), re.DOTALL)


def _strip_internal(text: str) -> str:
    """Strip internal audit blocks, hookify self-checks, and system noise."""
    text = _INTERNAL_RE.sub('', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _chunk_markdown(text: str, max_size: int = 2800) -> list[str]:
    """Split text at paragraph boundaries for Feishu's per-element limit."""
    if len(text) <= max_size:
        return [text]
    chunks, current = [], ""
    for line in text.split('\n'):
        if len(line) > max_size:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), max_size):
                chunks.append(line[i:i + max_size])
            continue
        if len(current) + len(line) + 1 > max_size:
            if current:
                chunks.append(current)
            current = line
        else:
            current = current + '\n' + line if current else line
    if current:
        chunks.append(current)
    return chunks


def _render_tool_detail(tool: dict) -> dict:
    """Render a single tool call as a collapsible panel (PokoClaw pattern).
    Each tool gets: status icon + bold name + summary, expandable detail inside."""
    status = tool.get("status", "running")  # running | completed
    icon = "✅" if status == "completed" else "⏳"
    name = tool["name"]
    summary = tool.get("summary", "")
    detail = tool.get("detail", "")

    title = f"{icon} **{name}** — {summary}" if summary else f"{icon} **{name}**"

    return {
        "tag": "collapsible_panel",
        "expanded": status == "running",  # running tools show expanded
        "header": {
            "title": {"tag": "markdown", "content": title},
            "vertical_align": "center",
            "icon": {"tag": "standard_icon", "token": "down-small-ccm_outlined", "size": "16px 16px"},
            "icon_position": "follow_text",
            "icon_expanded_angle": -180,
        },
        "border": {"color": "grey", "corner_radius": "5px"},
        "vertical_spacing": "8px",
        "padding": "8px 8px 8px 8px",
        "elements": [
            {"tag": "markdown", "content": detail, "text_size": "notation"}
        ] if detail else [],
    }


def _render_tool_history_panel(tools: list[dict], suffix: str = "") -> dict:
    """Render collapsed history panel for prior tools (PokoClaw ☕ pattern)."""
    return {
        "tag": "collapsible_panel",
        "expanded": False,
        "header": {
            "title": {"tag": "markdown", "content": f"☕ **{len(tools)}个工具调用**{suffix}"},
            "vertical_align": "center",
            "icon": {"tag": "standard_icon", "token": "down-small-ccm_outlined", "size": "16px 16px"},
            "icon_position": "follow_text",
            "icon_expanded_angle": -180,
        },
        "border": {"color": "blue", "corner_radius": "5px"},
        "vertical_spacing": "8px",
        "padding": "8px 8px 8px 8px",
        "elements": [_render_tool_detail(t) for t in tools],
    }


def _render_footer(is_running: bool, footer_status: str = "tool_running") -> list[dict]:
    """Render footer with status + stop hint (PokoClaw pattern)."""
    elements = []
    if is_running:
        if footer_status == "thinking":
            elements.append({"tag": "markdown", "content": "🧠 正在思考", "text_size": "notation"})
        else:
            elements.append({"tag": "markdown", "content": "🧰 正在调用工具", "text_size": "notation"})
    return elements


def _build_card_elements(
    tool_history: list[dict],
    accumulated: str,
    is_final: bool = False,
    compressed: bool = False,
) -> list[dict]:
    """Build CardKit 2.0 element array following PokoClaw's design language.

    Tool rendering (PokoClaw pattern):
    - Each tool = its own collapsible_panel with ✅/⏳ + name + summary
    - Prior completed tools collapse into ☕ history panel (blue border)
    - Latest/running tool shows expanded below history
    - Footer: status line below hr

    When compressed=True, tool history is a single text line (no nested panels).
    Triggered when card exceeds Feishu's 30KB element limit.
    """
    elements = []
    response = _strip_internal(accumulated) if accumulated else ""

    # ── Tool section (PokoClaw tool-calls.ts pattern) ──
    if tool_history:
        if compressed:
            count = len(tool_history)
            done = sum(1 for t in tool_history if t.get("status") == "completed")
            label = f"✅ {done}个工具已完成" if done == count else f"⏳ {done}/{count} 个工具"
            elements.append({"tag": "markdown", "content": label})
        elif is_final:
            # Final: all tools in collapsed history panel if >2, else individual panels
            if len(tool_history) > 2:
                elements.append(_render_tool_history_panel(tool_history))
            else:
                for tool in tool_history:
                    elements.append(_render_tool_detail(tool))
        else:
            # Streaming: prior tools in history panel, latest tool expanded
            if len(tool_history) > 1:
                prior = tool_history[:-1]
                elements.append(_render_tool_history_panel(prior, "（已结束）"))
            latest = tool_history[-1]
            elements.append(_render_tool_detail(latest))

    # ── Response section ──
    if response.strip():
        for chunk in _chunk_markdown(response, 2800):
            elements.append({"tag": "markdown", "content": chunk})
    elif not is_final and not tool_history:
        # No tools, no response yet — show thinking (only when there's no footer)
        elements.append({"tag": "markdown", "content": "⏳ 思考中..."})
    elif is_final and not tool_history:
        elements.append({"tag": "markdown", "content": "（无输出）"})
    # When streaming with tools but no response: footer handles the status — no duplicate

    # ── Footer (PokoClaw pattern) ──
    if not is_final and tool_history:
        footer_status = "thinking" if not accumulated.strip() else "tool_running"
        footer = _render_footer(True, footer_status)
        if footer:
            elements.append({"tag": "hr"})
            elements.extend(footer)

    return elements


async def _run_and_display(
    user_id: str, chat_id: str, is_group: bool,
    text: str, card_msg_id: str, session, notify_msg_id: str,
):
    """调用 Claude 并流式展示结果，检测选项时附加按钮。消息处理和按钮回复共用此函数。"""
    active_run = _active_runs.start_run(user_id, card_msg_id)

    accumulated = ""
    tool_history: list[dict] = []  # structured: {name, summary, detail, status}
    ask_options: list[tuple[str, str]] = []  # AskUserQuestion 解析出的选项
    plan_exited = False  # Claude 调了 ExitPlanMode
    plan_file_path = ""  # 检测到的 plan 文件路径
    last_push_time = 0.0
    push_failures = 0
    _PUSH_INTERVAL = 0.4

    # Threshold for permanently disabling mid-stream card updates.
    # Bumped from 3 → 10: Feishu occasionally 429s or has transient API hiccups.
    # With subagent heartbeats (see claude_runner.py), the card stays alive across
    # long subagent runs, so we can afford a more forgiving threshold.
    _PUSH_FAILURE_LIMIT = 10

    _card_compressed = False

    async def push_structured():
        """Push structured CardKit 2.0 element array to card."""
        nonlocal push_failures, _card_compressed
        if push_failures >= _PUSH_FAILURE_LIMIT:
            return
        try:
            elements = _build_card_elements(
                tool_history, accumulated, is_final=False, compressed=_card_compressed,
            )
            await feishu.update_card_elements(card_msg_id, elements)
            push_failures = 0
        except Exception as push_err:
            err_str = str(push_err)
            if ("element exceeds the limit" in err_str or "11310" in err_str) and not _card_compressed:
                _card_compressed = True
                print("[warn] card too large, compressing tool history and retrying", flush=True)
                try:
                    elements = _build_card_elements(
                        tool_history, accumulated, is_final=False, compressed=True,
                    )
                    await feishu.update_card_elements(card_msg_id, elements)
                    push_failures = 0
                    return
                except Exception:
                    pass
            push_failures += 1
            print(
                f"[warn] push structured failed ({push_failures}/{_PUSH_FAILURE_LIMIT}): {push_err}",
                flush=True,
            )
            if push_failures == _PUSH_FAILURE_LIMIT:
                print(
                    "[warn] stopping mid-stream card updates; final result will still be attempted.",
                    flush=True,
                )

    async def on_tool_use(name: str, inp: dict):
        nonlocal accumulated, last_push_time, plan_exited, plan_file_path
        if name.lower() == "exitplanmode":
            plan_exited = True
            return
        if name.lower() == "enterplanmode":
            if session.permission_mode != "plan":
                print(f"[Plan] EnterPlanMode 检测到，切换为 plan", flush=True)
                await store.set_permission_mode(user_id, chat_id, "plan")
            return
        if name.lower() == "enterworktree" and inp:
            wt_name = inp.get("name", "")
            if wt_name:
                print(f"[Worktree] 进入 worktree: {wt_name}", flush=True)
            return
        if name.lower() == "exitworktree":
            print(f"[Worktree] 退出 worktree", flush=True)
            return
        if name.lower() == "askuserquestion":
            question = inp.get("question", inp.get("text", ""))
            if question:
                accumulated += f"\n\n❓ **等待回复：**\n{question}"
                detected = _extract_options(question)
                if detected:
                    ask_options.clear()
                    ask_options.extend(detected)
                await push_structured()
                return
        # Track plan file writes
        if inp and name.lower() in ("write", "write_file", "edit", "edit_file"):
            path = inp.get("file_path", inp.get("path", ""))
            if "/.claude/plans/" in path and path.endswith(".md"):
                plan_file_path = path
                print(f"[Plan] plan file detected: {path}", flush=True)

        tool_obj = _build_tool_object(name, inp)
        if inp and tool_history:
            # Second call (with args) = tool completed, update last entry
            tool_obj["status"] = "completed"
            tool_history[-1] = tool_obj
        else:
            # First call (empty args) = tool starting
            tool_history.append(tool_obj)
        await push_structured()
        last_push_time = time.time()

    async def on_text_chunk(chunk: str):
        nonlocal accumulated, last_push_time
        accumulated += chunk
        now = time.time()
        if now - last_push_time >= _PUSH_INTERVAL:
            await push_structured()
            last_push_time = now

    claude_msg = text
    try:
        print(f"[run_claude] 开始调用...", flush=True)
        full_text, new_session_id, used_fresh_session_fallback = await run_claude(
            message=claude_msg,
            session_id=session.session_id,
            model=session.model,
            cwd=session.cwd,
            permission_mode=session.permission_mode,
            effort=session.effort,
            on_text_chunk=on_text_chunk,
            on_tool_use=on_tool_use,
            on_process_start=lambda proc: _active_runs.attach_process(user_id, proc),
        )
        print(f"[run_claude] 完成, session={new_session_id}", flush=True)
    except Exception as e:
        if active_run.stop_requested:
            return
        print(f"[error] Claude 运行失败: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        try:
            err_el = [
                {"tag": "markdown", "content": f"❌ Claude 执行出错：{type(e).__name__}: {e}"},
                {"tag": "hr"},
                {"tag": "markdown", "content": "❌ 出错", "text_align": "right", "text_size": "notation"},
            ]
            await feishu.update_card_elements(card_msg_id, err_el, header=_card_header("error"))
        except Exception:
            try:
                await feishu.update_card(card_msg_id, f"❌ Claude 执行出错：{type(e).__name__}: {e}")
            except Exception:
                pass
        return
    finally:
        _active_runs.clear_run(user_id, active_run)

    # ── 最终更新卡片（structured CardKit 2.0 elements）──
    # accumulated = 所有 turn 的文本（流式阶段展示的完整内容）
    # full_text = 只有最后一个 turn 的文本（result 事件）
    # 优先 accumulated，保留多 turn 回答的完整性
    final = _strip_internal(accumulated or full_text or "")
    if used_fresh_session_fallback:
        final = (
            "⚠️ 检测到工作目录已变化，旧会话无法继续。"
            "本次已自动切换到新 session。\n\n" + final
        )

    # Build structured final card
    elements = _build_card_elements(tool_history, final, is_final=True)

    # Embed plan content if plan file was written
    if plan_file_path:
        try:
            with open(plan_file_path, "r") as f:
                plan_content = f.read()
            if plan_content.strip():
                # Strip markdown tables — Feishu has a card-wide table count limit
                clean = re.sub(
                    r'^\|.*\|$\n?(?:^\|[-:| ]+\|$\n?)?(?:^\|.*\|$\n?)*',
                    '（表格已省略，见方案文件）\n',
                    plan_content, flags=re.MULTILINE,
                )
                plan_panel = {
                    "tag": "collapsible_panel",
                    "expanded": True,
                    "header": {
                        "title": {"tag": "plain_text",
                                  "content": "📋 方案" + ("（待审核）" if plan_exited else "")},
                        "icon": {"tag": "standard_icon", "token": "down-small-ccm_outlined",
                                 "size": "16px 16px"},
                        "icon_position": "right",
                        "icon_expanded_angle": -180,
                    },
                    "border": {"color": "blue", "corner_radius": "8px"},
                    "vertical_spacing": "8px",
                    "elements": [{"tag": "markdown", "content": chunk}
                                 for chunk in _chunk_markdown(clean, 2800)],
                }
                insert_idx = 0
                for i, el in enumerate(elements):
                    if el.get("tag") == "hr":
                        insert_idx = i + 1
                        break
                elements.insert(insert_idx, plan_panel)
        except Exception as e:
            print(f"[Plan] failed to read plan file: {e}", flush=True)

    # Add option buttons if detected
    options = _extract_options(final) or ask_options
    if options:
        buttons = []
        for display, value in options:
            buttons.append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": display},
                "type": "default", "size": "small",
                "name": f"opt_{value}",
                "value": {"reply": value, "cid": chat_id},
                "behaviors": [{"type": "callback", "value": {"reply": value, "cid": chat_id}}],
            })
        short = all(len(d) <= 10 for d, _ in options)
        if short:
            columns = [{"tag": "column", "width": "auto", "elements": [b]} for b in buttons]
            elements.append({"tag": "column_set", "flex_mode": "flow", "columns": columns})
        else:
            elements.extend(buttons)

    # Add plan approve/revise buttons (BEFORE card patch)
    if plan_exited and session.permission_mode == "plan":
        print(f"[Plan] ExitPlanMode detected, awaiting approval", flush=True)
        elements.append({"tag": "hr"})
        approve_btn = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "✅ 批准执行"},
            "type": "primary", "size": "medium",
            "value": {"action": "plan_approve", "cid": chat_id},
            "behaviors": [{"type": "callback", "value": {"action": "plan_approve", "cid": chat_id}}],
        }
        revise_btn = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "✏️ 修改方案"},
            "type": "default", "size": "medium",
            "value": {"action": "plan_revise", "cid": chat_id},
            "behaviors": [{"type": "callback", "value": {"action": "plan_revise", "cid": chat_id}}],
        }
        columns = [
            {"tag": "column", "width": "auto", "elements": [approve_btn]},
            {"tag": "column", "width": "auto", "elements": [revise_btn]},
        ]
        elements.append({"tag": "column_set", "flex_mode": "flow", "columns": columns})

    # Determine card header based on state
    header = _card_header("plan_review") if plan_exited else _card_header("completed")

    # Footer: right-aligned status (skip for plan review — buttons are the signal)
    if not (plan_exited and session.permission_mode == "plan"):
        elements.append({"tag": "hr"})
        elements.append({"tag": "markdown", "content": "✅ 完成", "text_align": "right", "text_size": "notation"})

    # Try structured card, fall back to plain text
    card_patched = False
    try:
        await feishu.update_card_elements(card_msg_id, elements, header=header)
        card_patched = True
    except Exception as e:
        err_str = str(e)
        if "element exceeds the limit" in err_str or "11310" in err_str:
            print("[warn] final card too large, retrying with compressed tools", flush=True)
            try:
                elements = _build_card_elements(tool_history, accumulated, is_final=True, compressed=True)
                if plan_exited and session.permission_mode == "plan":
                    elements.append({"tag": "column_set", "flex_mode": "flow", "columns": columns})
                if not (plan_exited and session.permission_mode == "plan"):
                    elements.append({"tag": "hr"})
                    elements.append({"tag": "markdown", "content": "✅ 完成", "text_align": "right", "text_size": "notation"})
                await feishu.update_card_elements(card_msg_id, elements, header=header)
                card_patched = True
            except Exception as e_compressed:
                print(f"[error] compressed card also failed: {e_compressed}", flush=True)
        if not card_patched:
            print(f"[error] structured card failed, falling back to text: {e}", flush=True)
            try:
                plain = final or "（无输出）"
                await feishu.update_card(card_msg_id, plain)
                card_patched = True
            except Exception as e2:
                print(f"[error] text card also failed, falling back to message: {e2}", flush=True)
                fallback_content = (
                    "⚠️ 卡片更新失败，以下为本次回复（补发）：\n\n" + (final or "（无输出）")
                )
                try:
                    if is_group and notify_msg_id:
                        await feishu.reply_card(
                            notify_msg_id, content=fallback_content, loading=False
                        )
                    else:
                        await feishu.send_text_to_user(user_id, fallback_content)
                except Exception as fallback_err:
                    print(f"[error] all fallbacks failed: {fallback_err}", flush=True)

    # No standalone ✅ message — card header IS the completion signal

    if new_session_id:
        await store.on_claude_response(user_id, chat_id, new_session_id, text)

    # Plan mode: buttons handle approval/revision. Do NOT auto-switch here.
    # (Old auto-switch block deleted — approve button handles it now.)

    # Dispatch any pending /btw messages
    pending = _btw_pending.pop(user_id, [])
    if pending:
        combined = "\n".join(f"[追加] {p}" for p in pending)
        print(f"[btw] dispatching {len(pending)} queued items", flush=True)
        asyncio.ensure_future(_auto_dispatch_btw(user_id, chat_id, is_group, combined))


async def _auto_dispatch_btw(user_id: str, chat_id: str, is_group: bool, text: str):
    """Auto-dispatch queued /btw messages after current run completes."""
    if chat_id not in _chat_locks:
        _chat_locks[chat_id] = asyncio.Lock()
    async with _chat_locks[chat_id]:
        try:
            session = await store.get_current(user_id, chat_id)
            card = await feishu.send_card_to_user(user_id, loading=True)
            await _run_and_display(user_id, chat_id, is_group, text, card, session, "")
        except Exception as e:
            print(f"[btw] auto-dispatch failed: {e}", flush=True)


async def _dispatch_buffered(buf_key: str):
    """debounce 到期后刷出缓冲区（仅文件/图片场景）"""
    msgs = _msg_buffers.pop(buf_key, [])
    _msg_buffer_tasks.pop(buf_key, None)
    if msgs:
        await _dispatch_messages(msgs)


async def _dispatch_messages(msgs: list):
    """将一组消息（可能1条或多条）发给 Claude 处理"""
    if not msgs:
        return
    first_uid, first_cid, first_is_group, first_msg = msgs[0]
    print(f"[dispatch] 处理 {len(msgs)} 条消息", flush=True)

    # /btw: queue without interrupting active run
    if first_msg.message_type == "text":
        try:
            raw = json.loads(first_msg.content).get("text", "").strip()
        except Exception:
            raw = ""
        # Strip @mentions for group chat
        if first_is_group:
            for m in (getattr(first_msg, 'mentions', None) or []):
                key = getattr(m, 'key', '')
                if key:
                    raw = raw.replace(key, '').strip()
        if raw.lower().startswith("/btw") and _active_runs.get_run(first_uid):
            content = raw[4:].strip()
            if content:
                _btw_pending.setdefault(first_uid, []).append(content)
                try:
                    await feishu.add_reaction(first_msg.message_id, "PUSHPIN")
                except Exception as e:
                    print(f"[btw] reaction failed (non-fatal): {e}", flush=True)
                print(f"[btw] queued for {first_uid[:8]}: {content[:50]}", flush=True)
            return  # don't interrupt

    active = _active_runs.get_run(first_uid)
    if active and not active.stop_requested:
        _btw_pending.pop(first_uid, None)  # clear stale btw on interrupt
        print(f"[打断] 新消息到达，自动停止当前任务", flush=True)
        await stop_run(_active_runs, first_uid, on_stopped=_announce_interrupted)

    if first_cid not in _chat_locks:
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[first_cid] = asyncio.Lock()
    lock = _chat_locks[first_cid]

    async with lock:
        try:
            if len(msgs) == 1:
                await _process_message(first_uid, first_cid, first_is_group, msgs[0][3])
            else:
                await _process_bundled_messages(first_uid, first_cid, first_is_group, msgs)
        except Exception as e:
            print(f"[error] 消息处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()


async def _transcribe_audio(audio_path: str, timeout_s: int = 60) -> str:
    """调用 scripts/feishu-audio-transcribe.py（用 system python3 + faster-whisper）转写语音。
    返回纯文本 transcript；失败返回 ""。超时/异常都吞掉，不阻塞 bridge。"""
    workspace = config.DEFAULT_CWD
    script = os.path.join(workspace, "scripts", "feishu-audio-transcribe.py")
    try:
        proc = await asyncio.create_subprocess_exec(
            "/usr/bin/env", "python3", script, audio_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        except asyncio.TimeoutError:
            proc.kill()
            print(f"[audio] transcribe timeout ({timeout_s}s)", flush=True)
            return ""
        if proc.returncode != 0:
            print(f"[audio] transcribe rc={proc.returncode}: {stderr.decode('utf-8', 'ignore')[:200]}", flush=True)
            return ""
        return stdout.decode("utf-8", "ignore").strip()
    except Exception as e:
        print(f"[audio] subprocess error: {e}", flush=True)
        return ""


async def _extract_message_text(msg, feishu_client: FeishuClient) -> str:
    """从飞书消息中提取文本（供聚合使用），返回空字符串表示不支持的类型。"""
    if msg.message_type == "text":
        try:
            return json.loads(msg.content).get("text", "").strip()
        except Exception:
            return ""

    elif msg.message_type == "image":
        try:
            image_key = json.loads(msg.content).get("image_key", "")
            if not image_key:
                return ""
            img_path = await feishu_client.download_image(msg.message_id, image_key)
            return f"[用户发送了一张图片，路径：{img_path}，请用Read工具读取并分析]"
        except Exception as e:
            print(f"[error] 聚合下载图片失败: {e}", flush=True)
            return ""

    elif msg.message_type == "post":
        try:
            post = json.loads(msg.content)
            if "content" in post and isinstance(post.get("content"), list):
                lang = post
            else:
                lang = post.get("zh_cn") or post.get("en_us") or next((v for v in post.values() if isinstance(v, dict)), {})
            parts = []
            image_keys = []
            for para in lang.get("content", []):
                for el in para:
                    if el.get("tag") == "text":
                        parts.append(el.get("text", ""))
                    elif el.get("tag") == "a":
                        parts.append(f"{el.get('text', '')}({el.get('href', '')})")
                    elif el.get("tag") == "img":
                        key = el.get("image_key", "")
                        if key:
                            image_keys.append(key)
            text = " ".join(parts).strip()
            if image_keys:
                img_paths = []
                for ik in image_keys:
                    try:
                        p = await feishu_client.download_image(msg.message_id, ik)
                        img_paths.append(p)
                    except Exception as e:
                        print(f"[error] 聚合post图片下载失败: {e}", flush=True)
                if img_paths:
                    paths_str = "、".join(img_paths)
                    if text:
                        text += f"\n\n[用户同时发送了{len(img_paths)}张图片，路径：{paths_str}，请用Read工具读取并分析]"
                    else:
                        text = f"[用户发送了{len(img_paths)}张图片，路径：{paths_str}，请用Read工具读取并分析]"
            return text
        except Exception as e:
            print(f"[error] 聚合解析富文本失败: {e}", flush=True)
            return ""

    elif msg.message_type == "file":
        try:
            content_data = json.loads(msg.content)
            file_key = content_data.get("file_key", "")
            file_name = content_data.get("file_name", "unknown")
            if not file_key:
                return ""
            file_path = await feishu_client.download_file(msg.message_id, file_key, file_name)
            _, ext = os.path.splitext(file_name.lower())
            if ext == ".pdf":
                return f"[用户发送了PDF文件「{file_name}」，路径：{file_path}，请用Read工具读取]"
            elif ext in (".doc", ".docx"):
                return f"[用户发送了Word文档「{file_name}」，路径：{file_path}，请用python3和python-docx提取内容]"
            elif ext in (".xls", ".xlsx"):
                return f"[用户发送了Excel文件「{file_name}」，路径：{file_path}，请用python3和openpyxl提取内容]"
            elif ext in (".ppt", ".pptx"):
                return f"[用户发送了PPT文件「{file_name}」，路径：{file_path}，请用python3和python-pptx提取内容]"
            elif ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg"):
                return f"[用户发送了图片文件「{file_name}」，路径：{file_path}，请用Read工具读取]"
            else:
                return f"[用户发送了文件「{file_name}」（{ext or '未知格式'}），路径：{file_path}，请读取并分析]"
        except Exception as e:
            print(f"[error] 聚合下载文件失败: {e}", flush=True)
            return ""

    elif msg.message_type == "audio":
        try:
            content_data = json.loads(msg.content)
            file_key = content_data.get("file_key", "")
            duration_ms = content_data.get("duration", 0)
            if not file_key:
                return ""
            # Feishu voice messages are .opus — pass a synthetic file_name so downloader gives correct ext
            audio_path = await feishu_client.download_file(msg.message_id, file_key, "voice.opus")
            transcript = await _transcribe_audio(audio_path)
            if not transcript:
                return f"[用户发送了语音消息（{duration_ms}ms），但转写失败]"
            return f"[用户语音，{duration_ms}ms]: {transcript}"
        except Exception as e:
            print(f"[error] 聚合转写语音失败: {e}", flush=True)
            return ""

    return ""


async def _process_bundled_messages(user_id: str, chat_id: str, is_group: bool, msgs: list):
    """将多条快速连续到达的消息合并为一条发给 Claude"""
    parts = []
    first_msg_id = msgs[0][3].message_id

    for _, _, _, msg in msgs:
        # 群聊去 @mention
        text = await _extract_message_text(msg, feishu)
        if is_group and msg.message_type == "text":
            mentions = getattr(msg, 'mentions', None) or []
            for mention in mentions:
                key = getattr(mention, 'key', '')
                if key:
                    text = text.replace(key, '').strip()
        if text:
            parts.append(text)

    if not parts:
        return

    combined = "\n\n".join(parts)
    print(f"[聚合] 合并 {len(parts)} 条消息为一条 prompt", flush=True)

    # 检查是否为命令（仅当第一条是纯文本命令时）
    parsed = parse_command(parts[0])
    if parsed and len(parts) == 1:
        cmd, args = parsed
        reply = await handle_command(cmd, args, user_id, chat_id, store)
        if reply is not None:
            reply_text = reply["text"] if isinstance(reply, dict) else reply
            if is_group:
                await feishu.reply_card(first_msg_id, content=reply_text, loading=False)
            else:
                await feishu.send_card_to_user(user_id, content=reply_text, loading=False)
            return

    session = await store.get_current(user_id, chat_id)

    try:
        if is_group:
            card_msg_id = await feishu.reply_card(first_msg_id, loading=True)
        else:
            card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
    except Exception as e:
        print(f"[error] 发送占位卡片失败: {e}", flush=True)
        return

    await _run_and_display(user_id, chat_id, is_group, combined, card_msg_id, session, first_msg_id)


async def _process_message(user_id: str, chat_id: str, is_group: bool, msg):
    """实际处理消息的逻辑，在 per-chat lock 保护下执行"""
    print(f"[处理消息] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)
    text = ""
    img_path = None

    if msg.message_type == "text":
        try:
            text = json.loads(msg.content).get("text", "").strip()
        except Exception:
            return
        if not text:
            return

        # 群聊：去掉 @mention 占位符
        if is_group:
            mentions = getattr(msg, 'mentions', None) or []
            for mention in mentions:
                key = getattr(mention, 'key', '')
                if key:
                    text = text.replace(key, '').strip()
            if not text:
                return

        print(f"[文本] {text[:50]}", flush=True)

    elif msg.message_type == "image":
        try:
            image_key = json.loads(msg.content).get("image_key", "")
            if not image_key:
                return
            img_path = await feishu.download_image(msg.message_id, image_key)
            text = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片，直接回复用中文]"
        except Exception as e:
            print(f"[error] 下载图片失败: {e}")
            if is_group:
                try:
                    await feishu.reply_card(msg.message_id, content=f"❌ 下载图片失败：{e}", loading=False)
                except Exception:
                    pass
            else:
                await feishu.send_text_to_user(user_id, f"❌ 下载图片失败：{e}")
            return

    elif msg.message_type == "post":
        # 富文本消息：提取文字和图片
        try:
            post = json.loads(msg.content)
            # post 可能直接是 {"title":"","content":[[...]]} 或者包裹在语言 key 里 {"zh_cn":{"title":"","content":[[...]]}}
            if "content" in post and isinstance(post.get("content"), list):
                lang = post
            else:
                lang = post.get("zh_cn") or post.get("en_us") or next((v for v in post.values() if isinstance(v, dict)), {})
            parts = []
            image_keys = []
            for para in lang.get("content", []):
                for el in para:
                    if el.get("tag") == "text":
                        parts.append(el.get("text", ""))
                    elif el.get("tag") == "a":
                        parts.append(f"{el.get('text', '')}({el.get('href', '')})")
                    elif el.get("tag") == "img":
                        key = el.get("image_key", "")
                        if key:
                            image_keys.append(key)
            text = " ".join(parts).strip()
            if image_keys:
                img_paths = []
                for ik in image_keys:
                    try:
                        p = await feishu.download_image(msg.message_id, ik)
                        img_paths.append(p)
                    except Exception as e:
                        print(f"[error] post图片下载失败: {e}")
                if img_paths:
                    paths_str = "、".join(img_paths)
                    if text:
                        text += f"\n\n[用户同时发送了{len(img_paths)}张图片，路径：{paths_str}，请用Read工具读取并分析]"
                    else:
                        text = f"[用户发送了{len(img_paths)}张图片，路径：{paths_str}，请用Read工具读取并分析，直接回复用中文]"
            if not text:
                return
            print(f"[富文本] {text[:50]}", flush=True)
        except Exception as e:
            print(f"[error] 解析富文本失败: {e}")
            return

    elif msg.message_type == "file":
        # 文件消息：Word/PDF/Excel/PPT/文本等
        try:
            content_data = json.loads(msg.content)
            file_key = content_data.get("file_key", "")
            file_name = content_data.get("file_name", "unknown")
            if not file_key:
                return
            file_path = await feishu.download_file(msg.message_id, file_key, file_name)
            _, ext = os.path.splitext(file_name.lower())

            if ext == ".pdf":
                text = f"[用户发送了一个PDF文件「{file_name}」，路径：{file_path}，请用Read工具读取并分析，直接回复用中文]"
            elif ext in (".doc", ".docx"):
                text = f"[用户发送了一个Word文档「{file_name}」，路径：{file_path}，请用python3和python-docx提取文档内容（pip install python-docx），然后分析，直接回复用中文]"
            elif ext in (".xls", ".xlsx"):
                text = f"[用户发送了一个Excel文件「{file_name}」，路径：{file_path}，请用python3和openpyxl提取内容（pip install openpyxl），然后分析，直接回复用中文]"
            elif ext in (".ppt", ".pptx"):
                text = f"[用户发送了一个PPT文件「{file_name}」，路径：{file_path}，请用python3和python-pptx提取内容（已安装在scripts/pptx-venv/），然后分析，直接回复用中文]"
            elif ext in (".txt", ".csv", ".json", ".md", ".py", ".js", ".html", ".css", ".xml",
                          ".yaml", ".yml", ".log", ".sh", ".sql", ".r", ".ts", ".tsx", ".jsx",
                          ".swift", ".kt", ".java", ".c", ".cpp", ".h", ".go", ".rs", ".rb"):
                text = f"[用户发送了一个文本文件「{file_name}」，路径：{file_path}，请用Read工具读取并分析，直接回复用中文]"
            elif ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg"):
                text = f"[用户发送了一张图片文件「{file_name}」，路径：{file_path}，请用Read工具读取并分析，直接回复用中文]"
            else:
                text = f"[用户发送了一个文件「{file_name}」（{ext or '未知格式'}），路径：{file_path}，请根据文件类型选择合适的方式读取并分析，直接回复用中文]"

            print(f"[文件] {file_name} -> {file_path}", flush=True)
        except Exception as e:
            print(f"[error] 下载文件失败: {e}")
            if is_group:
                try:
                    await feishu.reply_card(msg.message_id, content=f"❌ 下载文件失败：{e}", loading=False)
                except Exception:
                    pass
            else:
                await feishu.send_text_to_user(user_id, f"❌ 下载文件失败：{e}")
            return

    elif msg.message_type == "audio":
        # 语音消息：下载 + faster-whisper 转写
        try:
            content_data = json.loads(msg.content)
            file_key = content_data.get("file_key", "")
            duration_ms = content_data.get("duration", 0)
            if not file_key:
                return
            audio_path = await feishu.download_file(msg.message_id, file_key, "voice.opus")
            transcript = await _transcribe_audio(audio_path)
            if not transcript:
                text = f"[用户发送了语音消息（{duration_ms}ms），转写失败。请回复让用户改发文字。]"
            else:
                text = f"[用户语音，{duration_ms}ms]: {transcript}"
            print(f"[语音] {duration_ms}ms -> {transcript[:60] if transcript else 'FAIL'}", flush=True)
        except Exception as e:
            print(f"[error] 语音转写失败: {e}")
            if is_group:
                try:
                    await feishu.reply_card(msg.message_id, content=f"❌ 语音转写失败：{e}", loading=False)
                except Exception:
                    pass
            else:
                await feishu.send_text_to_user(user_id, f"❌ 语音转写失败：{e}")
            return

    else:
        print(f"[skip] 不支持的消息类型: {msg.message_type}", flush=True)
        notice = f"⚠️ 暂不支持此消息类型（{msg.message_type}）。目前支持：文字、图片、富文本、文件（PDF/Word/Excel/PPT等）、语音。"
        try:
            if is_group:
                await feishu.reply_card(msg.message_id, content=notice, loading=False)
            else:
                await feishu.send_text_to_user(user_id, notice)
        except Exception:
            pass
        return

    # ── 斜杠命令 ──────────────────────────────────────────────
    parsed = parse_command(text)
    if parsed:
        cmd, args = parsed
        print(f"[cmd] 执行命令 {cmd}", flush=True)
        reply = await handle_command(cmd, args, user_id, chat_id, store)
        print(f"[cmd] 命令返回 type={type(reply).__name__}", flush=True)
        if reply is not None:
            if isinstance(reply, dict):
                reply_text, reply_buttons = reply["text"], reply.get("buttons", [])
            else:
                reply_text, reply_buttons = reply, []

            if reply_buttons:
                if is_group:
                    card_id = await feishu.reply_card(msg.message_id, content=reply_text, loading=False)
                else:
                    card_id = await feishu.send_card_to_user(user_id, content=reply_text, loading=False)
                print(f"[按钮] 卡片已发送 card_id={card_id}, 准备添加 {len(reply_buttons)} 个按钮", flush=True)
                try:
                    short = all(len(b["text"]) <= 12 for b in reply_buttons)
                    await feishu.update_card_with_buttons(card_id, reply_text, reply_buttons, flow=short)
                    print(f"[按钮] 按钮添加成功", flush=True)
                except Exception as btn_err:
                    print(f"[按钮] 按钮添加失败: {btn_err}", flush=True)
            else:
                if is_group:
                    await feishu.reply_card(msg.message_id, content=reply_text, loading=False)
                else:
                    await feishu.send_card_to_user(user_id, content=reply_text, loading=False)
            return
        # reply is None → 不是 bot 命令
        # Keep / for known Claude skills so CLI invokes the skill.
        # Strip / for casual shorthand (/btw, /fyi, /ps, etc.) so Claude
        # reads as natural language.
        # Heuristic: Claude skills use lowercase-hyphenated names or plugin:skill
        # format. Single short words that aren't known skills get stripped.
        _CLAUDE_SKILLS = {
            # Claude CLI built-ins (work in --print mode)
            "context", "compact", "cost",
            # Built-in skills
            "commit", "review", "init", "security-review", "simplify",
            "fewer-permission-prompts", "loop", "schedule", "claude-api",
            "grill-me", "update-config", "keybindings-help",
            # Plugins (base name — colon-namespaced handled separately)
            "hookify", "skill-creator", "frontend-design",
            "claude-md-management", "claude-md-improver", "revise-claude-md",
            # anthropic-skills
            "xlsx", "pdf", "pptx", "docx", "consolidate-memory", "setup-cowork",
        }
        # Colon-namespaced skills (hookify:list, anthropic-skills:pdf) always pass through
        is_skill = cmd in _CLAUDE_SKILLS or ":" in cmd
        if not is_skill:
            text = text.lstrip("/")

    # ── 普通消息 → 调用 Claude ──────────────────────────────
    session = await store.get_current(user_id, chat_id)
    print(f"[Claude] session={session.session_id} model={session.model}", flush=True)

    # 1. 发送"思考中"占位卡片，拿到 message_id
    try:
        if is_group:
            card_msg_id = await feishu.reply_card(msg.message_id, loading=True)
        else:
            card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
        print(f"[卡片] card_msg_id={card_msg_id}", flush=True)
    except Exception as e:
        print(f"[error] 发送占位卡片失败: {e}", flush=True)
        if is_group:
            try:
                await feishu.reply_card(msg.message_id, content=f"❌ 发送消息失败：{e}", loading=False)
            except Exception:
                pass
        else:
            await feishu.send_text_to_user(user_id, f"❌ 发送消息失败：{e}")
        return

    await _run_and_display(user_id, chat_id, is_group, text, card_msg_id, session, msg.message_id)


def _extract_options(text: str) -> list[tuple[str, str]]:
    """从文本中提取选项，适配 Claude Code 原生输出格式。返回 [(按钮文字, 回复值), ...]"""
    lines = text.strip().split('\n')

    # 从末尾向上扫描连续的编号选项
    option_lines = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            if option_lines:
                break
            continue
        # 匹配: 1. xxx / 1) xxx / 1、xxx / a) xxx / A) xxx
        m = re.match(r'^(\d+|[a-zA-Z])[.）\)、]\s*(.+)', line)
        if m:
            option_lines.append((m.group(1), m.group(2).strip()))
        elif option_lines:
            break
        else:
            break
    option_lines.reverse()
    if len(option_lines) >= 2:
        return [
            (f"{key}. {desc}" if len(desc) <= 18 else f"{key}. {desc[:16]}..", key)
            for key, desc in option_lines
        ]

    # Y/N 及变体
    tail = "\n".join(lines[-3:]) if len(lines) >= 3 else text
    if re.search(r'\by\b.*\bn\b|Y/N|yes.*no|是/否|确认/取消', tail, re.IGNORECASE):
        return [("Yes", "yes"), ("No", "no")]

    return []


def _shorten_path(path: str) -> str:
    """Strip workspace prefix from paths for cleaner display."""
    home = os.path.expanduser("~")
    cwd = config.DEFAULT_CWD
    # Strip workspace first (more specific), then home
    if cwd and cwd != home:
        path = path.replace(cwd + '/', '').replace(cwd, '')
    return path.replace(home + '/', '~/').replace(home, '~')


def _build_tool_object(name: str, inp: dict) -> dict:
    """Build structured tool dict for PokoClaw-style card rendering."""
    n = name.lower()
    obj = {"name": name, "summary": "", "detail": "", "status": "running"}

    if n == "bash":
        cmd = inp.get("command", "")
        cmd_clean = re.sub(r'^cd\s+"[^"]*"\s*&&\s*', '', cmd)
        obj["name"] = "bash"
        obj["summary"] = cmd_clean[:80] + "..." if len(cmd_clean) > 80 else cmd_clean
        if cmd:
            obj["detail"] = f"**Command**\n```bash\n{cmd_clean[:240]}\n```"
    elif n in ("read_file", "read"):
        path = _shorten_path(inp.get('file_path', inp.get('path', '')))
        obj["name"] = "read"
        obj["summary"] = path
        obj["detail"] = f"`{path}`" if path else ""
    elif n in ("write_file", "write"):
        path = _shorten_path(inp.get('file_path', inp.get('path', '')))
        obj["name"] = "write"
        obj["summary"] = path
        obj["detail"] = f"`{path}`" if path else ""
    elif n in ("edit_file", "edit"):
        path = _shorten_path(inp.get('file_path', inp.get('path', '')))
        obj["name"] = "edit"
        obj["summary"] = path
        obj["detail"] = f"`{path}`" if path else ""
    elif n in ("glob",):
        obj["name"] = "glob"
        obj["summary"] = inp.get('pattern', '')
    elif n in ("grep",):
        obj["name"] = "grep"
        obj["summary"] = inp.get('pattern', '')
    elif n == "task":
        obj["name"] = "task"
        obj["summary"] = inp.get('description', inp.get('prompt', '')[:60])
    elif n == "webfetch":
        obj["name"] = "web_fetch"
        obj["summary"] = "抓取网页"
    elif n == "websearch":
        obj["name"] = "web_search"
        obj["summary"] = inp.get('query', '')
    elif n.startswith("[subagent]"):
        obj["name"] = name  # keep as-is: [subagent] Grep, [subagent] ⏳
        obj["summary"] = ""
    else:
        obj["name"] = name

    return obj


def _format_tool(name: str, inp: dict) -> str:
    """格式化工具调用的进度提示 — compact for Feishu cards"""
    n = name.lower()
    if n == "bash":
        cmd = inp.get("command", "")
        # Strip cd to workspace prefix
        cmd = re.sub(r'^cd\s+"[^"]*Reighst Claude[^"]*"\s*&&\s*', '', cmd)
        if len(cmd) > 60:
            cmd = cmd[:57] + "..."
        return f"🔧 `{cmd}`" if cmd else "🔧 执行命令..."
    elif n in ("read_file", "read"):
        return f"📄 `{_shorten_path(inp.get('file_path', inp.get('path', '')))}`"
    elif n in ("write_file", "write"):
        return f"✏️ `{_shorten_path(inp.get('file_path', inp.get('path', '')))}`"
    elif n in ("edit_file", "edit"):
        return f"✂️ `{_shorten_path(inp.get('file_path', inp.get('path', '')))}`"
    elif n in ("glob",):
        return f"🔍 `{inp.get('pattern', '')}`"
    elif n in ("grep",):
        return f"🔎 `{inp.get('pattern', '')}`"
    elif n == "task":
        desc = inp.get('description', inp.get('prompt', '')[:40])
        return f"🤖 {desc}"
    elif n == "webfetch":
        return "🌐 抓取网页..."
    elif n == "websearch":
        return f"🔍 {inp.get('query', '')}"
    else:
        return f"⚙️ {name}"


# ── 飞书事件回调（同步）→ 调度异步任务 ───────────────────────

# ── 卡片按钮点击处理（选项选择）──────────────────────────────

def on_card_action(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
    """用户点击卡片按钮：选项回复 or 模式切换"""
    global _last_event
    _last_event = time.time()

    event = data.event
    user_id = event.operator.open_id
    value = event.action.value or {}
    action_type = value.get("action", "")
    chat_id = value.get("cid", user_id)
    clicked_msg_id = event.context.open_message_id if event.context else None

    # 模式切换按钮
    if action_type == "set_mode":
        mode = value.get("mode", "")
        if mode:
            asyncio.ensure_future(_handle_set_mode(user_id, chat_id, mode, clicked_msg_id))
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "success"
        toast.content = f"已切换: {mode}"
        resp.toast = toast
        return resp

    # 命令菜单按钮 → 当作用户发了一条命令消息
    if action_type == "run_cmd":
        cmd_text = value.get("cmd", "")
        if cmd_text and _ws_loop:
            asyncio.ensure_future(_handle_menu_command(user_id, chat_id, cmd_text, clicked_msg_id))
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = cmd_text
        resp.toast = toast
        return resp

    # 方案批准按钮
    if action_type == "plan_approve":
        asyncio.ensure_future(_handle_plan_approve(user_id, chat_id, clicked_msg_id))
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "success"
        toast.content = "方案已批准"
        resp.toast = toast
        return resp

    # 方案修改按钮
    if action_type == "plan_revise":
        asyncio.ensure_future(_handle_plan_revise(user_id, chat_id, clicked_msg_id))
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = "请发送修改意见"
        resp.toast = toast
        return resp

    # 恢复会话按钮
    if action_type == "resume_session":
        sid = value.get("sid", "")
        if sid:
            asyncio.ensure_future(_handle_resume_session(user_id, chat_id, sid, clicked_msg_id))
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = "正在恢复..."
        resp.toast = toast
        return resp

    # 选项回复按钮（发给 Claude）
    reply_text = value.get("reply", "")
    if reply_text:
        print(f"[按钮] user={user_id[:8]}... reply={reply_text}", flush=True)
        asyncio.ensure_future(_handle_button_reply(user_id, chat_id, reply_text, clicked_msg_id))

    resp = P2CardActionTriggerResponse()
    toast = CallBackToast()
    toast.type = "info"
    toast.content = f"已发送: {reply_text}"
    resp.toast = toast
    return resp


async def _handle_menu_command(user_id: str, chat_id: str, cmd_text: str, card_msg_id: str):
    """命令菜单按钮点击 → 执行命令并更新卡片"""
    is_group = (chat_id != user_id)
    parsed = parse_command(cmd_text)
    if not parsed:
        return
    cmd, args = parsed

    # /stop 特殊处理
    if cmd == "stop":
        reply_text = await _handle_stop_command(user_id)
        if card_msg_id:
            try:
                await feishu.update_card(card_msg_id, reply_text)
            except Exception:
                pass
        return

    reply = await handle_command(cmd, args, user_id, chat_id, store)
    if reply is None:
        return

    if isinstance(reply, dict):
        reply_text, reply_buttons = reply["text"], reply.get("buttons", [])
    else:
        reply_text, reply_buttons = reply, []

    if card_msg_id:
        try:
            if reply_buttons:
                short = all(len(b["text"]) <= 12 for b in reply_buttons)
                await feishu.update_card_with_buttons(card_msg_id, reply_text, reply_buttons, flow=short)
            else:
                await feishu.update_card(card_msg_id, reply_text)
        except Exception as e:
            print(f"[error] 菜单命令卡片更新失败: {e}", flush=True)


async def _handle_resume_session(user_id: str, chat_id: str, session_id: str, card_msg_id: str):
    """卡片按钮恢复历史会话"""
    sid, old_title = await store.resume_session(user_id, chat_id, session_id)
    if not sid:
        print(f"[resume] 未找到 session: {session_id[:8]}", flush=True)
        return
    print(f"[resume] 已恢复 session: {sid[:8]}", flush=True)
    if card_msg_id:
        try:
            name = store.get_summary(user_id, sid) or f"#{sid[:8]}"
            text = f"✅ 已恢复会话「{name}」，继续对话吧。"
            if old_title:
                text += f"\n上个会话：「{old_title}」"
            await feishu.update_card(card_msg_id, text)
        except Exception:
            pass


async def _handle_plan_approve(user_id: str, chat_id: str, card_msg_id: str):
    """用户批准方案 → 切换到执行模式 + 自动开始"""
    await store.set_permission_mode(user_id, chat_id, "bypassPermissions")
    print(f"[Plan] approved, switching to bypassPermissions", flush=True)

    # Update card: green header, confirmation
    if card_msg_id:
        try:
            elements = [{"tag": "markdown", "content": "✅ **方案已批准** — 开始执行"}]
            await feishu.update_card_elements(card_msg_id, elements, header=_card_header("plan_approved"))
        except Exception:
            try:
                await feishu.update_card(card_msg_id, "✅ 方案已批准，开始执行。")
            except Exception:
                pass

    # Auto-dispatch to Claude → starts implementation
    is_group = (chat_id != user_id)
    if chat_id not in _chat_locks:
        _chat_locks[chat_id] = asyncio.Lock()
    async with _chat_locks[chat_id]:
        try:
            session = await store.get_current(user_id, chat_id)
            new_card = await feishu.send_card_to_user(user_id, loading=True)
            await _run_and_display(user_id, chat_id, is_group,
                "Plan approved. Execute the plan.", new_card, session, "")
        except Exception as e:
            print(f"[Plan] auto-execution failed: {e}", flush=True)


async def _handle_plan_revise(user_id: str, chat_id: str, card_msg_id: str):
    """用户要修改方案 → 更新卡片，等待文字反馈"""
    print(f"[Plan] revision requested", flush=True)
    if card_msg_id:
        try:
            await feishu.update_card(card_msg_id, "✏️ 请发送修改意见，我会据此修改方案。")
        except Exception:
            pass
    # Stay in plan mode. User's next text → Claude revises → ExitPlanMode → cycle repeats.


async def _handle_set_mode(user_id: str, chat_id: str, mode: str, card_msg_id: str):
    """卡片按钮切换权限模式"""
    from commands import VALID_MODES
    await store.set_permission_mode(user_id, chat_id, mode)
    desc = VALID_MODES.get(mode, "")
    print(f"[模式切换] user={user_id[:8]}... mode={mode}", flush=True)
    if card_msg_id:
        try:
            await feishu.update_card(card_msg_id, f"✅ 已切换为 **{mode}**\n{desc}")
        except Exception:
            pass


async def _handle_button_reply(user_id: str, chat_id: str, text: str, clicked_msg_id: str):
    """按钮点击 → 走正常的 lock + Claude 流程"""
    is_group = (chat_id != user_id)

    # 自动打断活跃任务
    active = _active_runs.get_run(user_id)
    if active and not active.stop_requested:
        await stop_run(_active_runs, user_id, on_stopped=_announce_interrupted)

    if chat_id not in _chat_locks:
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[chat_id] = asyncio.Lock()
    lock = _chat_locks[chat_id]

    async with lock:
        try:
            session = await store.get_current(user_id, chat_id)
            try:
                if is_group and clicked_msg_id:
                    card_msg_id = await feishu.reply_card(clicked_msg_id, loading=True)
                else:
                    card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
            except Exception as e:
                print(f"[error] 按钮回复占位卡片失败: {e}", flush=True)
                return
            await _run_and_display(
                user_id, chat_id, is_group, text,
                card_msg_id, session, clicked_msg_id or "",
            )
        except Exception as e:
            print(f"[error] 按钮回复处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)


# ── 飞书事件回调（同步）→ 调度异步任务 ───────────────────────

def on_message_receive(data: P2ImMessageReceiveV1) -> None:
    """
    飞书 SDK 同步回调。
    ws.Client 内部运行 asyncio loop，此处用 ensure_future 调度异步任务。
    """
    global _last_event, _ws_loop
    _last_event = time.time()
    if _ws_loop is None:
        _ws_loop = asyncio.get_event_loop()
    asyncio.ensure_future(handle_message_async(data))


# ── 卡片回调 HTTP 服务（配合 ngrok 暴露给飞书）────────────────

class _CardCallbackHandler(BaseHTTPRequestHandler):
    """处理飞书卡片按钮点击的 HTTP 回调"""

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
        except Exception:
            self._respond(400, {"error": "bad json"})
            return

        # 飞书 URL 验证
        if data.get("type") == "url_verification":
            self._respond(200, {"challenge": data.get("challenge", "")})
            return

        event = data.get("event", {})
        operator = event.get("operator", {})
        user_id = operator.get("open_id", "")
        action = event.get("action", {})
        value = action.get("value", {})
        context = event.get("context", {})

        action_type = value.get("action", "")
        chat_id = value.get("cid", user_id)
        clicked_msg_id = context.get("open_message_id", "")

        print(f"[HTTP回调] user={user_id[:8]}... action={action_type or 'reply'}", flush=True)

        if action_type == "set_mode":
            mode = value.get("mode", "")
            if mode and _ws_loop:
                asyncio.run_coroutine_threadsafe(
                    _handle_set_mode(user_id, chat_id, mode, clicked_msg_id),
                    _ws_loop,
                )
            self._respond(200, {"toast": {"type": "success", "content": f"已切换: {mode}"}})
        elif action_type == "run_cmd":
            cmd_text = value.get("cmd", "")
            if cmd_text and _ws_loop:
                asyncio.run_coroutine_threadsafe(
                    _handle_menu_command(user_id, chat_id, cmd_text, clicked_msg_id),
                    _ws_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": cmd_text}})
        elif action_type == "resume_session":
            sid = value.get("sid", "")
            if sid and _ws_loop:
                asyncio.run_coroutine_threadsafe(
                    _handle_resume_session(user_id, chat_id, sid, clicked_msg_id),
                    _ws_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": "正在恢复..."}})
        else:
            reply_text = value.get("reply", "")
            if reply_text and _ws_loop:
                asyncio.run_coroutine_threadsafe(
                    _handle_button_reply(user_id, chat_id, reply_text, clicked_msg_id),
                    _ws_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": f"已发送: {reply_text}"}})

    def _respond(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # 静默 HTTP 日志


# ── 后台定时摘要生成 ─────────────────────────────────────────

def _bg_summary_thread():
    """后台线程: 每 10 分钟扫描未摘要的会话，逐个生成摘要"""
    time.sleep(60)  # 启动后等 1 分钟再开始
    while True:
        try:
            unsummarized = store.get_all_unsummarized()
            if unsummarized:
                print(f"[摘要] 发现 {len(unsummarized)} 个未摘要会话", flush=True)
                count = 0
                for user_id, sid in unsummarized[:5]:
                    try:
                        summary = generate_summary(sid)
                        if summary:
                            store._data.setdefault(user_id, {}).setdefault("summaries", {})[sid] = summary
                            _write_custom_title(sid, summary)
                            count += 1
                            print(f"[摘要] #{sid[:8]} → {summary}", flush=True)
                    except Exception as e:
                        print(f"[摘要] #{sid[:8]} 失败: {e}", flush=True)
                    time.sleep(5)  # 每个请求间隔 5 秒，避免 429
                if count:
                    store._save()  # 同步原子写入
                    print(f"[摘要] 本轮完成 {count}/{len(unsummarized)} 个", flush=True)
        except Exception as e:
            print(f"[摘要] 定时任务异常: {e}", flush=True)
        time.sleep(600)  # 10 分钟


def _start_callback_server(port):
    server = HTTPServer(('0.0.0.0', port), _CardCallbackHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()


def _start_ngrok(port):
    """启动 ngrok 隧道，返回公网 URL"""
    import subprocess
    import urllib.request

    # 先检查已有的 ngrok 隧道
    try:
        with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=2) as r:
            tunnels = json.loads(r.read())
            for t in tunnels.get("tunnels", []):
                if t.get("proto") == "https":
                    return t["public_url"]
    except Exception:
        pass

    # 启动新 ngrok（有固定域名就用，保证重启后 URL 不变）
    try:
        ngrok_domain = os.environ.get("NGROK_DOMAIN", "")
        ngrok_cmd = ["ngrok", "http", "--url", ngrok_domain, str(port)] if ngrok_domain else ["ngrok", "http", str(port)]
        subprocess.Popen(
            ngrok_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(3)
        with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=5) as r:
            tunnels = json.loads(r.read())
            for t in tunnels.get("tunnels", []):
                if t.get("proto") == "https":
                    return t["public_url"]
    except Exception as e:
        print(f"   [warn] ngrok 启动失败: {e}", flush=True)
    return None


# ── 启动 ──────────────────────────────────────────────────────

def main():
    print("🚀 飞书 Claude Bot 启动中...")
    print(f"   App ID      : {config.FEISHU_APP_ID}")
    print(f"   默认模型    : {config.DEFAULT_MODEL}")
    print(f"   默认工作目录: {config.DEFAULT_CWD}")
    print(f"   权限模式    : {config.PERMISSION_MODE}")

    # 卡片回调 HTTP 服务 + ngrok 隧道
    cb_port = config.CALLBACK_PORT
    _start_callback_server(cb_port)
    ngrok_url = _start_ngrok(cb_port)
    if ngrok_url:
        print(f"   卡片回调    : {ngrok_url}/callback")
    else:
        print(f"   卡片回调    : http://localhost:{cb_port}/callback (需启动 ngrok)")

    handler = lark.EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(on_message_receive) \
        .register_p2_card_action_trigger(on_card_action) \
        .build()

    ws_client = lark.ws.Client(
        config.FEISHU_APP_ID,
        config.FEISHU_APP_SECRET,
        event_handler=handler,
        log_level=lark.LogLevel.INFO,
    )

    # 启动后台线程
    threading.Thread(target=_watchdog, daemon=True).start()
    threading.Thread(target=_bg_summary_thread, daemon=True).start()

    print("✅ 连接飞书 WebSocket 长连接（自动重连）...")
    ws_client.start()  # 阻塞，内部运行 asyncio loop


if __name__ == "__main__":
    main()
