import asyncio
import os
import sys

import pytest

os.environ.setdefault("FEISHU_APP_ID", "test_app_id")
os.environ.setdefault("FEISHU_APP_SECRET", "test_app_secret")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from claude_runner import run_claude


class FakeStdin:
    def __init__(self):
        self.buffer = b""
        self.closed = False

    def write(self, data: bytes):
        self.buffer += data

    async def drain(self):
        return None

    def close(self):
        self.closed = True


class FakeStdout:
    def __init__(self, lines: list[bytes]):
        self._lines = list(lines)
        self._index = 0

    async def readline(self):
        if self._index >= len(self._lines):
            return b""
        line = self._lines[self._index]
        self._index += 1
        return line


class FakeStderr:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self):
        return self._data


class FakeProc:
    def __init__(self, stdout_lines: list[bytes], stderr: bytes = b"", returncode: int = 0):
        self.stdin = FakeStdin()
        self.stdout = FakeStdout(stdout_lines)
        self.stderr = FakeStderr(stderr)
        self.returncode = returncode

    async def wait(self):
        return self.returncode

    def kill(self):
        pass


def test_run_claude_prefers_final_result_over_partial_deltas(monkeypatch):
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_123"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello"}}}\n',
        b'{"type":"result","session_id":"sid_123","result":"Hello world"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    text, session_id, used_fallback = asyncio.run(run_claude("hi"))

    assert text == "Hello world"
    assert session_id == "sid_123"
    assert used_fallback is False
    assert proc.stdin.buffer.endswith(b"hi\n")
    assert proc.stdin.closed is True


def test_run_claude_returns_partial_output_on_nonzero_exit_with_stderr(monkeypatch):
    """When there's partial output + stderr + nonzero exit, return partial text (don't raise)"""
    proc = FakeProc([
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"partial"}}}\n',
    ], stderr=b"boom", returncode=1)

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    text, session_id, used_fallback = asyncio.run(run_claude("hi"))
    assert text == "partial"
    assert used_fallback is False


def test_run_claude_raises_on_nonzero_exit_without_output(monkeypatch):
    """When there's NO output and nonzero exit, raise RuntimeError"""
    proc = FakeProc([], stderr=b"fatal error", returncode=1)

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match=r"fatal error"):
        asyncio.run(run_claude("hi"))


def test_run_claude_retries_without_resume_on_empty_stderr_failure(monkeypatch):
    first = FakeProc([], stderr=b"", returncode=1)
    second = FakeProc([
        b'{"type":"system","session_id":"sid_new"}\n',
        b'{"type":"result","session_id":"sid_new","result":"fresh answer"}\n',
    ])
    procs = iter([first, second])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return next(procs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    text, session_id, used_fallback = asyncio.run(run_claude("hi", session_id="sid_old"))

    assert text == "fresh answer"
    assert session_id == "sid_new"
    assert used_fallback is True
    assert first.stdin.closed is True
    assert second.stdin.closed is True


def test_run_claude_streams_text_chunks_via_callback(monkeypatch):
    """Test that on_text_chunk callback fires for text deltas"""
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello "}}}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"world"}}}\n',
        b'{"type":"result","session_id":"sid_1","result":"Hello world"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    chunks = []

    async def collect_chunk(chunk):
        chunks.append(chunk)

    text, session_id, _ = asyncio.run(
        run_claude("hi", on_text_chunk=collect_chunk)
    )

    assert chunks == ["Hello ", "world"]
    assert text == "Hello world"


def test_run_claude_fires_tool_use_callback(monkeypatch):
    """Test that on_tool_use callback fires for tool calls"""
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_start","content_block":{"type":"tool_use","name":"Bash"}}}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{\\"command\\": \\"ls\\"}"}}}\n',
        b'{"type":"stream_event","event":{"type":"content_block_stop"}}\n',
        b'{"type":"result","session_id":"sid_1","result":"done"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    tool_calls = []

    async def collect_tool(name, inp):
        tool_calls.append((name, inp))

    text, _, _ = asyncio.run(
        run_claude("hi", on_tool_use=collect_tool)
    )

    # Should fire twice: once on block_start (empty input), once on block_stop (full input)
    assert len(tool_calls) == 2
    assert tool_calls[0] == ("Bash", {})
    assert tool_calls[1] == ("Bash", {"command": "ls"})


# ── SDK envelope handling (post-4.7 stream-json format) ─────────────────────


def test_run_claude_assistant_text_envelope_does_not_duplicate_text(monkeypatch):
    """type:'assistant' with text content arrives alongside content_block_delta.
    Firing on_text_chunk for BOTH would double the card content. The runner must
    forward text ONLY via content_block_delta, and IGNORE assistant text envelopes.
    """
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello"}}}\n',
        # Top-level assistant envelope with the SAME text — must NOT fire another chunk
        b'{"type":"assistant","parent_tool_use_id":null,"message":{"type":"message","content":[{"type":"text","text":"Hello"}]}}\n',
        b'{"type":"result","session_id":"sid_1","result":"Hello"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    chunks = []

    async def collect_chunk(chunk):
        chunks.append(chunk)

    text, _, _ = asyncio.run(run_claude("hi", on_text_chunk=collect_chunk))

    # Only ONE chunk (from content_block_delta), not two
    assert chunks == ["Hello"]
    assert text == "Hello"


def test_run_claude_subagent_tool_use_fires_subagent_heartbeat(monkeypatch):
    """When the main agent spawns a subagent via the Agent tool, the subagent's
    internal tool_use events arrive as type:'assistant' with parent_tool_use_id set.
    The runner must forward these as [subagent]-prefixed on_tool_use calls so the
    Feishu card has a heartbeat while the subagent runs.
    """
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        # Main agent spawns the Agent tool (handled via stream_event elsewhere)
        # Subagent's INTERIOR tool_use — this is what we need to surface
        b'{"type":"assistant","parent_tool_use_id":"toolu_ABC","message":{"type":"message","content":[{"type":"tool_use","name":"Glob","input":{"pattern":"**/*.md"}}]}}\n',
        b'{"type":"result","session_id":"sid_1","result":"done"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    tool_calls = []

    async def collect_tool(name, inp):
        tool_calls.append((name, inp))

    text, _, _ = asyncio.run(run_claude("hi", on_tool_use=collect_tool))

    # Subagent tool_use must be surfaced with [subagent] prefix
    assert tool_calls == [("[subagent] Glob", {"pattern": "**/*.md"})]


def test_run_claude_user_tool_result_from_subagent_fires_progress_heartbeat(monkeypatch):
    """tool_result envelopes inside a subagent arrive as type:'user' with
    parent_tool_use_id set. The runner must fire a lightweight progress heartbeat
    so the card updates when the subagent works through multiple tools.
    Main-agent (parent_tool_use_id=null) tool_results must NOT fire extra events.
    """
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        # Subagent tool_result — fire heartbeat
        b'{"type":"user","parent_tool_use_id":"toolu_ABC","message":{"role":"user","content":[{"type":"tool_result","content":"stuff"}]}}\n',
        # Main agent tool_result — do NOT fire
        b'{"type":"user","parent_tool_use_id":null,"message":{"role":"user","content":[{"type":"tool_result","content":"stuff"}]}}\n',
        b'{"type":"result","session_id":"sid_1","result":"done"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    tool_calls = []

    async def collect_tool(name, inp):
        tool_calls.append((name, inp))

    asyncio.run(run_claude("hi", on_tool_use=collect_tool))

    # Exactly one heartbeat for the subagent, nothing for the main-agent result
    assert tool_calls == [("[subagent] ⏳", {})]


def test_run_claude_rate_limit_event_ignored_silently(monkeypatch):
    """rate_limit_event envelopes are informational; must not crash or fire callbacks."""
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        b'{"type":"rate_limit_event","rate_limit_info":{"status":"allowed"},"session_id":"sid_1"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"ok"}}}\n',
        b'{"type":"result","session_id":"sid_1","result":"ok"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    chunks = []
    tool_calls = []

    async def collect_chunk(chunk):
        chunks.append(chunk)

    async def collect_tool(name, inp):
        tool_calls.append((name, inp))

    text, _, _ = asyncio.run(
        run_claude("hi", on_text_chunk=collect_chunk, on_tool_use=collect_tool)
    )

    assert text == "ok"
    assert chunks == ["ok"]
    assert tool_calls == []


def test_run_claude_unknown_event_type_does_not_break_stream(monkeypatch, capsys):
    """Unknown envelope types must be logged (for observability) but not crash the stream."""
    proc = FakeProc([
        b'{"type":"system","session_id":"sid_1"}\n',
        b'{"type":"some_future_envelope","anything":"goes"}\n',
        b'{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"after"}}}\n',
        b'{"type":"result","session_id":"sid_1","result":"after"}\n',
    ])

    async def fake_create_subprocess_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    text, _, _ = asyncio.run(run_claude("hi"))

    assert text == "after"
    out = capsys.readouterr().out
    assert "unknown event_type='some_future_envelope'" in out
