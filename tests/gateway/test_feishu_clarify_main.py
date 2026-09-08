"""Clarify answers must resume their originating Feishu lane."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageType, SendResult
from gateway.session import SessionSource, build_session_key
from plugins.platforms.feishu.adapter import FeishuAdapter
from tools import clarify_gateway as cm


@pytest.fixture
def adapter():
    a = object.__new__(FeishuAdapter)
    BasePlatformAdapter.__init__(a, PlatformConfig(enabled=True, extra={}), Platform.FEISHU)
    a.send = AsyncMock(return_value=SendResult(success=True, message_id="om_prompt"))
    a._extract_message_content = AsyncMock(return_value=("品牌B", MessageType.TEXT, [], [], []))
    a._fetch_message_text = AsyncMock(return_value="Which brand?")
    a.get_chat_info = AsyncMock(return_value={"type": "group", "name": "group"})
    a._resolve_sender_profile = AsyncMock(return_value={"user_id": "ou_owner", "user_name": "owner", "user_id_alt": None})
    a._dispatch_inbound_event = AsyncMock()
    yield a
    with cm._lock:
        cm._entries.clear()
        cm._session_index.clear()


def source(thread=None):
    return SessionSource(platform=Platform.FEISHU, chat_id="oc_group", chat_type="group", user_id="ou_owner", thread_id=thread)


async def prompt(a, thread=None):
    key = build_session_key(
        source(thread),
        group_sessions_per_user=a.config.extra.get("group_sessions_per_user", True),
        thread_sessions_per_user=a.config.extra.get("thread_sessions_per_user", False),
        profile=a._session_key_profile(source(thread)),
    )
    entry = cm.register("cl_test", key, "Which brand?", ["品牌A", "品牌B"])
    await a.send_clarify("oc_group", entry.question, entry.choices, entry.clarify_id, key,
                         metadata={"thread_id": thread} if thread else None)
    return entry


async def inbound(a, *, root=None, thread=None):
    message = SimpleNamespace(chat_id="oc_group", root_id=root, parent_id=root, thread_id=thread)
    await a._process_inbound_message(data=message, message=message,
                                    sender_id=SimpleNamespace(open_id="ou_owner"),
                                    chat_type="group", message_id="om_answer")
    return a._dispatch_inbound_event.await_args.args[0]


@pytest.mark.asyncio
async def test_unquoted_main_answer_already_matches_original_session(adapter):
    entry = await prompt(adapter)
    event = await inbound(adapter)
    assert build_session_key(event.source) == entry.session_key
    assert cm.resolve_text_response_for_session(entry.session_key, event.text)
    assert entry.response == "品牌B"


@pytest.mark.asyncio
async def test_quoted_main_clarify_answer_keeps_original_session(adapter):
    entry = await prompt(adapter)
    event = await inbound(adapter, root="om_prompt")
    assert event.source.thread_id is None
    assert build_session_key(event.source) == entry.session_key
    assert cm.resolve_text_response_for_session(build_session_key(event.source), event.text)
    assert entry.response == "品牌B"


@pytest.mark.asyncio
async def test_shared_group_other_user_can_resolve_clarify(adapter):
    from tests.gateway.test_clarify_thread_followup_not_swallowed import (
        _make_runner, _dispatch, _FellThroughIntercept,
    )
    adapter.config.extra["group_sessions_per_user"] = False
    event = await inbound(adapter)
    key = build_session_key(event.source, group_sessions_per_user=False)
    entry = cm.register("cl_shared", key, "Which brand?", None)
    runner = _make_runner(adapter)
    runner._session_key_for_source = lambda s: build_session_key(s, group_sessions_per_user=False)
    await _dispatch(runner, event)
    assert entry.event.is_set()
    assert entry.response == "品牌B"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["other_user", "other_chat", "explicit_topic", "other_root", "expired", "resolved", "cancelled"])
async def test_quote_alias_does_not_cross_boundaries(adapter, case):
    adapter.config.extra["group_sessions_per_user"] = False
    entry = await prompt(adapter)
    message = SimpleNamespace(chat_id="oc_group", chat_type="group", root_id="om_prompt", parent_id="om_prompt", thread_id=None)
    sender = SimpleNamespace(open_id="ou_owner")
    if case == "other_user":
        sender.open_id = "ou_other"
    elif case == "other_chat":
        message.chat_id = "oc_other"
    elif case == "explicit_topic":
        message.thread_id = "omt_explicit"
    elif case == "other_root":
        message.root_id = message.parent_id = "om_unrelated"
    elif case == "expired":
        assert await asyncio.to_thread(cm.wait_for_response, entry.clarify_id, 0.01) is None
    elif case == "resolved":
        assert cm.resolve_gateway_clarify(entry.clarify_id, "品牌A")
    elif case == "cancelled":
        cm.clear_session(entry.session_key)
    if case == "other_user":
        assert adapter._clarify_reply_route(message, sender) is not None
    else:
        assert adapter._clarify_reply_route(message, sender) is None


@pytest.mark.asyncio
async def test_existing_explicit_topic_clarify_stays_in_topic(adapter):
    entry = await prompt(adapter, thread="omt_original")
    event = await inbound(adapter, root="om_prompt", thread="omt_original")
    assert event.source.thread_id == "omt_original"
    assert build_session_key(event.source) == entry.session_key
    assert adapter.send.await_args.kwargs["metadata"] == {"thread_id": "omt_original"}


@pytest.mark.asyncio
async def test_profile_namespaced_quote_alias(adapter):
    adapter.set_owner_profile("test_profile")
    entry = await prompt(adapter)
    event = await inbound(adapter, root="om_prompt")
    assert event.source.thread_id is None
    assert build_session_key(event.source, profile="test_profile") == entry.session_key


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("shared", [False, True])
async def test_busy_agent_receives_answer_without_new_turn(adapter, quoted, shared):
    from tests.gateway.test_clarify_thread_followup_not_swallowed import _make_runner, _dispatch
    adapter.config.extra["group_sessions_per_user"] = not shared
    entry = await prompt(adapter)
    original_source = source()
    event = await inbound(adapter, root="om_prompt" if quoted else None)
    runner = _make_runner(adapter)
    runner._session_key_for_source = lambda source: build_session_key(source, group_sessions_per_user=not shared)
    adapter._message_handler = lambda ev: _dispatch(runner, ev)
    adapter._busy_session_handler = AsyncMock(return_value=True)
    adapter._active_sessions[entry.session_key] = asyncio.Event()
    waiter = asyncio.create_task(asyncio.to_thread(cm.wait_for_response, entry.clarify_id, 1.0))
    await asyncio.sleep(0)  # Real event-loop scheduling while the agent thread waits.
    await adapter.handle_message(event)
    assert await waiter == "品牌B"
    adapter._busy_session_handler.assert_not_awaited()
    assert adapter._pending_messages == {}
    assert original_source.thread_id is None
    # Continuation delivery uses the ORIGINAL source, never the clarify reply root.
    from gateway.platforms.base import _thread_metadata_for_source
    assert not (_thread_metadata_for_source(original_source) or {}).get("thread_id")


@pytest.mark.asyncio
async def test_prompt_expiring_during_inbound_lookup_does_not_alias(adapter):
    entry = await prompt(adapter)

    async def fetched(_message_id):
        await asyncio.sleep(0)
        cm.clear_session(entry.session_key)
        return "Which brand?"

    adapter._fetch_message_text.side_effect = fetched
    event = await inbound(adapter, root="om_prompt")
    assert event.source.thread_id == "om_prompt"


@pytest.mark.asyncio
async def test_failed_prompt_send_creates_no_alias(adapter):
    adapter.send.return_value = SendResult(success=False, error="not delivered")
    await prompt(adapter)
    event = await inbound(adapter, root="om_prompt")
    assert event.source.thread_id == "om_prompt"


@pytest.mark.asyncio
async def test_document_without_text_is_not_a_clarify_answer(adapter):
    from tests.gateway.test_clarify_thread_followup_not_swallowed import _make_runner, _dispatch, _FellThroughIntercept
    entry = await prompt(adapter)
    adapter._extract_message_content.return_value = ("", MessageType.DOCUMENT, ["/tmp/spec.pdf"], ["application/pdf"], [])
    event = await inbound(adapter, root="om_prompt")
    runner = _make_runner(adapter)
    runner._session_key_for_source = build_session_key
    with pytest.raises(_FellThroughIntercept):
        await _dispatch(runner, event)
    assert not entry.event.is_set()
    assert event.media_urls == ["/tmp/spec.pdf"]
