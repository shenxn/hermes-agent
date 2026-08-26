"""Regression tests for the Feishu CardKit streaming integration."""

import asyncio
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import StreamingConfig
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from plugins.platforms.feishu.adapter import (
    FeishuAdapter,
    _FeishuStreamingCard,
    _apply_yaml_config,
)


def _cardkit_adapter():
    adapter = MagicMock()
    adapter.streaming_cards_enabled = True
    adapter.REQUIRES_EDIT_FINALIZE = False
    adapter.MAX_MESSAGE_LENGTH = 30_000
    adapter.send_streaming_card = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="om_card")
    )
    adapter.send = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="om_plain")
    )
    adapter.edit_message = AsyncMock(
        return_value=SimpleNamespace(success=True, message_id="om_card")
    )
    adapter.stop_streaming_card = AsyncMock(return_value=True)
    return adapter


def _cardkit_config(**overrides):
    values = {
        "transport": "edit",
        "edit_interval": 0.0,
        "buffer_threshold": 1,
        "merge_segments": True,
        "streaming_mode": "cardkit",
    }
    values.update(overrides)
    return StreamConsumerConfig(**values)


def test_streaming_config_round_trip_preserves_cardkit_fields():
    cfg = StreamingConfig.from_dict(
        {"merge_segments": True, "streaming_mode": "cardkit"}
    )
    assert cfg.merge_segments is True
    assert cfg.streaming_mode == "cardkit"
    assert cfg.to_dict()["merge_segments"] is True
    assert cfg.to_dict()["streaming_mode"] == "cardkit"


def test_feishu_yaml_bridge_preserves_group_controls(monkeypatch):
    monkeypatch.delenv("FEISHU_ALLOW_BOTS", raising=False)
    extras = _apply_yaml_config(
        {},
        {
            "group_rules": {"oc_free": {"require_mention": False}},
            "admins": ["ou_admin"],
            "default_group_policy": "open",
        },
    )
    assert extras == {
        "group_rules": {"oc_free": {"require_mention": False}},
        "admins": ["ou_admin"],
        "default_group_policy": "open",
    }


def test_streaming_card_json_and_footer_contract():
    card = FeishuAdapter._build_streaming_card_json("hello")
    assert card["config"]["streaming_mode"] is True
    assert card["body"]["elements"][0]["content"] == "hello"
    assert card["body"]["elements"][0]["element_id"] == "streaming_md_1"
    assert FeishuAdapter._streaming_status_footer("completed", 1.25).startswith(
        "✅ 已完成"
    )
    assert "1.2s" in FeishuAdapter._streaming_status_footer("completed", 1.25)
    assert FeishuAdapter._streaming_status_footer("continued", 65).startswith(
        "⏸ 待续 →"
    )
    assert "1m 5s" in FeishuAdapter._streaming_status_footer("continued", 65)


@pytest.mark.asyncio
async def test_cardkit_first_send_empty_inject_and_stop_once():
    adapter = _cardkit_adapter()
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    consumer.inject("🔧 web_search")
    consumer.on_delta("最终回答")
    consumer.finish()
    await consumer.run()

    adapter.send_streaming_card.assert_called_once()
    adapter.send.assert_not_called()
    initial = adapter.send_streaming_card.call_args.kwargs["content"]
    assert "> 🔧 web_search" in initial
    final_visible = (
        adapter.edit_message.await_args_list[-1].kwargs["content"]
        if adapter.edit_message.await_args_list
        else initial
    )
    assert "最终回答" in final_visible
    adapter.stop_streaming_card.assert_awaited_once_with(
        "om_card", status="completed"
    )
    assert consumer.final_response_sent is True


@pytest.mark.asyncio
async def test_cardkit_merges_across_tool_segment_boundary():
    adapter = _cardkit_adapter()
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    consumer.on_delta("先检查")
    consumer.on_segment_break()
    consumer.inject("🔧 terminal")
    consumer.on_delta("检查完成")
    consumer.finish()
    await consumer.run()

    adapter.send_streaming_card.assert_called_once()
    adapter.send.assert_not_called()
    assert adapter.edit_message.await_count >= 1
    final_edit = adapter.edit_message.await_args_list[-1].kwargs["content"]
    assert "先检查" in final_edit
    assert "> 🔧 terminal" in final_edit
    assert "检查完成" in final_edit
    adapter.stop_streaming_card.assert_awaited_once_with(
        "om_card", status="completed"
    )


@pytest.mark.asyncio
async def test_cardkit_config_falls_back_when_capability_is_unavailable():
    adapter = _cardkit_adapter()
    adapter.streaming_cards_enabled = False
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    consumer.inject("should not be injected")
    consumer.on_delta("plain response")
    consumer.finish()
    await consumer.run()

    adapter.send_streaming_card.assert_not_called()
    adapter.send.assert_called_once()
    assert "should not be injected" not in adapter.send.call_args.kwargs["content"]
    adapter.stop_streaming_card.assert_not_called()


@pytest.mark.asyncio
async def test_progress_followed_by_silence_marker_is_suppressed():
    adapter = _cardkit_adapter()
    adapter.delete_message = AsyncMock(return_value=True)
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    task = asyncio.create_task(consumer.run())
    consumer.inject("🔧 web_search")
    await asyncio.sleep(0.1)
    consumer.on_delta("NO_REPLY")
    # Let the consumer process the marker before DONE; it must not flash.
    await asyncio.sleep(0.1)
    consumer.finish()
    await task

    adapter.stop_streaming_card.assert_awaited_once_with(
        "om_card", status="terminated"
    )
    adapter.delete_message.assert_awaited_once_with("oc_chat", "om_card")
    visible = [
        call.kwargs.get("content", "")
        for call in adapter.send_streaming_card.await_args_list
        + adapter.edit_message.await_args_list
    ]
    assert all("NO_REPLY" not in text for text in visible)
    assert consumer.final_response_sent is False


@pytest.mark.asyncio
async def test_cardkit_commentary_stays_in_one_card():
    adapter = _cardkit_adapter()
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    consumer.on_delta("开始")
    consumer.on_commentary("正在检查")
    consumer.on_delta("完成")
    consumer.finish()
    await consumer.run()

    adapter.send_streaming_card.assert_awaited_once()
    adapter.send.assert_not_called()
    final_text = (
        adapter.edit_message.await_args_list[-1].kwargs["content"]
        if adapter.edit_message.await_args_list
        else adapter.send_streaming_card.await_args.kwargs["content"]
    )
    assert "开始" in final_text
    assert "正在检查" in final_text
    assert "完成" in final_text
    adapter.stop_streaming_card.assert_awaited_once_with(
        "om_card", status="completed"
    )


@pytest.mark.asyncio
async def test_cardkit_overflow_stops_old_card_as_continued():
    adapter = _cardkit_adapter()
    adapter.MAX_MESSAGE_LENGTH = 700
    adapter.send_streaming_card = AsyncMock(
        side_effect=[
            SimpleNamespace(success=True, message_id="om_card_1"),
            SimpleNamespace(success=True, message_id="om_card_2"),
            SimpleNamespace(success=True, message_id="om_card_3"),
        ]
    )
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())
    assert await consumer._send_or_edit("开头内容") is True

    consumer.on_delta("长" * 1200)
    consumer.finish()
    await consumer.run()

    statuses = [call.kwargs["status"] for call in adapter.stop_streaming_card.await_args_list]
    assert statuses[:-1]
    assert all(status == "continued" for status in statuses[:-1])
    assert statuses[-1] == "completed"


@pytest.mark.asyncio
async def test_failed_overflow_stop_is_retried_for_original_card():
    adapter = _cardkit_adapter()
    adapter.MAX_MESSAGE_LENGTH = 700
    adapter.send_streaming_card = AsyncMock(
        side_effect=[
            SimpleNamespace(success=True, message_id="om_card_1"),
            SimpleNamespace(success=True, message_id="om_card_2"),
        ]
    )
    attempts = {"om_card_1": 0}

    async def stop_card(message_id, *, status):
        if message_id == "om_card_1":
            attempts[message_id] += 1
            return attempts[message_id] > 1
        return True

    adapter.stop_streaming_card = AsyncMock(side_effect=stop_card)
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())
    assert await consumer._send_or_edit("开头内容") is True

    consumer.on_delta("长" * 900)
    consumer.finish()
    await consumer.run()

    calls = [
        (call.args[0], call.kwargs["status"])
        for call in adapter.stop_streaming_card.await_args_list
    ]
    assert calls.count(("om_card_1", "continued")) == 2
    assert ("om_card_2", "completed") in calls
    assert consumer._cardkit_pending_stops == {}


def test_overflow_transform_appends_only_suffix_to_last_card():
    consumer = GatewayStreamConsumer(
        _cardkit_adapter(), "oc_chat", _cardkit_config()
    )
    consumer._cardkit_overflowed = True
    consumer._stream_ledger = "sealed head + active tail"
    consumer._last_sent_text = "active tail"

    payload = consumer.cardkit_transformed_update_payload(
        "sealed head + active tail\n\n[plugin footer]"
    )

    assert payload == "active tail\n\n[plugin footer]"
    assert "sealed head" not in payload


def test_overflow_transform_rewrite_requires_fresh_delivery():
    consumer = GatewayStreamConsumer(
        _cardkit_adapter(), "oc_chat", _cardkit_config()
    )
    consumer._cardkit_overflowed = True
    consumer._stream_ledger = "original"
    consumer._last_sent_text = "original tail"

    assert consumer.cardkit_transformed_update_payload("rewritten") is None


@pytest.mark.asyncio
async def test_failed_completed_stop_retries_as_completed_in_finally():
    adapter = _cardkit_adapter()
    adapter.stop_streaming_card = AsyncMock(side_effect=[False, True])
    consumer = GatewayStreamConsumer(adapter, "oc_chat", _cardkit_config())

    consumer.on_delta("最终回答")
    consumer.finish()
    await consumer.run()

    assert adapter.stop_streaming_card.await_count == 2
    assert [call.kwargs["status"] for call in adapter.stop_streaming_card.await_args_list] == [
        "completed",
        "completed",
    ]
    assert consumer._streaming_card_stopped is True


@pytest.mark.asyncio
async def test_stale_cardkit_run_is_terminated_exactly_once():
    adapter = _cardkit_adapter()
    consumer = GatewayStreamConsumer(
        adapter,
        "oc_chat",
        _cardkit_config(),
        run_still_current=lambda: False,
    )
    consumer._message_id = "om_card"

    await consumer.run()

    adapter.stop_streaming_card.assert_awaited_once_with(
        "om_card", status="terminated"
    )


@pytest.mark.asyncio
async def test_initial_cardkit_failure_falls_back_with_identical_send_arguments():
    adapter = _cardkit_adapter()
    adapter.send_streaming_card.return_value = SimpleNamespace(
        success=False, message_id=None, error="create failed"
    )
    consumer = GatewayStreamConsumer(
        adapter,
        "oc_chat",
        _cardkit_config(),
        initial_reply_to_id="om_parent",
    )

    assert await consumer._send_or_edit("fallback body") is True

    card_kwargs = adapter.send_streaming_card.call_args.kwargs
    plain_kwargs = adapter.send.call_args.kwargs
    assert plain_kwargs == card_kwargs
    assert consumer._uses_streaming_card is False
    consumer.inject("must not enter the CardKit queue")
    assert consumer._queue.empty()


class _Builder:
    """Small stand-in for the fluent lark-oapi request builders."""

    def __getattr__(self, _name):
        return lambda *args, **kwargs: self


class _BuilderType:
    @staticmethod
    def builder():
        return _Builder()


@pytest.mark.asyncio
async def test_stop_failure_retains_card_for_retry_and_final_edits_use_cardkit():
    adapter = FeishuAdapter.__new__(FeishuAdapter)
    adapter._client = SimpleNamespace(
        cardkit=SimpleNamespace(
            v1=SimpleNamespace(
                card=SimpleNamespace(settings=MagicMock(), update=MagicMock()),
                card_element=SimpleNamespace(content=MagicMock()),
            )
        ),
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(update=MagicMock()))),
    )
    card = _FeishuStreamingCard(
        card_id="card_1",
        element_id="streaming_md_1",
        message_id="om_card",
        last_sent_content="answer",
    )
    adapter._streaming_cards = {"om_card": card}
    adapter._finalized_streaming_cards = OrderedDict()
    adapter._streaming_card_locks = {}
    adapter._run_blocking = AsyncMock(side_effect=[
        SimpleNamespace(code=0),       # settings succeeds
        SimpleNamespace(code=500, msg="retry"),  # full update fails
        SimpleNamespace(code=0),       # retry only repeats full update
        SimpleNamespace(code=0),       # transformed edit
    ])

    with patch.multiple(
        "plugins.platforms.feishu.adapter",
        SettingsCardRequestBody=_BuilderType,
        SettingsCardRequest=_BuilderType,
        UpdateCardRequestBody=_BuilderType,
        UpdateCardRequest=_BuilderType,
        Card=_BuilderType,
    ):
        assert await adapter.stop_streaming_card("om_card") is False
        assert adapter._streaming_cards["om_card"] is card
        assert card.streaming_disabled is True
        assert adapter._run_blocking.await_count == 2

        assert await adapter.stop_streaming_card("om_card") is True
        assert adapter._run_blocking.await_count == 3
        assert "om_card" not in adapter._streaming_cards
        assert adapter._finalized_streaming_cards["om_card"] is card

        result = await adapter.edit_message("oc_chat", "om_card", "transformed")

    assert result.success is True
    assert card.last_sent_content == "transformed"
    assert adapter._run_blocking.await_count == 4
    adapter._client.im.v1.message.update.assert_not_called()


@pytest.mark.asyncio
async def test_feishu_delete_message_cleans_cardkit_registries():
    adapter = FeishuAdapter.__new__(FeishuAdapter)
    delete_api = MagicMock()
    adapter._client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(delete=delete_api)))
    )
    card = _FeishuStreamingCard(
        card_id="card_1",
        element_id="streaming_md_1",
        message_id="om_card",
    )
    adapter._streaming_cards = {"om_card": card}
    adapter._finalized_streaming_cards = OrderedDict({"om_card": card})
    adapter._run_blocking = AsyncMock(
        return_value=SimpleNamespace(success=lambda: True)
    )

    assert await adapter.delete_message("oc_chat", "om_card") is True
    assert "om_card" not in adapter._streaming_cards
    assert "om_card" not in adapter._finalized_streaming_cards
    adapter._run_blocking.assert_awaited_once()


def test_title_generation_enabled_false_skips_worker():
    from agent.title_generator import maybe_auto_title

    db = MagicMock()
    history = [{"role": "user", "content": "hello"}]
    with (
        patch(
            "hermes_cli.config.load_config_readonly",
            return_value={
                "auxiliary": {"title_generation": {"enabled": False}}
            },
        ),
        patch("agent.title_generator.auto_title_session") as worker,
    ):
        maybe_auto_title(db, "session", "hello", "world", history)
    worker.assert_not_called()
