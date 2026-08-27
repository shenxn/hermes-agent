import pytest

from gateway.cardkit_progress import CardKitProgressAggregator


def test_turn_runner_routes_structured_progress_to_active_cardkit():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from gateway.run import TurnRunner

    consumer = SimpleNamespace(
        _uses_streaming_card=True,
        cfg=SimpleNamespace(merge_segments=True),
        inject=MagicMock(),
    )
    ctx = SimpleNamespace(
        _run_still_current=lambda: True,
        stream_consumer_holder=[consumer],
        _live_status_adapter=None,
        _live_status_mode="off",
        log_queue=None,
        progress_queue=None,
    )
    turn = TurnRunner(SimpleNamespace(), ctx)

    turn.progress_callback(
        "subagent.start",
        subagent_id="child-1",
        task_count=1,
    )
    turn.progress_callback("tool.started", "web_search", "Hermes v0.20")
    turn._flush_cardkit_progress()

    injected = [call.args[0] for call in consumer.inject.call_args_list]
    assert injected == ["🔀 子任务启动", "🔧 web_search: Hermes v0.20"]


def test_subagent_aggregation_resets_between_delegation_batches():
    aggregator = CardKitProgressAggregator()

    first = []
    for child in ("a", "b"):
        _, lines = aggregator.push(
            "subagent.complete",
            subagent_id=child,
            task_count=2,
            status="completed",
        )
        first.extend(lines)
    assert first == ["✅ 子任务完成 ×2"]

    _, early = aggregator.push(
        "subagent.complete",
        subagent_id="c",
        task_count=2,
        status="completed",
    )
    assert early == []
    _, second = aggregator.push(
        "subagent.complete",
        subagent_id="d",
        task_count=2,
        status="completed",
    )
    assert second == ["✅ 子任务完成 ×2"]


@pytest.mark.asyncio
async def test_delayed_cardkit_claim_is_rechecked_at_progress_transport():
    import asyncio
    import queue
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from gateway.run import TurnRunner

    current = [True]
    consumer = SimpleNamespace(
        _uses_streaming_card=True,
        cfg=SimpleNamespace(merge_segments=True),
        inject=MagicMock(side_effect=lambda _text: current.__setitem__(0, False)),
    )

    class Adapter:
        name = "feishu"
        MAX_MESSAGE_LENGTH = 4000
        streaming_cards_enabled = True
        send = AsyncMock(side_effect=AssertionError("standalone progress must not send"))
        send_typing = AsyncMock()

        async def edit_message(self, *args, **kwargs):
            raise AssertionError("standalone progress must not edit")

    adapter = Adapter()
    runner = SimpleNamespace(
        _adapter_for_source=lambda _source: adapter,
        config=SimpleNamespace(
            streaming=SimpleNamespace(
                enabled=True,
                streaming_mode="cardkit",
                merge_segments=True,
            )
        ),
    )
    progress_queue = queue.Queue()
    progress_queue.put("🔧 web_search: delayed claim")
    holder = [None]
    ctx = SimpleNamespace(
        progress_queue=progress_queue,
        stream_consumer_holder=holder,
        source=SimpleNamespace(chat_id="oc_chat"),
        _native_slack_task_cards=False,
        progress_grouping="grouped",
        _progress_metadata=None,
        _progress_reply_to=None,
        _cleanup_progress=False,
        _cleanup_msg_ids=[],
        _run_still_current=lambda: current[0],
        agent_holder=[None],
        last_progress_msg=[None],
        repeat_count=[0],
    )
    turn = TurnRunner(runner, ctx)

    async def claim_on_later_tick():
        await asyncio.sleep(0.05)
        holder[0] = consumer

    await asyncio.gather(turn.send_progress_messages(), claim_on_later_tick())

    consumer.inject.assert_called_once_with("🔧 web_search: delayed claim")
    adapter.send.assert_not_awaited()


def _push_all(aggregator, events):
    lines = []
    for event_type, tool_name, preview, metadata in events:
        consumed, flushed = aggregator.push(
            event_type, tool_name, preview, **metadata
        )
        assert consumed is True
        lines.extend(flushed)
    lines.extend(aggregator.flush())
    return lines


def _meta(child, tool_count=None):
    data = {"subagent_id": child, "task_count": 3}
    if tool_count is not None:
        data["tool_count"] = tool_count
    return data


def test_interleaved_subagent_progress_folds_by_batch_not_adjacency():
    agg = CardKitProgressAggregator()
    events = [
        ("subagent.start", None, "goal-a", _meta("a")),
        ("subagent.thinking", None, "working-a", _meta("a")),
        ("subagent.start", None, "goal-b", _meta("b")),
        ("subagent.tool", "web_search", "query-a", _meta("a", 1)),
        ("subagent.thinking", None, "working-b", _meta("b")),
        ("subagent.start", None, "goal-c", _meta("c")),
        ("subagent.tool", "web_search", "query-b", _meta("b", 1)),
        ("subagent.thinking", None, "working-c", _meta("c")),
        ("subagent.tool", "web_search", "query-c", _meta("c", 1)),
        # Derived summaries are silent and do not split/double-count tools.
        ("subagent.progress", None, "web_search", _meta("a")),
        ("subagent.progress", None, "web_search", _meta("b")),
        ("subagent.progress", None, "web_search", _meta("c")),
        ("subagent.complete", None, "done-a", {**_meta("a"), "status": "completed"}),
        ("subagent.complete", None, "done-b", {**_meta("b"), "status": "completed"}),
        ("subagent.complete", None, "done-c", {**_meta("c"), "status": "completed"}),
    ]

    assert _push_all(agg, events) == [
        "🔀 子任务启动 ×3",
        "💭 子任务思考中 ×3",
        "🔧 web_search ×3: query-a",
        "✅ 子任务完成 ×3",
    ]


def test_duplicate_subagent_lifecycle_event_is_counted_once_per_child():
    agg = CardKitProgressAggregator()
    lines = _push_all(agg, [
        ("subagent.start", None, "goal", _meta("a")),
        ("subagent.start", None, "goal", _meta("a")),
        ("subagent.start", None, "goal", _meta("b")),
        ("subagent.start", None, "goal", _meta("c")),
    ])
    assert lines == ["🔀 子任务启动 ×3"]


def test_failed_child_is_visible_and_not_counted_as_success():
    agg = CardKitProgressAggregator()
    lines = _push_all(agg, [
        ("subagent.complete", None, "done", {**_meta("a"), "status": "completed"}),
        ("subagent.complete", None, "network broke", {**_meta("b"), "status": "failed"}),
        ("subagent.complete", None, "Timed out after 30s", {**_meta("c"), "status": "timeout"}),
    ])
    assert lines == [
        "❌ 子任务失败: network broke",
        "❌ 子任务超时: Timed out after 30s",
        "✅ 子任务完成",
    ]


def test_regular_tool_fold_semantics_are_preserved():
    agg = CardKitProgressAggregator()
    lines = _push_all(agg, [
        ("tool.started", "web_search", "q1", {}),
        ("tool.completed", "web_search", None, {}),
        ("reasoning.available", "_thinking", "x", {}),
        ("tool.started", "web_search", "q2", {}),
        ("tool.started", "read_file", "a.py", {}),
    ])
    assert lines == [
        "🔧 web_search ×2: q1",
        "🔧 read_file: a.py",
    ]


def test_child_text_and_derived_progress_are_silent_in_parent_card():
    agg = CardKitProgressAggregator()
    assert agg.push("subagent.text", preview="child answer") == (True, [])
    assert agg.push("subagent.progress", preview="web_search") == (True, [])
    assert agg.push("subagent_progress", preview="legacy summary") == (True, [])
    assert agg.flush() == []
