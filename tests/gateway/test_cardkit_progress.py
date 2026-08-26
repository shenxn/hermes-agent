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
