"""Structured progress folding for CardKit streaming cards.

CardKit ``merge_segments`` keeps progress and answer text in one card, but it
cannot deduplicate business events: every ``inject()`` call appends a new quote
block. Fold tool/subagent lifecycle events while they still carry structured
identity, before ``gateway.run`` renders them to strings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class _Bucket:
    count: int = 0
    preview: Optional[str] = None
    task_count: int = 1
    seen: Set[Tuple[Any, ...]] = field(default_factory=set)


class CardKitProgressAggregator:
    """Fold structured tool/subagent events into compact CardKit lines.

    Subagents run concurrently, so their events are not guaranteed to arrive
    in phase order (start(a), thinking(a), start(b), ...). Lifecycle and child
    tool buckets therefore persist independently instead of relying on adjacent
    events. A batch is emitted once it reaches the delegation's ``task_count``;
    remainders are emitted on completion/model text/status/end-of-run flushes.
    """

    _SUCCESS_STATUSES = {"completed", "success", "ok"}

    def __init__(self) -> None:
        self._tool: Optional[Tuple[str, int, Optional[str]]] = None
        self._lifecycle: Dict[str, _Bucket] = {
            "start": _Bucket(),
            "thinking": _Bucket(),
            "complete": _Bucket(),
        }
        self._subagent_tools: Dict[str, _Bucket] = {}
        self._completed_children: Set[Tuple[Any, ...]] = set()

    @staticmethod
    def _task_count(metadata: Dict[str, Any]) -> int:
        try:
            return max(1, int(metadata.get("task_count") or 1))
        except (TypeError, ValueError):
            return 1

    @staticmethod
    def _child_identity(metadata: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
        child = metadata.get("subagent_id")
        return (child,) if child else None

    @staticmethod
    def _tool_identity(metadata: Dict[str, Any]) -> Optional[Tuple[Any, ...]]:
        child = metadata.get("subagent_id")
        ordinal = metadata.get("tool_count")
        if child and ordinal is not None:
            return (child, ordinal)
        return None

    @staticmethod
    def _count_label(prefix: str, count: int) -> str:
        return f"{prefix} ×{count}" if count > 1 else prefix

    def _record(
        self,
        bucket: _Bucket,
        *,
        identity: Optional[Tuple[Any, ...]],
        preview: Optional[str],
        task_count: int,
    ) -> bool:
        bucket.task_count = max(bucket.task_count, task_count)
        if identity is not None:
            if identity in bucket.seen:
                return False
            bucket.seen.add(identity)
        bucket.count += 1
        if not bucket.preview and preview:
            bucket.preview = preview
        return True

    @staticmethod
    def _drain_bucket(bucket: _Bucket, prefix: str, *, with_preview: bool = False) -> List[str]:
        if bucket.count <= 0:
            return []
        label = CardKitProgressAggregator._count_label(prefix, bucket.count)
        if with_preview and bucket.preview:
            label += f": {bucket.preview}"
        bucket.count = 0
        bucket.preview = None
        return [label]

    def _drain_lifecycle(self, name: str) -> List[str]:
        prefixes = {
            "start": "🔀 子任务启动",
            "thinking": "💭 子任务思考中",
            "complete": "✅ 子任务完成",
        }
        return self._drain_bucket(self._lifecycle[name], prefixes[name])

    def _drain_subagent_tools(self) -> List[str]:
        lines: List[str] = []
        for name, bucket in self._subagent_tools.items():
            lines.extend(self._drain_bucket(bucket, f"🔧 {name}", with_preview=True))
        return lines

    def push(
        self,
        event_type: str,
        tool_name: Optional[str] = None,
        preview: Optional[str] = None,
        **metadata: Any,
    ) -> tuple[bool, List[str]]:
        """Consume one gateway progress event.

        Returns ``(consumed, flushed_lines)``. A consumed event must not fall
        through to the legacy progress queue, otherwise CardKit renders it a
        second time without structure.
        """
        if event_type == "tool.started" and tool_name:
            if tool_name == "_thinking":
                return True, []
            lines: List[str] = []
            if self._tool is None or self._tool[0] != tool_name:
                lines.extend(self._drain_regular_tool())
                self._tool = (tool_name, 1, preview)
            else:
                name, count, first_preview = self._tool
                self._tool = (name, count + 1, first_preview or preview)
            return True, lines

        if event_type in {"tool.completed", "reasoning.available"}:
            return True, []

        if event_type in {"subagent.progress", "subagent_progress", "subagent.text"}:
            # progress is derived from canonical subagent.tool events; child
            # text belongs in child-watch/TUI surfaces, not the parent card.
            return True, []

        task_count = self._task_count(metadata)
        child_identity = self._child_identity(metadata)

        if event_type == "subagent.start":
            bucket = self._lifecycle["start"]
            self._record(
                bucket, identity=child_identity, preview=None, task_count=task_count
            )
            if bucket.count >= bucket.task_count:
                return True, self._drain_lifecycle("start")
            return True, []

        if event_type == "subagent.thinking":
            bucket = self._lifecycle["thinking"]
            self._record(
                bucket, identity=child_identity, preview=None, task_count=task_count
            )
            if bucket.count >= bucket.task_count:
                return True, self._drain_lifecycle("thinking")
            return True, []

        if event_type == "subagent.tool":
            name = tool_name or "tool"
            bucket = self._subagent_tools.setdefault(name, _Bucket())
            added = self._record(
                bucket,
                identity=self._tool_identity(metadata),
                preview=preview,
                task_count=task_count,
            )
            if added and bucket.count >= bucket.task_count:
                return True, self._drain_bucket(
                    bucket, f"🔧 {name}", with_preview=True
                )
            return True, []

        if event_type == "subagent.complete":
            if child_identity is not None:
                if child_identity in self._completed_children:
                    return True, []
                self._completed_children.add(child_identity)

            status = str(metadata.get("status") or "completed").lower()
            lines: List[str] = []
            if status not in self._SUCCESS_STATUSES:
                # Never fold a failure into a green completion count.
                lines.extend(self._drain_subagent_tools())
                reason = preview or metadata.get("summary") or status
                noun = "超时" if status == "timeout" else "失败"
                lines.append(f"❌ 子任务{noun}: {reason}")
            else:
                bucket = self._lifecycle["complete"]
                self._record(
                    bucket, identity=child_identity, preview=None, task_count=task_count
                )

            if len(self._completed_children) >= task_count:
                # All branches are terminal: expose any tool/lifecycle remainder.
                lines = self._drain_lifecycle("start") + self._drain_lifecycle("thinking") + lines
                lines.extend(self._drain_subagent_tools())
                lines.extend(self._drain_lifecycle("complete"))
            return True, lines

        return False, []

    def _drain_regular_tool(self) -> List[str]:
        if self._tool is None:
            return []
        name, count, preview = self._tool
        label = self._count_label(f"🔧 {name}", count)
        if preview:
            label += f": {preview}"
        self._tool = None
        return [label]

    def flush(self) -> List[str]:
        lines = self._drain_regular_tool()
        lines.extend(self._drain_lifecycle("start"))
        lines.extend(self._drain_lifecycle("thinking"))
        lines.extend(self._drain_subagent_tools())
        lines.extend(self._drain_lifecycle("complete"))
        return lines
