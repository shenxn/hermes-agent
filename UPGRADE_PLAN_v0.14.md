# Hermes Agent v0.9.0 → v0.14.0 升级方案

**创建时间：** 2026-05-22
**当前版本：** v0.9.0 (release-patched 分支，基于 v2026.4.13)
**目标版本：** v0.14.0 (v2026.5.16)
**差距：** 5 个大版本 / 4436 upstream commits / 21 个自定义 patch

---

## 一、策略选择：Patch 重应用法（非 rebase）

**为什么不用 rebase：**
- `gateway/run.py`：双方各改 3000+ / 11000+ 行 → rebase 会产生 100+ 冲突
- `run_agent.py`：双方各改 4000+ / 9000+ 行 → 同上
- 我们的分支混合了 upstream 的 `feat/ink-refactor` 开发分支（数百 commits），rebase 会试图回放所有这些

**策略：** 从 v0.14.0 干净分支出发，逐个提取自定义 patch 的 diff，手动应用到新代码。

---

## 二、自定义 Patch 清单 & 分类

### A. CardKit 流式核心（14 个 commits → 必须保留，upstream 无等效实现）

| # | Commit | 描述 | 涉及文件 |
|---|--------|------|---------|
| 1 | `8dbf50b09` | Cherry-pick CardKit streaming cards PR | feishu.py, stream_consumer.py, config.py |
| 2 | `a14c9768c` | CardKit streaming + merge_segments for tool-progress | stream_consumer.py, feishu.py |
| 3 | `4276a4e68` | Inject only new tool line into stream consumer | stream_consumer.py |
| 4 | `f89f18290` | Three flashing dots loading indicator | feishu.py |
| 5 | `33b2693e7` | Fix icon nesting for loading indicator | feishu.py |
| 6 | `213ee5419` | Clear loading indicator on streaming stop | feishu.py |
| 7 | `4a1052507` | Replace entire card body on streaming stop | feishu.py |
| 8 | `9f3fb4d51` | Completed status footer with elapsed time | feishu.py |
| 9 | `7853e27fb` | Typing reaction indicator + reply_to on streaming cards | feishu.py |
| 10 | `3a0679360` | Bridge feishu group_rules to extra, require_mention | feishu.py, config.py |
| 11 | `a46ee1a01` | Early typing reaction on message receipt | feishu.py |
| 12 | `caae9d0a0` | Feishu short reply blank card + compression status emit | feishu.py |
| 13 | `70affe608` | Improve blockquote/paragraph spacing in inject | stream_consumer.py |
| 14 | `281d54bac` | Format injected tool-progress as markdown blockquote | stream_consumer.py |

### B. 其他自定义（7 个 commits）

| # | Commit | 描述 | 涉及文件 |
|---|--------|------|---------|
| 15 | `28ea79dc0` | Add tushare/pandas dependencies | pyproject.toml |
| 16 | `ae5f04af2` | Remove extra blank lines (streaming) | stream_consumer.py |
| 17 | `0aec5fea4` | Title generation disable config + code | title_generator.py, config.py, run_agent.py |
| 18 | `46403d2fd` | Inject status_callback closure scope | run.py |
| 19 | `7ee103c3d` | Distinguish terminated/continued in CardKit footer | feishu.py |
| 20 | `30d560283` | Fix rebase fallout (undefined event, missing _add_ack_reaction) | feishu.py |
| 21 | `aed820bfe` | Add generic group_rules bridge to gateway config | config.py |

---

## 三、集成点分析（CardKit 与 v0.14.0 代码的对接）

### 3.1 `gateway/config.py` — StreamConsumerConfig 扩展

**我们的改动：** 添加 `merge_segments` 和 `streaming_mode` 字段到 `StreamConsumerConfig`

**v0.14.0 状态：** `StreamConsumerConfig` 在 L49，`StreamingConfig` 在 L349，两者独立。新增了 `fresh_final_after_seconds` 字段。

**对接方式：** 在 `StreamConsumerConfig` 中添加 `merge_segments` 和 `streaming_mode`，类似之前。

### 3.2 `gateway/stream_consumer.py` — CardKit 路由

**我们的改动：**
- `_INJECT` sentinel 用于 tool-progress 注入
- `merge_segments` 逻辑：tool 输出合并到流式消息
- CardKit 生命周期管理（`_stop_streaming_card_if_active`）
- `_send_or_edit` 添加 `finalize` 参数

**v0.14.0 状态：**
- `_send_or_edit` 已有 `finalize` 参数 ✅
- 有 draft streaming 支持（Telegram sendMessageDraft）
- 新增了 `_send_draft_frame`、`_try_strip_cursor`、`_try_fresh_final` 等方法
- `_resolve_draft_streaming()` 方法判断平台是否支持 draft

**对接策略：**
1. `_INJECT` sentinel + merge_segments 逻辑直接加入，与 upstream 的 draft 逻辑共存
2. CardKit 路由：在 `_send_or_edit` 中添加 CardKit 分支（与 draft/edit 分支并列）
3. `_stop_streaming_card_if_active` 作为新方法添加到 `GatewayStreamConsumer`

### 3.3 `gateway/platforms/feishu.py` — CardKit API 调用

**我们的改动：**
- `send_streaming_card()` — 创建 CardKit 卡片
- `_update_streaming_card_content()` — 流式更新卡片内容
- `stop_streaming_card()` — 停止流式，设置最终状态
- `streaming_cards_enabled` 属性
- `_FeishuStreamingCard` 数据类
- Typing reaction 发送

**v0.14.0 状态：**
- `edit_message()` 只用 `im.v1.message.update`
- `send_typing()` 已存在
- 新增了 `@mention` 保持、topic 线程修复、lazy import 等
- 卡片只用于 approval/update prompt

**对接策略：**
1. CardKit 方法作为独立方法添加到 `FeishuAdapter`，不改 upstream 现有方法
2. 在 `edit_message` 中添加 CardKit 分支：如果 message_id 对应 streaming card，走 CardKit 更新路径
3. `_FeishuStreamingCard` 数据类、`streaming_cards_enabled` 属性直接添加
4. `cardkit.v1` 的 import 加入 lazy import 块

### 3.4 `gateway/run.py` — Stream Consumer 创建 & status_callback

**我们的改动：**
- `merge_segments` 传给 `StreamConsumerConfig`
- `_status_callback_sync` 注入 tool-progress 到 stream consumer
- `_effective_cursor` 逻辑
- Streaming card 的创建/停止调用

**v0.14.0 状态：**
- `_start_stream_consumer` 在 L15904，结构类似
- 新增了 `run_generation` 用于防止 stale agent
- 大量重构（interrupt handling、session management）

**对接策略：** ⚠️ 这是最大难点
1. 找到 `_start_stream_consumer` / `StreamConsumerConfig` 创建点，注入 `merge_segments` + `streaming_mode`
2. 找到 stream consumer 回调注册点，添加 `_status_callback_sync` 闭包
3. `_effective_cursor` 逻辑加入 stream consumer 创建参数

### 3.5 `agent/title_generator.py` — 禁用 title generation

**v0.14.0 状态：** upstream 改了 58 行，可能已有类似机制
**对接方式：** 检查 upstream 是否有 `title_generation.enabled` config，有就直接用 config，没有就保留我们的 patch

---

## 四、执行步骤

### Phase 0: 准备（30 分钟）

```bash
cd /home/ubuntu/hermes-agent

# 1. 确保所有 remote 最新的
git fetch upstream --tags
git fetch origin

# 2. 备份当前工作分支
git branch release-patched-v0.9-snapshot release-patched

# 3. 创建升级分支
git checkout -b release-patched-v0.14 v2026.5.16

# 4. 验证干净构建
source venv/bin/activate
pip install -e .
python -c "import gateway.run; import gateway.stream_consumer; print('✅ v0.14.0 base OK')"
```

### Phase 1: 配置层 patch（简单，30 分钟）

按顺序应用，每个 commit 后验证 import：

1. **`StreamConsumerConfig` 扩展** (`config.py`)
   - 添加 `merge_segments: bool = True`
   - 添加 `streaming_mode: str = ""`
   - 更新 `to_dict()` / `from_dict()`

2. **`StreamingConfig` 扩展** (`config.py`)
   - 添加 `streaming_mode` 支持（如果需要）

3. **Title generation disable** (`title_generator.py`, `config.py`, `run_agent.py`)
   - 先检查 upstream 是否已有等效实现

4. **`pyproject.toml`** — tushare/pandas 依赖

### Phase 2: Feishu CardKit 方法（核心，2-3 小时）

1. **添加 lazy import 块** — `lark_oapi.api.cardkit.v1`
2. **添加 `_FeishuStreamingCard` 数据类**
3. **添加 `streaming_cards_enabled` 属性**
4. **添加 `send_streaming_card()` 方法** — 从我们的 patch 提取，适配 v0.14.0 的 `_client` 结构
5. **添加 `_update_streaming_card_content()` 方法**
6. **添加 `stop_streaming_card()` 方法**
7. **修改 `edit_message()`** — 添加 CardKit 分支
8. **添加 loading indicator、status footer、typing reaction** 等 UI 元素
9. **添加 `group_rules` require_mention 支持**

每个方法添加后验证：
```bash
python -c "from gateway.platforms.feishu import FeishuAdapter; print('✅ feishu OK')"
```

### Phase 3: Stream Consumer 集成（核心，2-3 小时）

1. **添加 `_INJECT` sentinel**
2. **修改 `__init__`** — 检测 `adapter.streaming_cards_enabled`
3. **修改 `_filter_and_accumulate`** — 处理 _INJECT 消息
4. **修改 `_send_or_edit`** — 添加 CardKit 分支（在 draft/edit 路径旁边）
5. **添加 `_stop_streaming_card_if_active`** 方法
6. **添加 merge_segments 边界处理逻辑**
7. **修改 `finish()`** — 确保停止 streaming card

### Phase 4: Gateway Runner 集成（最难点，2-3 小时）

1. **找到 `StreamConsumerConfig` 创建点** — 注入 `merge_segments` + `streaming_mode`
2. **找到 stream delta callback** — 添加 CardKit 路由
3. **添加 `_status_callback_sync` 闭包** — tool-progress 注入
4. **添加 `_effective_cursor` 逻辑**

### Phase 5: 测试 & 部署（1-2 小时）

```bash
# 1. Import 检查
python -c "
import gateway.platforms.feishu
import gateway.stream_consumer
import gateway.config
import gateway.run
import run_agent
print('✅ All imports OK')
"

# 2. 功能标记检查
python -c "
import gateway.stream_consumer as sc
assert hasattr(sc, '_INJECT'), '_INJECT missing'
print('✅ _INJECT present')
"

# 3. 用 foreman-lobster 测试（遵循"不重启 default gateway"铁律）
hermes --profile foreman-lobster gateway restart

# 4. 在飞书发送测试消息，验证：
#    - CardKit 流式卡片正常渲染
#    - Loading 动画显示
#    - 完成后状态 footer 显示
#    - Tool progress 注入正常
#    - Typing reaction 显示

# 5. 确认无误后推送
git push origin release-patched-v0.14
```

---

## 五、回滚方案

如果升级后出问题：

```bash
# 回滚到 v0.9.0 快照
git checkout release-patched-v0.9-snapshot

# 重启所有 gateway
hermes --profile foreman-lobster gateway restart
# 确认 OK 后重启其他 profile
```

---

## 六、时间估算

| 阶段 | 估计时间 | 风险 |
|------|---------|------|
| Phase 0: 准备 | 30 min | 🟢 |
| Phase 1: 配置层 | 30 min | 🟢 |
| Phase 2: Feishu CardKit | 2-3 hr | 🟡 |
| Phase 3: Stream Consumer | 2-3 hr | 🟠 |
| Phase 4: Gateway Runner | 2-3 hr | 🔴 |
| Phase 5: 测试部署 | 1-2 hr | 🟡 |
| **总计** | **8-12 hr** | |

---

## 七、注意事项

1. **永远不重启 default gateway（龙虾人）** — 只用 foreman-lobster 测试
2. **改 py 后清 `__pycache__`** — `find . -name __pycache__ -exec rm -rf {} +`
3. **不用 hack** — 如果 upstream 的 `run.py` 结构大变，优先适配 upstream 结构而非强行注入
4. **CardKit 是独一份** — upstream 到 v0.14.0 都没有 CardKit 实现，这部分必须完整保留
5. **考虑拆分** — 如果 Phase 4 太痛苦，可以把 CardKit 逻辑做成 mixin/plugin，减少与 `run.py` 的耦合
