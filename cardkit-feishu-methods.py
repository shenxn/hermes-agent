"""
CardKit streaming card additions for gateway/platforms/feishu.py

This file consolidates all CardKit-related feishu.py changes from custom-patches-v0.9.txt
into a single clean reference. Each section is annotated with where it should be inserted
into the upstream feishu.py file.

Commits incorporated:
  8dbf50b09 - Cherry-pick CardKit streaming cards PR (initial implementation)
  a14c9768c - CardKit streaming + merge_segments for tool-progress injection
  f89f18290 - Three flashing dots loading indicator
  33b2693e7 - Fix icon nesting for loading indicator
  213ee5419 - Clear loading indicator on streaming stop
  4a1052507 - Replace entire card body on streaming stop
  9f3fb4d518 - Completed status footer with elapsed time
  7853e27fb - Typing reaction indicator + reply_to on streaming cards
  a46ee1a01 - Early typing reaction on message receipt for Feishu
  caae9d0a0 - Feishu short reply blank card + compression status emit
  7ee103c3d - Distinguish terminated/continued status in CardKit footer
  30d560283 - Fix rebase fallout (undefined event, missing _add_ack_reaction)
"""

# ============================================================================
# SECTION 1: IMPORTS
# Insert after existing lark_oapi imports in the try block
# (around line 105 in upstream feishu.py, after "from lark_oapi.core.model import BaseRequest")
# ============================================================================

# --- CardKit v1 imports ---
from lark_oapi.api.cardkit.v1 import (
    CreateCardRequest,
    CreateCardRequestBody,
    ContentCardElementRequest,
    ContentCardElementRequestBody,
    SettingsCardRequest,
    SettingsCardRequestBody,
    UpdateCardRequest,
    UpdateCardRequestBody,
)

# --- IM reaction imports (for Typing reaction lifecycle) ---
from lark_oapi.api.im.v1 import (
    CreateMessageReactionRequest,
    CreateMessageReactionRequestBody,
    DeleteMessageReactionRequest,
)
from lark_oapi.api.im.v1.model.emoji import Emoji

# --- After the try/except block, set availability flag ---
# Inside the try block (after FEISHU_AVAILABLE = True):
#     FEISHU_CARDKIT_AVAILABLE = True
# Inside the except block (after FEISHU_AVAILABLE = False):
#     FEISHU_CARDKIT_AVAILABLE = False
#     CreateCardRequest = None  # type: ignore[assignment]
#     CreateCardRequestBody = None  # type: ignore[assignment]
#     ContentCardElementRequest = None  # type: ignore[assignment]
#     ContentCardElementRequestBody = None  # type: ignore[assignment]
#     SettingsCardRequest = None  # type: ignore[assignment]
#     SettingsCardRequestBody = None  # type: ignore[assignment]
#     UpdateCardRequest = None  # type: ignore[assignment]
#     UpdateCardRequestBody = None  # type: ignore[assignment]


# ============================================================================
# SECTION 2: STREAMING CARD CONSTANTS
# Insert after _ONBOARD_REQUEST_TIMEOUT_S (near the module-level constants block)
# ============================================================================

# ---------------------------------------------------------------------------
# Streaming card constants
# ---------------------------------------------------------------------------

_STREAMING_CARD_ELEMENT_ID = "streaming_md_1"
_STREAMING_LOADING_ELEMENT_ID = "streaming_loading"
_STREAMING_LOADING_ICON_KEY = "img_v3_02vb_496bec09-4b43-4773-ad6b-0cdd103cd2bg"
_TYPING_EMOJI_TYPE = "Typing"  # Feishu built-in keyboard/typing animation emoji
_STREAMING_CARD_PRINT_FREQUENCY_MS = 50
_STREAMING_CARD_PRINT_STEP = 2
_STREAMING_CARD_PRINT_STRATEGY = "fast"


# ============================================================================
# SECTION 3: DATA CLASS
# Insert near FeishuBatchState / FeishuAdapterSettings (module-level dataclasses)
# ============================================================================

@dataclass
class _FeishuStreamingCard:
    """Tracks state for a single streaming card session."""

    card_id: str
    element_id: str
    message_id: str
    sequence: int = 1  # Strictly increasing across all card operations
    last_sent_content: str = ""  # Track what was last sent for delta optimization
    created_at: float = 0.0  # When the card was created (for elapsed time)
    typing_reaction_id: Optional[str] = None  # Typing emoji reaction ID for cleanup
    reply_to_message_id: Optional[str] = None  # Original message we're replying to


# ============================================================================
# SECTION 4: FEISHUADAPTER.__INIT__ ADDITIONS
# Add these lines inside FeishuAdapter.__init__(), after the
# _pending_processing_reactions OrderedDict initialization
# ============================================================================

# CardKit streaming card state (message_id → _FeishuStreamingCard)
self._streaming_cards: Dict[str, _FeishuStreamingCard] = {}

# Early typing reactions: inbound message_id → reaction_id
# Added immediately on receipt so users see typing before LLM TTFT.
# Handed off to streaming card or cleaned up on completion.
self._early_typing_reactions: Dict[str, str] = {}


# ============================================================================
# SECTION 5: EDIT_MESSAGE MODIFICATION
# The existing edit_message() method needs a CardKit routing guard at the top.
# Replace the docstring and add the routing block after the "Not connected" check.
# ============================================================================

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent Feishu text/post message.

        If the *message_id* is associated with an active streaming card
        (created via :meth:`send_streaming_card`), the edit is performed via
        the CardKit streaming text update API instead of the regular IM update.
        """
        if not self._client:
            return SendResult(success=False, error="Not connected")

        # --- CardKit streaming card path ---
        sc = self._streaming_cards.get(message_id)
        if sc is not None:
            return await self._update_streaming_card_content(sc, content)

        # --- Regular IM update path (existing upstream code continues below) ---
        content = self.format_message(content)
        # ... rest of existing edit_message code ...


# ============================================================================
# SECTION 6: CARDKIT STREAMING CARD PROPERTIES AND METHODS
# Insert after edit_message() (or after send_exec_approval / similar location)
# These are all methods on the FeishuAdapter class.
# ============================================================================

    # =========================================================================
    # CardKit streaming card support
    # =========================================================================

    @property
    def streaming_cards_enabled(self) -> bool:
        """Return *True* if the CardKit streaming API is usable."""
        return bool(FEISHU_CARDKIT_AVAILABLE and self._client)

    async def send_streaming_card(
        self,
        chat_id: str,
        content: str = "",
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Create a streaming card entity and send it as a message.

        Returns a :class:`SendResult` whose *message_id* can be used with
        :meth:`edit_message` (which will route through the CardKit streaming
        text update API) and finally :meth:`stop_streaming_card`.
        """
        if not self._client:
            return SendResult(success=False, error="Not connected")
        if not FEISHU_CARDKIT_AVAILABLE:
            return SendResult(success=False, error="CardKit SDK not available")

        try:
            # 1. Create card entity with streaming_mode enabled
            card_json = self._build_streaming_card_json(content)
            create_body = (
                CreateCardRequestBody.builder()
                .type("card_json")
                .data(json.dumps(card_json, ensure_ascii=False))
                .build()
            )
            create_req = (
                CreateCardRequest.builder()
                .request_body(create_body)
                .build()
            )
            create_resp = await asyncio.to_thread(
                self._client.cardkit.v1.card.create, create_req,
            )
            if not create_resp or create_resp.code != 0:
                err_msg = getattr(create_resp, "msg", "unknown error") if create_resp else "no response"
                logger.warning("[Feishu] CardKit card.create failed: code=%s msg=%s",
                               getattr(create_resp, "code", "?"), err_msg)
                return SendResult(success=False, error=f"CardKit create failed: {err_msg}")

            card_id = create_resp.data.card_id
            logger.debug("[Feishu] Created streaming card: %s", card_id)

            # 2. Send the card as a message via IM API
            card_content = json.dumps(
                {"type": "card", "data": {"card_id": card_id}},
                ensure_ascii=False,
            )
            response = await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=card_content,
                reply_to=reply_to,
                metadata=metadata,
            )
            result = self._finalize_send_result(response, "send streaming card failed")
            if not result.success:
                return result

            message_id = result.message_id

            # 3. Add "Typing" reaction to original message (best-effort)
            #    Follows OpenClaw pattern: create → capture reaction_id → delete on complete.
            #    If an early typing reaction was already added in on_processing_start,
            #    reuse it instead of creating a duplicate.
            typing_reaction_id: Optional[str] = None
            if reply_to:
                # Check for an existing early typing reaction from on_processing_start
                early_rid = self._early_typing_reactions.pop(reply_to, None)
                if early_rid:
                    typing_reaction_id = early_rid
                    logger.debug("[Feishu] Reusing early typing reaction for %s → %s",
                                 reply_to, typing_reaction_id)
                else:
                    try:
                        react_body = (
                            CreateMessageReactionRequestBody.builder()
                            .reaction_type(
                                Emoji.builder().emoji_type(_TYPING_EMOJI_TYPE).build()
                            )
                            .build()
                        )
                        react_req = (
                            CreateMessageReactionRequest.builder()
                            .message_id(reply_to)
                            .request_body(react_body)
                            .build()
                        )
                        react_resp = await asyncio.to_thread(
                            self._client.im.v1.message_reaction.create, react_req,
                        )
                        if react_resp and react_resp.code == 0:
                            typing_reaction_id = getattr(react_resp.data, "reaction_id", None)
                            logger.debug("[Feishu] Added Typing reaction to %s → %s",
                                         reply_to, typing_reaction_id)
                    except Exception as react_exc:
                        logger.debug("[Feishu] Failed to add Typing reaction: %s", react_exc)

            # 4. Track the streaming card state
            sc = _FeishuStreamingCard(
                card_id=card_id,
                element_id=_STREAMING_CARD_ELEMENT_ID,
                message_id=message_id,
                sequence=1,
                created_at=time.time(),
                typing_reaction_id=typing_reaction_id,
                reply_to_message_id=reply_to,
                last_sent_content=content,  # Track initial content for stop_streaming_card
            )
            self._streaming_cards[message_id] = sc
            logger.debug("[Feishu] Streaming card %s linked to message %s", card_id, message_id)
            return result

        except Exception as exc:
            logger.error("[Feishu] send_streaming_card error: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def _update_streaming_card_content(
        self,
        sc: "_FeishuStreamingCard",
        content: str,
    ) -> SendResult:
        """Push a streaming text update to a card element.

        *content* should be the **full accumulated text** — this method
        computes the delta against ``sc.last_sent_content`` so that Feishu's
        typewriter effect only animates **new characters**, avoiding a
        full replay of all historical text on every tool-call boundary.
        """
        try:
            # Delta optimisation: only send the new suffix.
            prev = sc.last_sent_content
            if content.startswith(prev) and len(content) > len(prev):
                delta = content[len(prev):]
                # Reconstruct what Feishu should display after this update
                send_content = content  # CardKit needs full state for rendering
                logger.debug("[Feishu] CardKit delta: +%d chars (total %d)",
                             len(delta), len(content))
            else:
                # Full replacement (first send or non-contiguous change)
                delta = None
                send_content = content

            sc.sequence += 1
            body = (
                ContentCardElementRequestBody.builder()
                .uuid(str(uuid.uuid4()))
                .sequence(sc.sequence)
                .content(send_content)
                .build()
            )
            req = (
                ContentCardElementRequest.builder()
                .card_id(sc.card_id)
                .element_id(sc.element_id)
                .request_body(body)
                .build()
            )
            resp = await asyncio.to_thread(
                self._client.cardkit.v1.card_element.content, req,
            )
            if resp and resp.code == 0:
                sc.last_sent_content = content
                return SendResult(success=True, message_id=sc.message_id)
            err_msg = getattr(resp, "msg", "unknown") if resp else "no response"
            logger.warning("[Feishu] Streaming card content update failed: code=%s msg=%s",
                           getattr(resp, "code", "?"), err_msg)
            return SendResult(success=False, error=f"streaming update failed: {err_msg}")
        except Exception as exc:
            logger.error("[Feishu] _update_streaming_card_content error: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def stop_streaming_card(self, message_id: str, *, status: str = "completed") -> bool:
        """Disable streaming mode on a card and clean up tracking state.

        Replaces the entire card body with one that contains only the text
        element (no loading indicator).  This mirrors the OpenClaw plugin's
        closeStreamingAndUpdate → updateCardKitCard flow.

        *status* controls the footer text shown on the card:

        - ``"completed"`` — normal completion (✅ 已完成)
        - ``"terminated"`` — cancelled or errored out (⏹ 已终止)
        - ``"continued"`` — split/overflow, a new card follows (⏸ 待续 →)

        Returns *True* on success.  Safe to call even if the message is not a
        streaming card (returns *True* immediately).
        """
        from lark_oapi.api.cardkit.v1.model.card import Card

        sc = self._streaming_cards.pop(message_id, None)
        if sc is None:
            return True
        if not self._client:
            return False
        try:
            # Step 1: disable streaming mode
            sc.sequence += 1
            settings_json = json.dumps({"config": {"streaming_mode": False}}, ensure_ascii=False)
            body = (
                SettingsCardRequestBody.builder()
                .settings(settings_json)
                .sequence(sc.sequence)
                .build()
            )
            req = (
                SettingsCardRequest.builder()
                .card_id(sc.card_id)
                .request_body(body)
                .build()
            )
            resp = await asyncio.to_thread(
                self._client.cardkit.v1.card.settings, req,
            )

            # Step 2: replace entire card body (removes loading indicator)
            # Build a final card with text + footer (status + elapsed time)
            elapsed_ms = (time.time() - sc.created_at) * 1000
            elapsed_sec = elapsed_ms / 1000
            elapsed_str = f"{elapsed_sec:.1f}s" if elapsed_sec < 60 else f"{int(elapsed_sec // 60)}m {int(elapsed_sec % 60)}s"
            _STATUS_FOOTERS = {
                "completed": f"✅ 已完成 · 耗时 {elapsed_str}",
                "terminated": f"⏹ 已终止 · 耗时 {elapsed_str}",
                "continued": f"⏸ 待续 → · 耗时 {elapsed_str}",
            }
            footer_content = _STATUS_FOOTERS.get(status, _STATUS_FOOTERS["completed"])

            final_elements = [
                {
                    "tag": "markdown",
                    "content": sc.last_sent_content,
                    "element_id": _STREAMING_CARD_ELEMENT_ID,
                },
                {
                    "tag": "markdown",
                    "content": footer_content,
                    "text_size": "notation",
                },
            ]
            final_card_json = {
                "schema": "2.0",
                "config": {"streaming_mode": False},
                "body": {"elements": final_elements},
            }
            sc.sequence += 1
            update_body = (
                UpdateCardRequestBody.builder()
                .card(
                    Card.builder()
                    .type("card_json")
                    .data(json.dumps(final_card_json, ensure_ascii=False))
                    .build()
                )
                .uuid(str(uuid.uuid4()))
                .sequence(sc.sequence)
                .build()
            )
            update_req = (
                UpdateCardRequest.builder()
                .card_id(sc.card_id)
                .request_body(update_body)
                .build()
            )
            update_resp = await asyncio.to_thread(
                self._client.cardkit.v1.card.update, update_req,
            )
            if update_resp and update_resp.code != 0:
                logger.warning("[Feishu] Failed to replace final card body: code=%s msg=%s",
                               getattr(update_resp, "code", "?"), getattr(update_resp, "msg", "?"))

            # Step 3: remove "Typing" reaction from original message (best-effort)
            if sc.typing_reaction_id and sc.reply_to_message_id:
                try:
                    del_req = (
                        DeleteMessageReactionRequest.builder()
                        .message_id(sc.reply_to_message_id)
                        .reaction_id(sc.typing_reaction_id)
                        .build()
                    )
                    await asyncio.to_thread(
                        self._client.im.v1.message_reaction.delete, del_req,
                    )
                    logger.debug("[Feishu] Removed Typing reaction from %s",
                                 sc.reply_to_message_id)
                except Exception as del_exc:
                    logger.debug("[Feishu] Failed to remove Typing reaction: %s", del_exc)

            if resp and resp.code == 0:
                logger.debug("[Feishu] Stopped streaming card %s", sc.card_id)
                return True
            logger.warning("[Feishu] Failed to stop streaming card %s: code=%s msg=%s",
                           sc.card_id, getattr(resp, "code", "?"), getattr(resp, "msg", "?"))
            return False
        except Exception as exc:
            logger.error("[Feishu] stop_streaming_card error: %s", exc, exc_info=True)
            return False

    @staticmethod
    def _build_streaming_card_json(initial_content: str = "") -> dict:
        """Build a Card JSON 2.0 structure with streaming mode enabled."""
        return {
            "schema": "2.0",
            "config": {
                "streaming_mode": True,
                "summary": {"content": ""},
                "streaming_config": {
                    "print_frequency_ms": {"default": _STREAMING_CARD_PRINT_FREQUENCY_MS},
                    "print_step": {"default": _STREAMING_CARD_PRINT_STEP},
                    "print_strategy": _STREAMING_CARD_PRINT_STRATEGY,
                },
            },
            "body": {
                "elements": [
                    {
                        "tag": "markdown",
                        "content": initial_content,
                        "element_id": _STREAMING_CARD_ELEMENT_ID,
                    },
                    {
                        "tag": "markdown",
                        "content": " ",
                        "icon": {
                            "tag": "custom_icon",
                            "img_key": _STREAMING_LOADING_ICON_KEY,
                            "size": "16px 16px",
                        },
                        "element_id": _STREAMING_LOADING_ELEMENT_ID,
                    },
                ],
            },
        }


# ============================================================================
# SECTION 7: REACTION LIFECYCLE OVERRIDES
# Override on_processing_start() and on_processing_complete() on FeishuAdapter
# to add early typing reaction support.
# ============================================================================

    # =========================================================================
    # Early typing reaction — overrides from BasePlatformAdapter
    # =========================================================================

    async def on_processing_start(self, event: MessageEvent) -> None:
        """Add early Typing + processing reactions on message receipt.

        Two reactions are added:
        1. **Early Typing** — added immediately via ``_add_reaction`` and tracked
           in ``_early_typing_reactions``.  This gives the user instant visual feedback.
        2. **Processing (IN_PROGRESS)** — added via ``_add_reaction`` and tracked in
           ``_pending_processing_reactions``.  This is the standard upstream lifecycle.
        """
        # --- Early typing reaction: instant visual feedback ---
        message_id = getattr(event, "message_id", None)
        if message_id:
            early_rid = await self._add_reaction(message_id, _TYPING_EMOJI_TYPE)
            if early_rid:
                self._early_typing_reactions[message_id] = early_rid
                logger.debug(
                    "[Feishu] Early typing reaction added to %s → %s",
                    message_id, early_rid,
                )

        # --- Processing reaction: standard lifecycle ---
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        # ... rest of upstream on_processing_start code ...

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        """Clean up processing and early typing reactions.

        1. Removes the IN_PROGRESS reaction and optionally adds a FAILURE badge
           (standard upstream lifecycle).
        2. Cleans up any early Typing reaction that was not claimed by a streaming card.
        """
        # --- Processing reaction: standard lifecycle ---
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        # ... existing upstream processing reaction cleanup code ...

        # (After the existing upstream failure-reaction logic:)

        # --- Early typing reaction: clean up orphan ---
        early_rid = self._early_typing_reactions.pop(message_id, None)
        if early_rid:
            # Check if an active streaming card claimed it
            for sc in self._streaming_cards.values():
                if sc.reply_to_message_id == message_id and sc.typing_reaction_id == early_rid:
                    # Belongs to an active streaming card — put it back
                    self._early_typing_reactions[message_id] = early_rid
                    return
            # No streaming card claimed it — remove the orphan reaction
            try:
                from lark_oapi.api.im.v1 import DeleteMessageReactionRequest
                del_req = (
                    DeleteMessageReactionRequest.builder()
                    .message_id(message_id)
                    .reaction_id(early_rid)
                    .build()
                )
                await asyncio.to_thread(
                    self._client.im.v1.message_reaction.delete, del_req,
                )
                logger.debug(
                    "[Feishu] Cleaned up early typing reaction on %s", message_id,
                )
            except Exception:
                logger.debug(
                    "[Feishu] Failed to clean up early typing reaction on %s",
                    message_id, exc_info=True,
                )


# ============================================================================
# SECTION 8: _add_reaction FIX
# The upstream _add_ack_reaction() was removed in a refactor.
# Ensure _add_reaction() returns the reaction_id (needed by early typing flow).
# Verify that the existing _add_reaction() method returns the reaction_id
# and checks response.code == 0 (not response.success()).
# ============================================================================

    # In the existing _add_reaction / _add_ack_reaction method, ensure:
    # - response.code == 0  (NOT response.success())
    # - Returns getattr(data, "reaction_id", None)
    # - Logging at debug level (not warning) for ACK reactions
    #
    # Example fix from commit a46ee1a01:
    #   if response and response.code == 0:
    #       data = getattr(response, "data", None)
    #       reaction_id = getattr(data, "reaction_id", None)
    #       logger.debug("[Feishu] ACK reaction added to %s → %s", message_id, reaction_id)
    #       return reaction_id
