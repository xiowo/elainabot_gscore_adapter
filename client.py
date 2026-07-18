import asyncio
import base64
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

import websockets.client
from core.base.logger import PLUGIN, get_logger
from msgspec import json as msgjson
from websockets.exceptions import ConnectionClosed

from .models import Message, MessageReceive, MessageSend, RecallMessageId

SendFunc = Callable[[MessageSend], Awaitable[RecallMessageId]]
ControlFunc = Callable[[MessageSend, Dict[str, Any]], Awaitable[None]]

log = get_logger(PLUGIN, "早柚适配器")


class GsCoreClient:
    def __init__(
        self,
        *,
        route_bot_id: str,
        platform_bot_id: str,
        self_id: str,
        host: str,
        port: int,
        token: str = "",
        use_ssl: bool = False,
        max_size: Optional[int] = None,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = 10,
        send_func: Optional[SendFunc] = None,
        delete_func: Optional[ControlFunc] = None,
        ban_func: Optional[ControlFunc] = None,
    ):
        self.route_bot_id = route_bot_id
        self.platform_bot_id = platform_bot_id
        self.self_id = self_id
        self.host = host
        self.port = port
        self.token = token
        self.use_ssl = use_ssl
        self.max_size = max_size
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.send_func = send_func
        self.delete_func = delete_func
        self.ban_func = ban_func

        self.ws = None
        self.queue: asyncio.Queue[MessageReceive] = asyncio.Queue()
        self._running = False
        self._runner: Optional[asyncio.Task] = None

    @property
    def url(self) -> str:
        scheme = "wss" if self.use_ssl else "ws"
        url = f"{scheme}://{self.host}:{self.port}/ws/{self.route_bot_id}"
        if self.token:
            url += f"?token={self.token}"
        return url

    @property
    def safe_url(self) -> str:
        scheme = "wss" if self.use_ssl else "ws"
        url = f"{scheme}://{self.host}:{self.port}/ws/{self.route_bot_id}"
        if self.token:
            url += "?token=***"
        return url

    async def start(self) -> None:
        if self._runner and not self._runner.done():
            return
        self._running = True
        self._runner = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._running = False
        if self.ws is not None:
            await self.ws.close()
            self.ws = None
        if self._runner and not self._runner.done():
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
        self._runner = None

    async def input(self, msg: MessageReceive) -> None:
        await self.queue.put(msg)

    async def report_recall_receipt(self, msg: MessageSend, recall_id: RecallMessageId) -> None:
        await self.input(
            MessageReceive(
                bot_id=msg.bot_id,
                bot_self_id=msg.bot_self_id or self.self_id,
                content=[
                    Message(
                        "recall_message_id",
                        {"echo": msg.echo, "id": recall_id},
                    )
                ],
            )
        )

    async def _run_forever(self) -> None:
        reconnect_attempts = 0
        while self._running:
            try:
                await self._connect()
                await self._run_pair()
                if self._running:
                    log.warning("早柚连接已断开: %s", self.safe_url)
            except ConnectionClosed as exc:
                if self._running:
                    reconnect_attempts += 1
                    log.warning("早柚连接已关闭: %s", exc)
                    if self.max_reconnect_attempts > 0 and reconnect_attempts >= self.max_reconnect_attempts:
                        log.error("早柚连接在 %s 次尝试后停止", reconnect_attempts)
                        self._running = False
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnect_attempts += 1
                log.warning("早柚连接失败: %s", exc)
                if self.max_reconnect_attempts > 0 and reconnect_attempts >= self.max_reconnect_attempts:
                    log.error("早柚连接在 %s 次尝试后停止", reconnect_attempts)
                    self._running = False
                    break
            if self._running:
                await asyncio.sleep(self.reconnect_interval)
            self.ws = None

    async def _connect(self) -> None:
        self.ws = await websockets.client.connect(
            self.url,
            max_size=self.max_size,
            open_timeout=60,
            ping_timeout=60,
        )
        log.info("已连接早柚: %s", self.safe_url)

    async def _run_pair(self) -> None:
        recv_task = asyncio.create_task(self._recv_loop())
        send_task = asyncio.create_task(self._send_loop())
        tasks = [recv_task, send_task]
        try:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    continue
                if isinstance(result, ConnectionClosed) and not self._running:
                    continue
                if isinstance(result, BaseException):
                    raise result
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_loop(self) -> None:
        while self._running:
            msg = await self.queue.get()
            if self.ws is None:
                await self.queue.put(msg)
                raise ConnectionClosed(None, None)
            await self.ws.send(msgjson.encode(msg))

    async def _recv_loop(self) -> None:
        if self.ws is None:
            return
        async for raw in self.ws:
            try:
                msg = msgjson.decode(raw, type=MessageSend)
                await self._handle_send(msg)
            except Exception:
                log.exception("处理早柚下发消息失败")

    async def _handle_send(self, msg: MessageSend) -> None:
        if self._handle_log_packet(msg):
            return

        if self._is_control_packet(msg, "excute_delete_message"):
            data = msg.content[0].data or {}
            if self.delete_func:
                await self.delete_func(msg, data)
            else:
                log.warning("未配置撤回消息处理函数: %s", data)
            return

        if self._is_control_packet(msg, "excute_ban_user"):
            data = msg.content[0].data or {}
            if self.ban_func:
                await self.ban_func(msg, data)
            else:
                log.warning("当前适配器不支持禁言用户: %s", data)
            return

        if msg.bot_id != self.platform_bot_id:
            log.debug("忽略非官机平台的早柚下发消息: bot_id=%s", msg.bot_id)
            return

        recall_id: RecallMessageId = None
        try:
            if self.send_func:
                recall_id = await self.send_func(msg)
        finally:
            if msg.echo:
                await self.report_recall_receipt(msg, recall_id)

    def _handle_log_packet(self, msg: MessageSend) -> bool:
        if msg.bot_id != self.route_bot_id or not msg.content:
            return False
        first = msg.content[0]
        msg_type = first.type or ""
        if not msg_type.startswith("log_"):
            return False
        level = msg_type.split("_")[-1].lower()
        logger_func = getattr(log, level, log.info)
        logger_func("%s", first.data)
        return True

    @staticmethod
    def _is_control_packet(msg: MessageSend, msg_type: str) -> bool:
        return bool(msg.content and len(msg.content) == 1 and msg.content[0].type == msg_type)


def split_send_content(content: Optional[Iterable[Message]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "text": "",
        "image": None,
        "record": None,
        "video": None,
        "file": None,
        "node": [],
        "at_list": [],
        "reply": None,
        "markdown": "",
        "buttons": [],
        "template_markdown": {},
        "template_buttons": "",
        "ordered": [],
    }
    for item in content or []:
        if not item.data:
            continue
        if item.type == "text":
            result["text"] += str(item.data)
            result["ordered"].append(("text", str(item.data)))
        elif item.type == "image":
            result["image"] = item.data
            result["ordered"].append(("image", item.data))
        elif item.type == "record":
            result["record"] = item.data
        elif item.type == "video":
            result["video"] = item.data
        elif item.type == "file":
            result["file"] = item.data
        elif item.type == "node":
            result["node"] = item.data
        elif item.type == "at":
            result["at_list"].append(str(item.data))
        elif item.type == "reply":
            result["reply"] = item.data
        elif item.type == "markdown":
            result["markdown"] = item.data
            result["ordered"].append(("text", str(item.data)))
        elif item.type == "buttons":
            result["buttons"] = item.data
        elif item.type == "template_markdown":
            result["template_markdown"] = item.data
            result["ordered"].append(("text", str(item.data)))
        elif item.type == "template_buttons":
            result["template_buttons"] = item.data
    return result


def decode_media_payload(data: str) -> Tuple[str, Any]:
    if data.startswith("link://"):
        return "url", data[7:]
    if data.startswith("base64://"):
        return "bytes", base64.b64decode(data[9:])
    return "url", data
