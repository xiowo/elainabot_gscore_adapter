from __future__ import annotations

import asyncio
import base64
import binascii
from collections import OrderedDict
from io import BytesIO
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml
from aiohttp import ClientError, ClientSession, web
from PIL import Image, UnidentifiedImageError

import core.plugin.context as _ctx_mod
from core.base.config import cfg as global_config
from core.base.logger import PLUGIN, get_logger, report_error
from core.message._http import MSG_TYPE_MARKDOWN
from core.message.response import extract_message_id, extract_reference_id
from core.plugin.decorators import handler, interceptor, on_load, on_unload
from core.plugin.web_pages import register_page, register_route, unregister_page, unregister_route

from .client import GsCoreClient, decode_media_payload, split_send_content
from .models import Message, MessageReceive, MessageSend

__plugin_meta__ = {
    "name": "ElainaBot 早柚适配器",
    "author": "MortalCat",
    "description": "一个适用于ElainaBot的GScore适配器 ",
    "version": "1.1.0",
    "license": "MIT",
}

log = get_logger(PLUGIN, "早柚适配器")
ctx = _ctx_mod.ctx

PLATFORM_ID = "qqgroup"
ADAPTER_ROUTE_PREFIX = "ElainaBot"

DEFAULT_CONFIG = {
    "enabled_bots": [],
    "host": "127.0.0.1",
    "port": 8765,
    "token": "",
    "use_ssl": False,
    "reconnect_interval": 5,
    "max_reconnect_attempts": 10,
    "command_prefix": "#早柚",
    "unauthorized_silent": False,
    "disabled_groups": [],
    "blocked_users": [],
    "use_yunzai_user_id": False,
    "send_unsupported_as_text": True,
    "private_json_file_to_base64": False,
    "private_json_file_max_size_kb": 2048,
}

PAGE_KEY = "elainabot-gscore-adapter"
CONFIG_API = "/api/ext/elainabot-gscore-adapter/config"
EVENT_CACHE_TTL = 5 * 60

_clients: Dict[str, GsCoreClient] = {}
_last_events: "OrderedDict[str, Tuple[float, Any]]" = OrderedDict()
_started_at = 0.0
_manual_reconnecting = False


@on_load
async def init():
    register_page(
        key=PAGE_KEY,
        label="GScore 适配器",
        source="plugin",
        source_name="elainabot_gscore_adapter",
        html_file=str(ctx.get_resource_path("page.html")),
        icon="settings",
    )
    register_route("GET", CONFIG_API, _handle_get_config, auth=False)
    register_route("POST", CONFIG_API, _handle_save_config, auth=False)

    config = _normalize_config(ctx.ensure_config(DEFAULT_CONFIG, filename="config.yaml"))
    ctx.save_config(config, filename="config.yaml")
    await _apply_config(config)


@on_unload
async def cleanup():
    unregister_page(PAGE_KEY)
    unregister_route("GET", CONFIG_API)
    unregister_route("POST", CONFIG_API)
    await _stop_clients()
    log.info("早柚适配器已卸载")


async def _handle_get_config(request):
    config = _normalize_config(ctx.ensure_config(DEFAULT_CONFIG, filename="config.yaml"))
    return web.json_response(_build_config_payload(config))


async def _handle_save_config(request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"success": False, "error": "请求格式错误"}, status=400)

    old_config = ctx.read_config(filename="config.yaml") or {}
    if "token" not in body and old_config.get("token"):
        body["token"] = old_config.get("token")

    config = _normalize_config(body)
    ctx.save_config(config, filename="config.yaml")
    await _apply_config(config)
    return web.json_response(_build_config_payload(config))


async def _apply_config(config: Dict[str, Any]):
    await _stop_clients()
    bot_map = _get_framework_bot_map()
    enabled_bot_ids = set(config.get("enabled_bots") or [])
    selected_bots = [bot for bot in bot_map.values() if bot["bot_id"] in enabled_bot_ids and bot.get("framework_enabled", True)]

    if not selected_bots:
        log.info("早柚适配器未启用任何 bot")
        return

    global _started_at
    for bot in selected_bots:
        route_bot_id = str(bot["bot_id"])
        client = GsCoreClient(
            route_bot_id=route_bot_id,
            platform_bot_id=PLATFORM_ID,
            self_id=str(bot.get("robot_qq") or bot.get("appid") or ""),
            host=str(config["host"]),
            port=int(config["port"]),
            token=str(config.get("token") or ""),
            use_ssl=bool(config.get("use_ssl", False)),
            max_size=None,
            reconnect_interval=int(config.get("reconnect_interval", 5)),
            max_reconnect_attempts=int(config.get("max_reconnect_attempts", 10)),
            send_func=_send_to_elaina,
            delete_func=_delete_message,
            ban_func=_ban_user,
        )
        _clients[route_bot_id] = client
        await client.start()

    _started_at = time.time()


async def _stop_clients():
    global _started_at
    for client in list(_clients.values()):
        await client.stop()
    _clients.clear()
    _started_at = 0.0


def _normalize_config(data: Dict[str, Any]) -> Dict[str, Any]:
    migrated_enabled_bots = data.get("enabled_bots")
    if migrated_enabled_bots is None:
        legacy_route_bot_id = str(data.get("route_bot_id") or "").strip()
        legacy_platform_bot_id = str(data.get("platform_bot_id") or "").strip()
        migrated_enabled_bots = [item for item in (legacy_route_bot_id, legacy_platform_bot_id) if item]

    valid_bot_ids = {bot["bot_id"] for bot in _get_framework_bot_map().values() if bot.get("bot_id")}
    enabled_bots = []
    for bot_id in migrated_enabled_bots or []:
        bot_id = str(bot_id).strip()
        if bot_id and bot_id in valid_bot_ids and bot_id not in enabled_bots:
            enabled_bots.append(bot_id)

    config = dict(DEFAULT_CONFIG)
    config.update(
        {
            "enabled_bots": enabled_bots,
            "host": str(data.get("host") or DEFAULT_CONFIG["host"]),
            "port": _to_int(data.get("port"), DEFAULT_CONFIG["port"], 1, 65535),
            "token": str(data.get("token") or ""),
            "use_ssl": bool(data.get("use_ssl", DEFAULT_CONFIG["use_ssl"])),
            "reconnect_interval": _to_int(data.get("reconnect_interval"), DEFAULT_CONFIG["reconnect_interval"], 1, 3600),
            "max_reconnect_attempts": _to_int(data.get("max_reconnect_attempts"), DEFAULT_CONFIG["max_reconnect_attempts"], 0, 1000000),
            "command_prefix": str(data.get("command_prefix") or DEFAULT_CONFIG["command_prefix"]).strip() or DEFAULT_CONFIG["command_prefix"],
            "unauthorized_silent": bool(data.get("unauthorized_silent", DEFAULT_CONFIG["unauthorized_silent"])),
            "disabled_groups": _normalize_str_list(data.get("disabled_groups")),
            "blocked_users": _normalize_str_list(data.get("blocked_users")),
            "use_yunzai_user_id": bool(data.get("use_yunzai_user_id", DEFAULT_CONFIG["use_yunzai_user_id"])),
            "send_unsupported_as_text": bool(data.get("send_unsupported_as_text", DEFAULT_CONFIG["send_unsupported_as_text"])),
            "private_json_file_to_base64": bool(
                data.get("private_json_file_to_base64", DEFAULT_CONFIG["private_json_file_to_base64"])
            ),
            "private_json_file_max_size_kb": _to_int(
                data.get("private_json_file_max_size_kb"),
                DEFAULT_CONFIG["private_json_file_max_size_kb"],
                1,
                1024 * 1024,
            ),
        }
    )
    return config


def _normalize_str_list(value: Any) -> List[str]:
    result = []
    if isinstance(value, str):
        value = value.replace("\r", "\n").replace(",", "\n").split("\n")
    for item in value or []:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _to_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _build_config_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    safe_config = dict(config)

    return {
        "success": True,
        "platform_id": PLATFORM_ID,
        "config": safe_config,
        "framework_bots": list(_get_framework_bot_map().values()),
        "connected": _is_connected(),
        "connected_bots": [bot_id for bot_id, client in _clients.items() if _client_connected(client)],
        "urls": {bot_id: client.url for bot_id, client in _clients.items()},
        "uptime": int(time.time() - _started_at) if _started_at else 0,
    }


def _is_connected() -> bool:
    return any(_client_connected(client) for client in _clients.values())


def _client_connected(client: GsCoreClient) -> bool:
    return bool(client and client.ws is not None and not getattr(client.ws, "closed", True))


def _client_running(client: GsCoreClient) -> bool:
    runner = getattr(client, "_runner", None)
    return bool(client and getattr(client, "_running", False) and runner is not None and not runner.done())


def _is_auto_reconnecting() -> bool:
    return any(_client_running(client) and not _client_connected(client) for client in _clients.values())


async def _restart_clients() -> Dict[str, Any]:
    config = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG)
    ctx.save_config(config, filename="config.yaml")
    await _apply_config(config)
    return config


async def _manual_restart_clients(max_attempts: int = 3) -> Optional[bool]:
    global _manual_reconnecting
    if _manual_reconnecting or _is_auto_reconnecting():
        return None

    _manual_reconnecting = True
    try:
        config = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG)
        ctx.save_config(config, filename="config.yaml")

        manual_config = dict(config)
        manual_config["max_reconnect_attempts"] = max_attempts
        await _apply_config(manual_config)

        if not _clients:
            return False
        if _is_connected():
            return True

        reconnect_interval = max(1, int(config.get("reconnect_interval") or DEFAULT_CONFIG["reconnect_interval"]))
        deadline = time.time() + max_attempts * (reconnect_interval + 65)
        while time.time() < deadline:
            if _is_connected():
                return True
            if not any(_client_running(client) for client in _clients.values()):
                return False
            await asyncio.sleep(1)

        return _is_connected()
    finally:
        _manual_reconnecting = False


def _get_framework_bot_map() -> Dict[str, Dict[str, Any]]:
    bot_map: Dict[str, Dict[str, Any]] = {}

    try:
        bot_configs = global_config.get_bot_configs()
    except Exception as exc:
        log.warning("读取框架 bot 配置失败: %s", exc)
        bot_configs = []

    if not bot_configs:
        bot_configs = _read_bot_configs_from_file()

    running_bots = _get_running_bot_map()

    for index, bot in enumerate(bot_configs or []):
        if not isinstance(bot, dict):
            continue

        appid = str(bot.get("appid") or "").strip()
        robot_qq = str(bot.get("robot_qq") or "").strip()
        if not appid or not robot_qq:
            continue
        running = running_bots.get(appid, {})
        bot_self_openid = str(running.get("bot_self_openid") or "").strip()
        bot_id = _build_route_bot_id(robot_qq)
        key = appid
        avatar_url = _build_qq_avatar(robot_qq)

        bot_map[key] = {
            "appid": appid,
            "name": str(running.get("name") or bot.get("name") or appid),
            "robot_qq": robot_qq,
            "avatar_url": avatar_url,
            "bot_id": bot_id,
            "bot_self_openid": bot_self_openid,
            "route_bot_id": _build_route_bot_id(robot_qq),
            "platform_id": PLATFORM_ID,
            "framework_enabled": bool(bot.get("enabled", True)),
        }

    return bot_map


def _get_running_bot_map() -> Dict[str, Dict[str, str]]:
    try:
        from core.bot.manager import _bot_manager_ref

        running = {}
        for appid, inst in getattr(_bot_manager_ref, "_bots", {}).items():
            appid = str(appid)
            robot_qq = str(getattr(inst, "robot_qq", "") or "")
            running[appid] = {
                "name": str(getattr(inst, "name", "") or appid),
                "robot_qq": robot_qq,
                "bot_self_openid": str(getattr(inst, "bot_id", "") or ""),
                "avatar_url": _build_qq_avatar(robot_qq),
            }
        return running
    except Exception:
        return {}


def _read_bot_configs_from_file() -> List[Dict[str, Any]]:
    try:
        config_path = os.path.abspath(os.path.join(ctx.plugin_dir, "..", "..", "config", "bot.yaml"))
        if not os.path.isfile(config_path):
            return []
        with open(config_path, encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
        bots = data.get("bots") or []
        return [bot for bot in bots if isinstance(bot, dict)]
    except Exception as exc:
        log.warning("从 config/bot.yaml 读取框架 bot 配置失败: %s", exc)
        return []


def _build_route_bot_id(robot_qq: str) -> str:
    robot_qq = str(robot_qq or "").strip()
    return f"{ADAPTER_ROUTE_PREFIX}-{robot_qq}" if robot_qq else ""


def _build_qq_avatar(robot_qq: str) -> str:
    robot_qq = str(robot_qq or "").strip()
    return f"https://q1.qlogo.cn/g?b=qq&nk={robot_qq}&s=100" if robot_qq else ""


def _get_event_bot_info(event, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    bot_map = _get_framework_bot_map()
    appid = str(getattr(event, "appid", "") or "").strip()
    enabled_bot_ids = set(config.get("enabled_bots") or [])

    if appid and appid in bot_map:
        bot = bot_map[appid]
        return bot if bot.get("bot_id") in enabled_bot_ids else None
    return None


@interceptor(priority=10000)
async def gscore_adapter_command(event):
    config = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG)
    cmd = _parse_adapter_command(event, config)
    if not cmd:
        return False

    if not _is_owner_event(event):
        if not config.get("unauthorized_silent", False):
            await event.reply("❌ 没有权限，仅主人可操作")
        return True

    if cmd == "help":
        await event.reply(_build_help_text())
        return True
    if cmd == "status":
        await event.reply(_build_status_text(config))
        return True
    if cmd == "version":
        await event.reply(f"{__plugin_meta__['name']} v{__plugin_meta__['version']}")
        return True
    if cmd == "重连":
        result = await _manual_restart_clients(max_attempts=3)
        if result is None:
            await event.reply("🔄 正在重连中，请稍后查看状态。")
        elif result:
            await event.reply("✅ 当前 Bot 已连接。")
        else:
            await event.reply("❌ 连接失败，请检查配置。")
        return True

    group_id = _event_group_id(event)
    if cmd in {"群禁用", "群启用"}:
        if not group_id:
            await event.reply("❌ 请在群聊中使用此命令")
            return True
        disabled_groups = list(config.get("disabled_groups") or [])
        if cmd == "群禁用":
            if group_id not in disabled_groups:
                disabled_groups.append(group_id)
            config["disabled_groups"] = disabled_groups
            _save_runtime_config(config)
            await event.reply("🚫 本群早柚核心适配已关闭")
        else:
            if group_id in disabled_groups:
                disabled_groups.remove(group_id)
            config["disabled_groups"] = disabled_groups
            _save_runtime_config(config)
            await event.reply("✅ 本群早柚核心适配已启用")
        return True

    target_user = _extract_mentioned_user_id(event)
    if not target_user:
        await event.reply(f"❌ 请 @要拉黑的用户")
        return True
    blocked_users = list(config.get("blocked_users") or [])
    if cmd == "拉黑":
        if target_user in _owner_ids_for_event(event):
            await event.reply("❌ 你不能拉黑你自己！")
            return True
        if target_user not in blocked_users:
            blocked_users.append(target_user)
        config["blocked_users"] = blocked_users
        _save_runtime_config(config)
        await event.reply(f"✅ 已拉黑用户 {_format_mention(target_user)}")
    else:
        if target_user not in blocked_users:
            await event.reply(f"⚠️ 用户 {_format_mention(target_user)} 不在黑名单中")
            return True
        blocked_users.remove(target_user)
        config["blocked_users"] = blocked_users
        _save_runtime_config(config)
        await event.reply(f"✅ 已取消拉黑用户 {_format_mention(target_user)}")
    return True


def _parse_adapter_command(event, config: Dict[str, Any]) -> str:
    content = str(getattr(event, "content", "") or "").strip()
    prefix = str(config.get("command_prefix") or DEFAULT_CONFIG["command_prefix"]).strip()
    if not prefix or not content.startswith(prefix):
        return ""
    rest = content[len(prefix) :].strip()
    cmd = rest.split(None, 1)[0] if rest else ""
    return cmd if cmd in {"help", "status", "version", "重连", "群禁用", "群启用", "拉黑", "取消拉黑"} else ""


def _save_runtime_config(config: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_config(config)
    ctx.save_config(normalized, filename="config.yaml")
    return normalized


def _build_help_text() -> str:
    prefix = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG).get("command_prefix") or DEFAULT_CONFIG["command_prefix"]
    return "\n".join(
        [
            "[= 常用命令 =]",
            f"> `{prefix}help` - 显示帮助信息",
            f"> `{prefix}status` - 查看连接器状态",
            f"> `{prefix}version` - 查看插件版本",
            f"> `{prefix}重连` - 立即重连 GScore 服务",
            f"\n",
            f"[= 管理命令 =]",
            f"> `{prefix}群禁用` - 关闭本群早柚核心",
            f"> `{prefix}群启用` - 开启本群早柚核心",
            f"> `{prefix}拉黑 @用户` - 拉黑用户（不转发其消息）",
            f"> `{prefix}取消拉黑 @用户` - 取消拉黑用户",
        ]
    )


def _build_status_text(config: Dict[str, Any]) -> str:
    connected_bots = [bot_id for bot_id, client in _clients.items() if _client_connected(client)]
    return "\n".join(
        [
            "[= 适配器状态 =]",
            f"连接状态：{'已连接' if connected_bots else '未连接'}",
            f"已启用 bot：{len(config.get('enabled_bots') or [])}",
            f"已连接 bot：{len(connected_bots)}",
            f"群禁用数量：{len(config.get('disabled_groups') or [])}",
            f"拉黑用户数量：{len(config.get('blocked_users') or [])}",
            f"运行时长：{int(time.time() - _started_at) if _started_at else 0} 秒",
        ]
    )


def _owner_ids_for_event(event) -> List[str]:
    try:
        bot_cfg = global_config.get_bot_config(getattr(event, "appid", ""))
    except Exception:
        bot_cfg = None
    return [str(item) for item in (bot_cfg or {}).get("owner_ids", [])]


def _is_owner_event(event) -> bool:
    return _event_user_id(event) in _owner_ids_for_event(event)


def _event_user_id(event) -> str:
    return str(
        getattr(event, "member_openid", "")
        or getattr(event, "raw_user_id", "")
        or getattr(event, "user_id", "")
        or ""
    ).strip()


def _event_group_id(event) -> str:
    return str(
        getattr(event, "group_openid", "")
        or getattr(event, "group_id", "")
        or getattr(event, "channel_id", "")
        or ""
    ).strip()


def _extract_mentioned_user_id(event) -> str:
    for mention in getattr(event, "mentions", None) or []:
        mention_id = mention.get("id") or mention.get("user_id") or mention.get("openid") or mention.get("member_openid")
        if mention_id:
            return str(mention_id).strip()
    return ""


def _should_forward_event(event, config: Dict[str, Any]) -> bool:
    if _is_owner_event(event):
        return True
    user_id = _event_user_id(event)
    if user_id and user_id in set(config.get("blocked_users") or []):
        return False
    group_id = _event_group_id(event)
    if group_id and group_id in set(config.get("disabled_groups") or []):
        return False
    return True


def _cache_event(msg_id: str, event: Any) -> None:
    msg_id = str(msg_id or "").strip()
    if not msg_id:
        return
    now = time.time()
    _last_events[msg_id] = (now, event)
    _last_events.move_to_end(msg_id)
    _prune_event_cache(now)


def _get_cached_event(msg_id: Any) -> Optional[Any]:
    msg_id = str(msg_id or "").strip()
    if not msg_id:
        return None
    cached = _last_events.get(msg_id)
    if cached is None:
        return None
    cached_at, event = cached
    if time.time() - cached_at > EVENT_CACHE_TTL:
        _last_events.pop(msg_id, None)
        return None
    _last_events.move_to_end(msg_id)
    return event


def _prune_event_cache(now: Optional[float] = None) -> None:
    now = time.time() if now is None else now
    expired_before = now - EVENT_CACHE_TTL
    while _last_events:
        _, (cached_at, _) = next(iter(_last_events.items()))
        if cached_at >= expired_before:
            break
        _last_events.popitem(last=False)


@handler(
    r"^.*$",
    name="早柚适配器消息上报",
    desc="将 ElainaBot 消息转发给 GsCore",
    priority=-10000,
    event_types=[
        "GROUP_AT_MESSAGE_CREATE",
        "GROUP_MESSAGE_CREATE",
        "C2C_MESSAGE_CREATE",
        "DIRECT_MESSAGE_CREATE",
        "AT_MESSAGE_CREATE",
        "MESSAGE_CREATE",
        "INTERACTION_CREATE",
        "GROUP_MEMBER_ADD",
        "GROUP_MEMBER_REMOVE",
    ],
    ignore_at_check=True,
)
async def report_to_gscore(event, match):
    if not _clients:
        return

    try:
        config = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG)
        bot_info = _get_event_bot_info(event, config)
        if bot_info is None:
            return
        if not _should_forward_event(event, config):
            return

        msg = await _event_to_receive(event)
        if msg is None:
            return

        client = _clients.get(str(bot_info.get("route_bot_id") or msg.bot_id))
        if client is None:
            return

        _cache_event(msg.msg_id, event)
        await client.input(msg)
    except Exception as exc:
        report_error(
            PLUGIN,
            "早柚适配器",
            exc,
            context={"event_type": getattr(event, "event_type", ""), "message_id": getattr(event, "message_id", "")},
        )


async def _event_to_receive(event) -> Optional[MessageReceive]:
    config = _normalize_config(ctx.read_config(filename="config.yaml") or DEFAULT_CONFIG)
    bot_info = _get_event_bot_info(event, config)
    if not bot_info:
        return None

    content = await _event_to_content(event, config, bot_info)
    self_id = str(bot_info.get("robot_qq") or event.appid or "")
    meta = _event_to_meta(event, config, self_id)
    if meta:
        content = [meta]
    if not content:
        return None

    user_type = _get_user_type(event)
    group_id = str(event.group_id or event.channel_id or "") or None
    if user_type == "direct":
        group_id = None

    return MessageReceive(
        bot_id=PLATFORM_ID,
        bot_self_id=self_id,
        msg_id=str(event.message_id or event.event_id or ""),
        user_type=user_type,
        group_id=group_id,
        user_id=_format_outgoing_user_id(getattr(event, "user_id", ""), config, self_id),
        sender=_get_sender(event),
        user_pm=_get_user_pm(event),
        content=content,
    )


async def _event_to_content(event, config: Dict[str, Any], bot_info: Dict[str, Any]) -> List[Message]:
    content: List[Message] = []

    for mention in getattr(event, "mentions", None) or []:
        mention_id = mention.get("id")
        if mention_id:
            content.append(Message("at", str(mention_id)))

    reply_id = str(getattr(event, "message_reference_id", "") or "")
    if reply_id:
        content.append(Message("reply", reply_id))

    text = event.content if event.content is not None else event.raw_content
    if text:
        content.append(Message("text", str(text)))

    image_url = getattr(event, "image_url", "")
    if image_url:
        content.append(Message("image", str(image_url)))

    for attachment in getattr(event, "attachments", None) or []:
        converted = await _attachment_to_message(event, attachment, config)
        if converted:
            content.append(converted)

    return content


def _event_to_meta(event, config: Dict[str, Any], self_id: str) -> Optional[Message]:
    event_type = str(getattr(event, "event_type", "") or "")
    user_id = _format_outgoing_user_id(getattr(event, "user_id", ""), config, self_id)
    group_id = str(getattr(event, "group_id", "") or getattr(event, "channel_id", "") or "")

    if event_type == "GROUP_MEMBER_ADD":
        return Message("meta-user_join_group", {"user_id": user_id, "group_id": group_id})
    if event_type == "GROUP_MEMBER_REMOVE":
        return Message("meta-user_exit_group", {"user_id": user_id, "group_id": group_id})
    return None


def _format_outgoing_user_id(user_id: Any, config: Dict[str, Any], self_id: str) -> str:
    user_id = str(user_id or "").strip()
    if not user_id or not config.get("use_yunzai_user_id", False):
        return user_id

    bot_id = str(self_id or "").strip()
    if not bot_id or user_id.startswith(f"{bot_id}:"):
        return user_id
    return f"{bot_id}:{user_id}"


async def _attachment_to_message(event, attachment: Dict[str, Any], config: Dict[str, Any]) -> Optional[Message]:
    url = attachment.get("url")
    file_name = attachment.get("filename") or "file"
    content_type = str(attachment.get("content_type") or "")

    if not url:
        return None
    if "image" in content_type:
        return Message("image", str(url))
    if "audio" in content_type or "voice" in content_type:
        return Message("record", str(url))
    if _should_convert_private_json_file(event, attachment, config):
        limit_kb = int(config.get("private_json_file_max_size_kb") or DEFAULT_CONFIG["private_json_file_max_size_kb"])
        limit_bytes = limit_kb * 1024
        declared_size = _attachment_size(attachment)
        if declared_size is not None and declared_size > limit_bytes:
            await _reply_file_too_large(event, file_name, declared_size, limit_kb)
            return None

        payload = await _download_file_as_base64(str(url), limit_bytes)
        if payload is None:
            await _reply_file_too_large(event, file_name, None, limit_kb)
            return None
        log.info("私聊 JSON 文件准备上报: file_name=%s base64_size=%s", file_name, len(payload))
        return Message("file", f"{file_name}|{payload}")
    return Message("file", f"{file_name}|{url}")


def _should_convert_private_json_file(event, attachment: Dict[str, Any], config: Dict[str, Any]) -> bool:
    if not config.get("private_json_file_to_base64", False):
        return False
    if _get_user_type(event) != "direct":
        return False
    file_name = str(attachment.get("filename") or "").lower()
    content_type = str(attachment.get("content_type") or "").lower()
    return file_name.endswith(".json") or "json" in content_type


def _attachment_size(attachment: Dict[str, Any]) -> Optional[int]:
    value = attachment.get("size")
    if value in (None, ""):
        return None
    try:
        size = int(value)
    except (TypeError, ValueError):
        return None
    return size if size >= 0 else None


async def _download_file_as_base64(url: str, limit_bytes: int) -> Optional[str]:
    if not url.startswith(("http://", "https://")):
        return None
    try:
        async with ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content_length = resp.headers.get("Content-Length")
                if content_length:
                    try:
                        if int(content_length) > limit_bytes:
                            return None
                    except ValueError:
                        pass

                chunks: List[bytes] = []
                downloaded = 0
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    downloaded += len(chunk)
                    if downloaded > limit_bytes:
                        return None
                    chunks.append(chunk)
    except (ClientError, asyncio.TimeoutError, ValueError) as exc:
        log.warning("下载私聊 JSON 文件失败: url=%s error=%s", url, exc)
        return None

    raw = b"".join(chunks)
    payload = base64.b64encode(raw).decode("ascii")
    log.info("私聊 JSON 文件下载完成: raw_size=%s base64_size=%s", len(raw), len(payload))
    return payload


async def _reply_file_too_large(event, file_name: str, size: Optional[int], limit_kb: int) -> None:
    size_text = f"，当前约 {_format_file_size(size)}" if size is not None else ""
    try:
        await event.reply(f"❌ 文件过大{size_text}，最大支持{limit_kb}KB。")
    except Exception as exc:
        log.warning("发送文件过大提示失败: %s", exc)


def _format_file_size(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / 1024 / 1024:.2f}MB"
    return f"{size / 1024:.2f}KB"


def _get_user_type(event) -> str:
    if getattr(event, "is_direct", False):
        return "direct"
    return "group"


def _get_sender(event) -> Dict[str, Any]:
    appid = str(getattr(event, "appid", "") or "").strip()
    user_id = str(getattr(event, "user_id", "") or "").strip()
    group_id = str(getattr(event, "group_id", "") or "").strip()

    nickname = _first_non_empty(
        getattr(event, "username", ""),
        user_id,
        "未知用户",
    )
    user_avatar = _build_qqbot_member_avatar(appid, user_id)

    sender = {
        "nickname": nickname,
        "user_name": nickname,
        "avatar": user_avatar,
        "user_icon": user_avatar,
    }
    if group_id:
        sender.update(
            {
                "group_name": f"群聊-{group_id[:8]}",
                "group_icon": _build_group_placeholder_icon(group_id),
            }
        )
    return sender


def _first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _build_qqbot_member_avatar(appid: str, member_openid: str) -> str:
    appid = str(appid or "").strip()
    member_openid = str(member_openid or "").strip()
    if appid and member_openid:
        return f"https://q.qlogo.cn/qqapp/{appid}/{member_openid}/0"
    return "qqbot://user-avatar/unknown"


def _build_group_placeholder_icon(group_id: str) -> str:
    group_id = str(group_id or "").strip()
    return f"qqbot://group-avatar/{group_id}" if group_id else "qqbot://group-avatar/unknown"


def _get_user_pm(event) -> int:
    role = str(getattr(event, "member_role", "") or "").lower()
    if role in {"owner", "creator"}:
        return 2
    if role in {"admin", "administrator"}:
        return 3
    return 6


async def _send_to_elaina(msg: MessageSend):
    target_event = _get_cached_event(msg.msg_id)
    parts = split_send_content(msg.content)
    recall_ids: List[str] = []

    log.debug(
        "收到 GsCore 下发: bot_id=%s target=%s/%s msg_id=%s event_cached=%s content_types=%s",
        msg.bot_id,
        msg.target_type,
        msg.target_id,
        msg.msg_id,
        bool(target_event),
        [getattr(item, "type", None) for item in msg.content or []],
    )

    if parts["node"]:
        for node_item in parts["node"]:
            node_type = node_item.get("type") if isinstance(node_item, dict) else getattr(node_item, "type", "")
            node_data = node_item.get("data") if isinstance(node_item, dict) else getattr(node_item, "data", None)
            node_msg = MessageSend(
                bot_id=msg.bot_id,
                bot_self_id=msg.bot_self_id,
                msg_id=msg.msg_id,
                target_type=msg.target_type,
                target_id=msg.target_id,
                content=[Message(node_type, node_data)],
            )
            node_id = await _send_to_elaina(node_msg)
            if isinstance(node_id, list):
                recall_ids.extend(node_id)
            elif node_id:
                recall_ids.append(str(node_id))
        return recall_ids

    if target_event is not None:
        response = await _send_with_event(target_event, msg, parts)
    else:
        response = await _send_with_sender(msg, parts)
    message_id = _extract_message_id(response)
    log.debug("GsCore 下发处理完成: msg_id=%s response_id=%s response_type=%s", msg.msg_id, message_id, type(response).__name__)
    return message_id


async def _send_with_event(event, msg: MessageSend, parts: Dict[str, Any]):
    text = _compose_text(parts)
    response = None

    if parts["image"]:
        markdown = await _compose_ordered_markdown(parts, event=event)
        if markdown:
            log.info("通过原始 event 回复图床 Markdown 图片: target=%s/%s", msg.target_type, msg.target_id)
            response = await event.reply(
                markdown,
                msg_type=MSG_TYPE_MARKDOWN,
                buttons=_normalize_buttons(parts["buttons"]),
                skip_suffix=True,
            )
        else:
            log.warning("图片上传图床失败，回退文本回复: target=%s/%s", msg.target_type, msg.target_id)
            response = await event.reply(text or "图片上传失败", buttons=_normalize_buttons(parts["buttons"]))
    elif parts["record"]:
        media_type, payload = _decode_media_payload(parts["record"])
        log.info("通过原始 event 回复语音: media_type=%s target=%s/%s", media_type, msg.target_type, msg.target_id)
        response = await event.reply_voice(payload)
    elif parts["video"]:
        media_type, payload = _decode_media_payload(parts["video"])
        log.info("通过原始 event 回复视频: media_type=%s target=%s/%s", media_type, msg.target_type, msg.target_id)
        response = await event.reply_video(payload)
    elif parts["file"]:
        file_name, payload = _split_file_payload(str(parts["file"]))
        media_type, file_payload = _decode_media_payload(payload)
        log.info("通过原始 event 回复文件: media_type=%s file_name=%s target=%s/%s", media_type, file_name, msg.target_type, msg.target_id)
        response = await event.reply_file(file_payload, text or file_name, file_name=file_name)
    elif text or parts["buttons"]:
        log.info("通过原始 event 回复文本: target=%s/%s", msg.target_type, msg.target_id)
        response = await event.reply(text or " ", buttons=_normalize_buttons(parts["buttons"]))
    return response


async def _send_with_sender(msg: MessageSend, parts: Dict[str, Any]):
    from core.bot.manager import _bot_manager_ref

    sender = _select_sender(msg.bot_self_id, _bot_manager_ref)
    if sender is None:
        log.warning("无法主动发送消息，缺少可用 bot sender: bot_self_id=%s", msg.bot_self_id)
        return None
    text = _compose_text(parts)

    if msg.target_type == "group":
        if parts["image"]:
            markdown = await _compose_ordered_markdown(parts, sender=sender)
            if markdown:
                log.info("通过 sender 主动发送群图床 Markdown 图片: target_id=%s", msg.target_id)
                return await sender.send_to_group(
                    msg.target_id,
                    markdown,
                    buttons=_normalize_buttons(parts["buttons"]),
                    msg_type=MSG_TYPE_MARKDOWN,
                    skip_suffix=True,
                )
            log.warning("群图片上传图床失败，回退文本发送: target_id=%s", msg.target_id)
        log.info("通过 sender 主动发送群文本: target_id=%s", msg.target_id)
        return await sender.send_to_group(msg.target_id, text or " ")
    if msg.target_type == "direct":
        if parts["image"]:
            markdown = await _compose_ordered_markdown(parts, sender=sender)
            if markdown:
                log.info("通过 sender 主动发送私聊图床 Markdown 图片: target_id=%s", msg.target_id)
                return await sender.send_to_user(
                    msg.target_id,
                    markdown,
                    buttons=_normalize_buttons(parts["buttons"]),
                    msg_type=MSG_TYPE_MARKDOWN,
                    skip_suffix=True,
                )
            log.warning("私聊图片上传图床失败，回退文本发送: target_id=%s", msg.target_id)
        log.info("通过 sender 主动发送私聊文本: target_id=%s", msg.target_id)
        return await sender.send_to_user(msg.target_id, text or " ")
    if msg.target_type in {"channel", "sub_channel"}:
        return await sender.send_to_channel(msg.target_id, text or " ")
    return None


def _select_sender(bot_self_id: str, bot_manager) -> Optional[Any]:
    bots = list(getattr(bot_manager, "_bots", {}).values())
    if not bots:
        return None

    expected = str(bot_self_id or "").strip()
    if expected:
        for inst in bots:
            if expected == str(getattr(inst, "robot_qq", "") or "").strip():
                return getattr(inst, "sender", None)

    return None


def _compose_text(parts: Dict[str, Any], *, include_mentions: bool = True) -> str:
    at_prefix = _compose_mentions(parts) if include_mentions and parts["at_list"] else ""
    text = parts["markdown"] or parts["text"]
    if parts["template_markdown"]:
        text = str(parts["template_markdown"])
    if parts["template_buttons"]:
        text = f"{text}\n[按钮模板: {parts['template_buttons']}]".strip()
    if at_prefix and text:
        return f"{at_prefix}\n{text}".strip()
    return f"{at_prefix}{text}".strip()


def _compose_mentions(parts: Dict[str, Any]) -> str:
    return " ".join(_format_mention(user_id) for user_id in parts["at_list"] if str(user_id or "").strip())


async def _compose_ordered_markdown(parts: Dict[str, Any], *, event=None, sender=None) -> Optional[str]:
    segments: List[str] = []

    if parts["at_list"]:
        mentions = _compose_mentions(parts)
        if mentions:
            segments.append(mentions)

    ordered = parts.get("ordered") or []
    for item_type, data in ordered:
        if item_type == "text":
            text = str(data or "").strip()
            if text:
                segments.append(text)
        elif item_type == "image":
            image_markdown = await _compose_image_markdown(data, event=event, sender=sender)
            if not image_markdown:
                return None
            segments.append(image_markdown)

    if not segments:
        text = _compose_text(parts)
        if parts["image"]:
            image_markdown = await _compose_image_markdown(parts["image"], event=event, sender=sender)
            if not image_markdown:
                return None
            segments.extend([item for item in (text, image_markdown) if item])
        elif text:
            segments.append(text)

    if parts["template_buttons"]:
        segments.append(f"[按钮模板: {parts['template_buttons']}]")

    return "\n".join(segment for segment in segments if segment).strip()


def _format_mention(user_id: Any) -> str:
    user_id = str(user_id or "").strip()
    if not user_id:
        return ""
    if user_id in {"all", "全体成员"}:
        return "<@all>"
    if user_id.startswith("<@") and user_id.endswith(">"):
        return user_id
    if ":" in user_id:
        bot_id, raw_user_id = user_id.split(":", 1)
        if bot_id.isdigit() and raw_user_id.strip():
            user_id = raw_user_id.strip()
    return f"<@{user_id}>"


async def _compose_image_markdown(image_data: Any, text: str = "", *, event=None, sender=None) -> Optional[str]:
    media_type, payload = _decode_media_payload(image_data)
    url = payload if media_type == "url" and str(payload).startswith(("http://", "https://")) else None
    size_data = payload if isinstance(payload, bytes) else None

    if not url:
        if not isinstance(payload, bytes):
            log.warning("无法上传非 bytes 图片到图床: media_type=%s", media_type)
            return None
        url = await _upload_image_to_hosting(payload, event=event, sender=sender)

    if not url:
        return None

    width, height = _image_size(size_data)
    alt = f"图片 #{width}px #{height}px" if width and height else "图片"
    image_md = f"![{alt}]({url})"
    return f"{text}\n{image_md}".strip() if text else image_md


async def _upload_image_to_hosting(image_bytes: bytes, *, event=None, sender=None) -> Optional[str]:
    hosting = _get_hosting()
    if not hosting:
        log.warning("图床模块未启用，无法发送 Markdown 图片")
        return None

    token_manager = _get_token_manager(event=event, sender=sender)
    status = hosting.status() if hasattr(hosting, "status") else {}
    uploaders = (
        ("cos", lambda: hosting.upload_cos_url(image_bytes, "gscore.png")),
        ("bilibili", lambda: hosting.upload_bilibili(image_bytes)),
        ("qq_channel", lambda: hosting.upload_qq(image_bytes, token_manager)),
        ("chatglm", lambda: hosting.upload_chatglm(image_bytes)),
        ("ukaka", lambda: hosting.upload_ukaka(image_bytes)),
        ("xingye", lambda: hosting.upload_xingye(image_bytes)),
        ("nature", lambda: hosting.upload_nature(image_bytes)),
    )
    for name, fn in uploaders:
        if status and not status.get(name):
            continue
        try:
            result = await fn()
        except Exception as exc:
            log.warning("图床上传失败: provider=%s error=%s", name, exc)
            continue
        if isinstance(result, str) and result.startswith(("http://", "https://")):
            log.info("图床上传成功: provider=%s", name)
            return result
        if result:
            log.warning("图床返回无效: provider=%s result=%s", name, result)
    return None


def _get_hosting():
    from core.application import get_app

    app = get_app()
    module_manager = app.module_manager if app else None
    return module_manager.get("image_hosting") if module_manager else None


def _get_token_manager(*, event=None, sender=None):
    if sender is not None:
        token_manager = getattr(sender, "_token_mgr", None)
        if token_manager is not None:
            return token_manager

    event_sender = getattr(event, "_sender", None) if event is not None else None
    if event_sender is not None:
        token_manager = getattr(event_sender, "_token_mgr", None)
        if token_manager is not None:
            return token_manager

    appid = str(getattr(event, "appid", "") or "").strip() if event is not None else ""
    if not appid:
        return None
    try:
        from core.application import get_app

        app = get_app()
        bot = app.get_bot(appid) if app else None
        bot_sender = getattr(bot, "sender", None) if bot is not None else None
        return getattr(bot_sender, "_token_mgr", None)
    except Exception:
        return None


def _image_size(data: Optional[bytes]) -> tuple[Optional[int], Optional[int]]:
    if not data:
        return None, None
    try:
        with Image.open(BytesIO(data)) as image:
            return image.size
    except (UnidentifiedImageError, OSError, ValueError):
        return None, None


def _normalize_buttons(buttons):
    if not buttons:
        return None
    rows = buttons
    if isinstance(buttons, dict):
        rows = buttons.get("rows") or buttons.get("buttons") or buttons.get("btns") or []
    if not isinstance(rows, list):
        return None

    normalized_rows = []
    for row in rows:
        if isinstance(row, dict) and ("buttons" in row or "btns" in row):
            row_buttons = row.get("buttons") or row.get("btns") or []
        elif isinstance(row, list):
            row_buttons = row
        else:
            row_buttons = [row]

        normalized_buttons = []
        for button in row_buttons:
            normalized = _normalize_button(button)
            if normalized:
                normalized_buttons.append(normalized)
        if normalized_buttons:
            normalized_rows.append(normalized_buttons)

    return normalized_rows or None


def _normalize_button(button):
    if isinstance(button, str):
        return {"text": button, "data": button}
    if not isinstance(button, dict):
        return None

    text = str(button.get("text") or "").strip()
    data = str(button.get("data") or text).strip()
    if not text and not data:
        return None

    action_type = button.get("action")
    if action_type == -1:
        action_type = button.get("type", 2)
    elif not isinstance(action_type, int):
        action_type = button.get("type", 2)

    normalized = {
        "text": text or data,
        "show": str(button.get("pressed_text") or text or data),
        "style": button.get("style", 1),
        "type": action_type,
        "data": data,
        "tips": str(button.get("unsupport_tips") or "您的客户端暂不支持该功能, 请升级后适配..."),
    }

    permission_type = button.get("permisson")
    if permission_type is not None:
        permission = {"type": permission_type}
        specify_role_ids = button.get("specify_role_ids")
        specify_user_ids = button.get("specify_user_ids")
        if specify_role_ids:
            permission["specify_role_ids"] = specify_role_ids
        if specify_user_ids:
            permission["specify_user_ids"] = specify_user_ids
        normalized["permission"] = permission

    return normalized


def _split_file_payload(data: str):
    if "|" not in data:
        return "file", data
    file_name, payload = data.split("|", 1)
    return file_name or "file", payload


def _decode_media_payload(data: Any):
    if isinstance(data, bytes):
        return "bytes", data
    if isinstance(data, bytearray):
        return "bytes", bytes(data)
    text = str(data)
    if text.startswith(("link://", "base64://", "http://", "https://")):
        return decode_media_payload(text)
    try:
        raw = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        return decode_media_payload(text)

    return "bytes", raw


def _extract_message_id(response):
    if isinstance(response, tuple):
        response = response[1] if len(response) >= 2 else None
    if isinstance(response, dict):
        return extract_message_id(response) or extract_reference_id(response) or None
    if isinstance(response, list):
        ids = [_extract_message_id(item) for item in response]
        ids = [item for item in ids if item]
        return ids if ids else None
    return None


async def _delete_message(msg: MessageSend, data: Dict[str, Any]):
    message_id = data.get("message_id") if isinstance(data, dict) else None
    if not message_id:
        return
    target_event = _get_cached_event(str(message_id)) or _get_cached_event(msg.msg_id)
    if target_event is not None:
        await target_event.recall(message_id=str(message_id))
    else:
        log.warning("无法撤回消息，缺少可用 event: %s", message_id)


async def _ban_user(msg: MessageSend, data: Dict[str, Any]):
    log.warning("ElainaBot 早柚适配器暂未实现禁言控制: %s", data)
