# 🦊 ElainaBot早柚核心适配器 (elainabot_gscore_adapter)

这是一个适用于 [ElainaBot](https://github.com/ElainaCore/ElainaBot_v2) 的 [GScore](https://github.com/Genshin-bots/gsuid_core) (早柚核心) 适配器插件。它通过 WebSocket 连接 GScore 服务，将 ElainaBot 收到的 QQ 消息转发给 GScore 处理，并将 GScore 下发的回复发送回对应会话。

## ✨ 主要功能

- **🚀 简单快捷**: 框架商店安装配置插件后即可将 ElainaBot 连接至 GScore 服务。
- **⚙️ WebUI 配置**: 支持在 ElainaBot 扩展页面中选择启用 Bot、修改连接配置。
- **🔄 断线重连**: 自动检测 WebSocket 连接状态，并在断开后按配置间隔重连。
- **📡 事件上报**: 支持将 ElainaBot 消息事件转换为 GScore `MessageReceive` 协议。
- **📡 元事件上报**: 支持向 GScore 上报进群、退群两类标准 meta 事件。
- **↩️ 撤回回执**: 支持 `wait_recall` 场景，发送后回传 ElainaBot 消息 ID。

## 🛠️ 安装说明

1. 将本插件目录放置在 ElainaBot 的 `plugins` 目录下。
2. 启动或重启 ElainaBot，框架会自动安装 `requirements.txt` 中的依赖。
3. 首次启动后插件会生成配置文件 `data/config.yaml`。
4. 在 ElainaBot WebUI 的扩展页面中打开 **GScore 适配器**。
5. 选择需要接入 GScore 的 Bot，并按你的 GScore 配置修改连接参数。
6. 保存配置后插件会自动应用；如未生效，可重载插件或重启 ElainaBot。

> 容器部署时请确保 ElainaBot 容器可以访问 GScore 服务，并持久化 `plugins/elainabot_gscore_adapter/data` 目录，避免更新容器导致配置丢失。

## 📝 配置指南

在 ElainaBot WebUI 的 GScore 适配器页面中，您可以自定义以下选项：

| 配置项 | 说明 | 默认值 |
| :--- | :--- | :--- |
| **启用 Bot** | 需要转发到 GScore 的 ElainaBot bot ID 列表 | `[]` |
| **连接主机** | GScore WebSocket 服务主机 | `127.0.0.1` |
| **连接端口** | GScore WebSocket 服务端口 | `8765` |
| **连接 Token** | GScore `WS_TOKEN`，为空则不携带 Token | `空` |
| **重连间隔** | 断线后尝试重连的时间间隔 (秒) | `5` |
| **云崽用户 ID 格式** | 开启后上报 `user_id` 使用 `BotQQ号:用户ID` 格式 | `false` |

> ⚠️ **注意**: 如果 ElainaBot 运行在 Docker 容器中，请勿将连接主机设置为容器内的 `localhost` 或 `127.0.0.1`，除非 GScore 也运行在同一容器内。请使用宿主机 IP、Docker Network 容器名，或确保两个容器处于同一网络。


## ❓ 常见问题 (FAQ)

### Q1: 无法连接到 GScore (Connection Refused)?
**A**:
1. 请确保 GScore 服务已正常启动，并监听配置中的 `host` / `port`。
2. 如果 GScore 开启了 `WS_TOKEN`，请在插件配置中正确填写 `token`。
3. 如果 ElainaBot 运行在 Docker 容器中，容器内的 `127.0.0.1` 指向的是容器本身，而不是宿主机。请改用宿主机 IP 或 Docker Network 容器名。

### Q2: 为什么 GScore 没有收到消息？
**A**:
1. 请检查 WebUI 中是否已勾选对应的 ElainaBot Bot。
2. 确认该 Bot 在 ElainaBot 中处于启用状态。
3. 查看插件状态是否显示已连接，并确认 GScore 地址、端口、Token 与早柚核心配置一致。

## 📄 License
MIT License
