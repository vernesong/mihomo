# MITM 抓包接口

MITM 接口挂载在 external controller 的 `/mitm` 路径下，并沿用 controller 的鉴权方式。配置了 `secret` 时，请发送 `Authorization: Bearer <secret>`。

浏览器 WebSocket 无法设置 `Authorization` Header，可使用 controller 已有的查询参数鉴权：`ws://<controller>/mitm?token=<URL 编码后的 secret>`。

## 配置

```yaml
mitm:
  capture: false
```

- `capture: false`：记录被 MITM 处理的 HTTP 请求及响应元数据和 Headers，不保存正文。
- `capture: true`：同时保存请求及响应的完整 HTTP body。配置加载时会输出 WARNING，因为此模式可能消耗大量内存和 CPU。
- 在线切换只影响切换后创建的请求。通过接口修改的是运行时状态，不会写回配置文件；下一次配置加载会重新采用配置文件中的值。
- 最多保留最近 256 个请求。单个 body 不设大小上限，因此不建议长期开启。

## 获取请求记录

### HTTP 快照

```http
GET /mitm
```

响应示例：

```json
{
  "capture": true,
  "limit": 256,
  "sessions": [
    {
      "id": "6c47d457-56de-4e14-9555-957ec2122149",
      "requestIndex": 42,
      "startedAt": "2026-08-08T15:04:05.123456+08:00",
      "completedAt": "2026-08-08T15:04:05.223456+08:00",
      "source": "192.0.2.10:54321",
      "capture": true,
      "request": {
        "method": "POST",
        "url": "https://example.com/index.html?from=mitm",
        "proto": "HTTP/2.0",
        "headers": {
          "Content-Type": ["application/json"],
          "User-Agent": ["Example/1.0"]
        },
        "body": {
          "size": 17,
          "encoding": "utf8",
          "content": "{\"hello\":\"world\"}",
          "complete": true
        }
      },
      "response": {
        "statusCode": 200,
        "status": "200 OK",
        "proto": "HTTP/2.0",
        "headers": {
          "Content-Type": ["application/json"]
        },
        "body": {
          "size": 11,
          "encoding": "utf8",
          "content": "{\"ok\":true}",
          "complete": true
        }
      }
    }
  ]
}
```

字段说明：

- `capture`（顶层）：当前全局正文捕获开关。
- `limit`：服务端保留的最大请求数，当前为 256。
- `sessions`：按开始时间从旧到新排列。
- `session.id`：对应同一上游 TCP 连接在 `/connections` 中的 Tracker UUID。它不是 MITM 另外生成的 UUID；上游连接尚未建立时暂为空，`REJECT`、本地响应或拨号失败时可能一直为空。
- `session.requestIndex`：mihomo 进程内单调递增的 HTTP 请求序号，用于区分同一连接上的多个请求及关联 WebSocket 更新；它不是连接 ID，也不是 UUID。
- `session.capture`：该请求开始时是否启用了正文捕获。
- `request.url`：完整 URL，包含 scheme、host、path 和 query，不包含 fragment。
- `headers`：值始终是字符串数组，适合直接转换为多值 Header 列表；Go HTTP server 单独保存的请求 `Host` 也会合并到这里。
- `response`、`completedAt`：请求仍在进行时可能不存在。
- `error`：上游请求失败时出现，内容为错误文本。
- `body`：仅当该 session 的 `capture` 为 `true` 时出现；流尚未读完时也可能暂时不存在。
- `body.size`：捕获后的原始字节数。
- `body.encoding`：正文为有效 UTF-8 时是 `utf8`，否则是 `base64`。
- `body.complete`：`true` 表示正文已读到 EOF（或已确认达到 Content-Length）；连接提前关闭时为 `false`。

正文是当前 HTTP transport 交付给 MITM 的字节；接口不会再根据 `Content-Encoding` 做二次解压。状态码 `101 Switching Protocols` 只记录握手 Headers，不捕获升级后的双向数据。

### WebSocket 增量流

对相同路径发起 WebSocket upgrade：

```text
ws://<controller>/mitm
wss://<controller>/mitm
```

连接后首先收到一次 `snapshot`，之后收到增量事件。所有消息都是 JSON text frame。

```json
{"type":"snapshot","capture":false,"limit":256,"sessions":[]}
{"type":"session","capture":true,"session":{"id":"","requestIndex":42,"request":{"method":"GET","url":"https://example.com/","proto":"HTTP/1.1","headers":{}}}}
{"type":"session","capture":true,"session":{"id":"6c47d457-56de-4e14-9555-957ec2122149","requestIndex":42,"request":{"method":"GET","url":"https://example.com/","proto":"HTTP/1.1","headers":{}}}}
{"type":"capture","capture":false}
{"type":"remove","capture":false,"id":"6c47d457-56de-4e14-9555-957ec2122149","requestIndex":42}
{"type":"clear","capture":false}
{"type":"heartbeat","capture":false}
```

事件含义：

- `snapshot`：连接建立时的完整快照。
- `session`：新增或更新一个请求；同一个 `requestIndex` 会随着连接 UUID、响应 Headers、body 和完成时间到达而多次发送，前端应按 `requestIndex` upsert。
- `capture`：正文捕获开关发生变化。
- `remove`：达到 256 条上限时淘汰最旧记录，前端应删除对应 `requestIndex`。
- `clear`：所有记录已清空。
- `heartbeat`：每 30 秒发送一次的保活事件，不修改列表。

WebSocket 断线重连后应以新的 `snapshot` 替换本地状态。为避免慢速前端阻塞代理流量，客户端来不及消费事件时服务端会主动断开该 WebSocket，前端应自动重连。

## 清空请求记录

```http
DELETE /mitm
```

成功返回 `204 No Content`。该操作不修改 `capture` 开关。

## 获取正文捕获状态

```http
GET /mitm/capture
```

```json
{"capture":false}
```

## 在线切换正文捕获

```http
PUT /mitm/capture
Content-Type: application/json

{"capture":true}
```

也支持 `PATCH /mitm/capture`，请求及行为相同。成功返回 `204 No Content`；缺少 `capture` 或 JSON 无效时返回 `400 Bad Request`。在线开启时同样会输出资源开销 WARNING。

## 前端接入建议

1. 建立 `/mitm` WebSocket，以首个 `snapshot` 初始化列表。
2. 按 `session.requestIndex` 更新记录，并处理 `remove` 和 `clear`；`session.id` 用于关联 `/connections` 中的连接详情。
3. `encoding` 为 `base64` 时再解码正文；展示前根据 Content-Type 选择文本、JSON、图片或十六进制视图。
4. 明确提示用户 Headers 和 body 可能包含 Cookie、Authorization、Token、密码等敏感数据，并避免把抓包结果写入持久日志。
