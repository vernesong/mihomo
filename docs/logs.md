# 日志与 HTTP 交易事件

mihomo 将传输连接、诊断日志和 HTTP 交易记录分成三个层次：

- `/connections` 只表示当前仍然存活的 TCP/UDP 传输及流量统计；连接关闭后会从列表移除。
- `/logs` 是实时诊断流，适合终端和日志面板；它不保存历史，也不是交易状态的事实来源。
- `/mitm` 保存最近的 HTTP 交易快照，并通过 WebSocket 发送增量更新。HTTPS MITM、被选中的明文 HTTP、rewrite、JavaScript 和自定义 Handler 共用这条交易管线。

WebUI 应从 `/mitm` 展示“活跃”“已修改”“失败”等交易状态，再使用 `transactionId` 将它与 `/logs` 中的相关动作关联起来。不要根据日志文本反向推导交易状态。

## `/logs` 实时接口

接口沿用 external controller 的鉴权。配置了 `secret` 时，HTTP 请求使用 `Authorization: Bearer <secret>`；浏览器 WebSocket 可使用 `?token=<URL 编码后的 secret>`。

```http
GET /logs?level=info
```

`level` 支持 controller 已有的日志级别；省略时为 `info`。普通 HTTP 响应是持续刷新的 JSON Lines 流，对同一路径发起 WebSocket upgrade 时，每条日志对应一个 JSON text frame。

默认格式保持兼容：

```json
{"type":"info","payload":"[HTTP] 0f... request rewrite redirect responded redirect-302 status=302 target=https://example.net/"}
```

日志接口不重放连接前产生的事件。消费者断线后应重新连接；如果需要当前和最近交易的完整状态，应重新获取 `/mitm` 快照。

## 结构化格式

添加 `format=structured`：

```http
GET /logs?level=info&format=structured
```

响应示例：

```json
{
  "time": "15:04:05",
  "level": "info",
  "message": "[HTTP] 0f... request rewrite redirect responded redirect-302 status=302 target=https://example.net/",
  "fields": [
    {"key": "transactionId", "value": "0f1d94d5-9061-42cf-89b8-1c844c2a6a67"},
    {"key": "actionIndex", "value": "1"},
    {"key": "phase", "value": "request"},
    {"key": "source", "value": "rewrite"},
    {"key": "kind", "value": "redirect"},
    {"key": "outcome", "value": "responded"},
    {"key": "modified", "value": "true"},
    {"key": "name", "value": "redirect-302"},
    {"key": "statusCode", "value": "302"},
    {"key": "target", "value": "https://example.net/"}
  ]
}
```

`fields` 保持数组形式以兼容现有 controller API。所有 `value` 都是字符串，客户端应忽略不认识的字段，以便未来扩展。

HTTP 交易动作可能包含以下字段：

| 字段 | 含义 |
| --- | --- |
| `transactionId` | 每个 HTTP 请求唯一的 UUID，也就是 JavaScript `$request.id` |
| `actionIndex` | 动作在该交易内从 1 开始的顺序号 |
| `phase` | `request` 或 `response` |
| `source` | `rewrite`、`script` 或 `handler` |
| `kind` | 动作类别，例如 `url`、`header`、`body`、`redirect`、`reject`、`mock`、`script` |
| `outcome` | `applied`、`responded`、`aborted`、`unchanged`、`skipped` 或 `failed` |
| `modified` | 该动作是否实际改变交易 |
| `name` | rewrite 类型、Header 操作或脚本名称 |
| `rule` | 命中的 URL 正则表达式 |
| `statusCode` | 本地响应或脚本修改后的 HTTP 状态码 |
| `target` | 透明 URL 修改或重定向的目标 URL |
| `fields` | 逗号分隔的处理范围，不包含 Header 或 body 内容 |

`applied`、`responded` 和 `aborted` 动作通常使用 `info`；未产生变化的 `unchanged` 和因 body 限制跳过的 `skipped` 使用 `debug`；rewrite 或脚本执行错误使用 `error`。rewrite、脚本或上游失败还会产生带有 `state=failed`、`stage` 和 `source` 的 `warning` 日志。

## HTTP 交易管线

每个被拦截的 HTTP 请求按以下顺序执行：

1. request rewrite
2. request JavaScript
3. request Handler
4. 本地响应或上游请求
5. response Handler（仅上游响应）
6. response rewrite
7. response JavaScript
8. 完成、失败、中止或取消

每个匹配项会在执行位置生成一个有序 action。后续处理器仍可修改前一阶段生成的本地响应，因此最终状态可能包含多个动作。例如，rewrite mock 生成 `201` 后，response rewrite 仍可能增加 Header，response JavaScript 也可能再次修改 body。

以下内部结果都会写入交易 actions 和实时日志：

- rewrite 透明 URL 修改、request/response Header 和 body 修改；
- `redirect-302`、`redirect-307`、`reject` 和 `mock` 本地响应；
- JavaScript URL、Header、body、status 修改；
- JavaScript `$done({response: ...})` 和 `$done({abort: true})`；
- body 超限跳过、JQ/编码错误、JavaScript 异常和超时；
- 自定义 Handler 返回的新请求、本地响应或响应替换，以及可观察到的原地修改。

日志不会记录 Header 值或 body 内容。`rule`、`target` 和错误文本仍可能包含敏感 URL、查询参数或内部地址，部署方应按敏感数据处理日志。

## `/mitm` 中的交易状态

`GET /mitm` 和 `/mitm` WebSocket 的 session 现在包含显式状态与动作：

```json
{
  "id": "可选的上游连接 ID",
  "transactionId": "0f1d94d5-9061-42cf-89b8-1c844c2a6a67",
  "requestIndex": 42,
  "state": "completed",
  "modified": true,
  "actions": [
    {
      "index": 1,
      "at": "2026-08-12T15:04:05.123456+08:00",
      "phase": "request",
      "source": "rewrite",
      "kind": "mock",
      "outcome": "responded",
      "name": "mock",
      "rule": "^https://example\\.com/data$",
      "modified": true,
      "statusCode": 200
    }
  ]
}
```

`state` 的取值：

- `active`：请求或响应 body 仍在处理，包括长连接和流式响应。
- `completed`：响应已正常消费完毕；HTTP 4xx/5xx 本身不会自动视为代理失败。
- `failed`：rewrite、JavaScript、上游拨号、TLS 或 HTTP round trip 失败。此时同时提供结构化 `failure`，旧版 `error` 字段继续保留。
- `aborted`：JavaScript 明确返回 `{abort: true}`。
- `cancelled`：响应尚未完整消费便被关闭。

`modified` 与 `state` 相互独立。一笔交易可以“活跃且已修改”，也可以“已修改后中止”。只要至少一个 action 的 `modified` 为 `true`，交易的 `modified` 就为 `true`。

rewrite 或 JavaScript 动作失败采用与 Surge 一致的 fail-closed 行为：对应 action 为 `outcome=failed`，交易为 `state=failed`，后续处理停止并中断连接。请求阶段失败不会拨号原始上游；响应阶段失败不会向客户端发送原始响应。

## 标识符与前端更新

- `transactionId` 是 HTTP 交易的稳定标识，并与 JavaScript `$request.id` 一致。redirect、mock、reject 和脚本本地响应即使没有上游连接也始终拥有它。
- `requestIndex` 是进程内单调递增的序号，也是 `/mitm` WebSocket upsert 和 remove 的主键。
- `id` 为兼容现有 API 保留，表示可选的上游 `/connections` Tracker UUID。一条 HTTP/2 或 keep-alive 连接可以关联多个交易。
- action 的 `index` 只在单个交易内有意义，前端应按数组顺序展示处理历程。

WebUI 接入时应先用 `/mitm` WebSocket 的 `snapshot` 替换本地列表，然后按 `requestIndex` 处理 `session` upsert、`remove` 和 `clear`。`/logs` 可作为详情页的实时诊断补充，但不应替代这套状态同步。
