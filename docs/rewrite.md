# HTTP(S) Rewrite

Rewrite 只作用于已经被 `mitm.hostname`、`mitm.hostname-exclude` 和 `mitm.client-source-address` 选中的流量。HTTPS 会先由 MITM 解密；明文 HTTP 直接进入相同的 HTTP 处理链路。未进入 MITM HTTP 链路的连接不会执行 rewrite。

## 执行顺序

1. `url` 按配置顺序查找首个匹配项。`transparent` 修改请求 URL 与 `Host` 后继续处理；redirect 和 reject 直接生成响应。
2. 使用透明改写后的 URL 查找首个 `mock` 规则。匹配时直接生成静态响应，不连接上游。
3. 所有匹配的 request header 规则按配置顺序执行。
4. 首个匹配的 request body 规则执行；正则模式中的所有 `actions` 按配置顺序执行。
5. 收到上游响应或生成本地响应后，所有匹配的 response header 规则按配置顺序执行。
6. 首个匹配的 response body 规则执行。

所有 `match`、body action 的 `regex` 和 header 的 `replace-regex` 都使用 Go RE2 正则表达式；替换值支持 `$1` 形式的捕获组引用。无效表达式会使配置加载失败。

## URL

```yaml
rewrite:
  url:
    - match: '^https?://cn\.bing\.com(.*)$'
      type: transparent
      value: 'https://duckduckgo.com$1'

    - match: '^https?://example\.com/found(.*)$'
      type: redirect-302
      value: 'https://example.net$1'

    - match: '^https?://example\.com/temporary(.*)$'
      type: redirect-307
      value: 'https://example.net$1'

    - match: '^https?://example\.com/blocked$'
      type: reject
      value: array
```

`transparent` 同时修改上游 scheme、host、port、path、query 和请求 `Host`，客户端不会收到重定向响应。之后的 mock、header、body、连接规则和日志均使用改写后的 URL。

`reject.value` 支持：

- 省略或空字符串：`404`、空 body。
- `200`：`200`、空 body。
- `img`：`200`、1 px 透明 GIF、`Content-Type: image/gif`。
- `dict`：`200`、`{}`、`Content-Type: application/json`。
- `array`：`200`、`[]`、`Content-Type: application/json`。

其他值会使配置加载失败。

## Header

```yaml
rewrite:
  header:
    - match: '^https://api\.example\.com/'
      direction: request
      type: add
      field: X-Processed-By
      value: mihomo

    - match: '^https://api\.example\.com/'
      direction: response
      type: del
      field: X-Legacy

    - match: '^https://api\.example\.com/'
      direction: response
      type: replace
      field: X-Processed-By
      value: sing-box

    - match: '^https://api\.example\.com/'
      direction: response
      type: replace-regex
      field: X-Processed-By
      regex: '^old-(.*)$'
      value: 'mihomo-$1'
```

`direction` 只能是 `request` 或 `response`。`add` 会追加一个值；`del` 删除该字段的全部值；`replace` 和 `replace-regex` 仅修改已经存在的字段，不会创建缺失字段。

## Body

正则模式：

```yaml
rewrite:
  body:
    - match: '^https://api\.example\.com/'
      direction: request
      actions:
        - regex: '123'
          value: xray
        - regex: '153'
          value: mihomo
```

JQ 模式：

```yaml
rewrite:
  body:
    - match: '^https://api\.example\.com/'
      direction: response
      jq-expression: '.processed_by = "mihomo"'
```

每条 body 规则必须在 `actions` 与 `jq-expression` 中二选一。JQ 使用标准 jq filter 语义。JSON 解析失败或 filter 运行失败时保留原 body。

Body rewrite 会按 `Content-Encoding` 自动解压和重新压缩 `gzip`、`deflate`、`br`，也支持多个编码叠加；修改后会重新计算 `Content-Length`。遇到无效透明改写目标、不支持或损坏的编码、无效 JSON 或 jq 运行错误时，会在 HTTP 交易中记录 `outcome=failed`、将交易状态设为 `failed` 并立即中断连接；不会回退到原始 URL 或原始 body。

## Mock

`text` 和 `base64` 只能二选一；两者都省略时返回空 body。`status-code` 默认为 `200`，`Content-Length` 总是由引擎根据实际 body 计算。

Headers 可使用普通 YAML map：

```yaml
rewrite:
  mock:
    - match: '^https?://example\.com/json$'
      text: '{}'
      status-code: 200
      headers:
        Content-Type: application/json
```

也可使用 map 列表；需要重复 Header 时使用这种形式：

```yaml
rewrite:
  mock:
    - match: '^https?://example\.com/binary$'
      base64: 'AAE='
      headers:
        - Content-Type: application/octet-stream
        - X-Value: first
        - X-Value: second
```

透明改写后，MITM 抓包接口的 `request.raw_url` 保存客户端原始 URL，`request.url` 保存当前实际连接使用的改写后 URL。没有发生透明改写时两者相同。

## 交易动作与日志

每个命中的 URL、Header、body 或 mock 规则都会在 `/mitm` 交易中生成有序 action。`redirect-302`、`redirect-307`、`reject` 和 `mock` 等本地响应也会写入 `/logs`；结构化字段、动作结果和前端状态处理方式见 [logs.md](./logs.md)。
