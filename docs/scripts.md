# JavaScript Scripts

JavaScript 脚本使用 Sobek 执行。`http-request` 与 `http-response` 只作用于已经进入 MITM HTTP 链路的流量；`cron` 不依赖 MITM。请求脚本在 request rewrite 后执行，响应脚本在 response rewrite 后执行。多个脚本同时匹配时严格按 `scripts` 中的配置顺序依次执行，后一个脚本可见前一个脚本的修改结果。

## 配置

```yaml
scripts:
  request-script:
    enable: true
    debug: false
    type: http-request
    match: '^https://api\.example\.com/'
    path: ./scripts/request.js
    url: https://example.com/request.js
    interval: 600
    options:
      timeout: 5
      binary-body-mode: false
      requires-body: true
      max-body-size: 1024
      indirect-eval: false
    argument: '{"region":"sg"}'

  scheduled-script:
    enable: true
    type: cron
    cron: '0 2 * * *'
    path: ./scripts/cron.js
```

- `enable` 必填。
- `type` 只能是 `http-request`、`http-response` 或 `cron`。
- HTTP 脚本必须提供 `match`；它使用 Go RE2 正则表达式匹配完整请求 URL。
- Cron 脚本必须提供 5 栏表达式，或在最前面增加秒字段的 6 栏表达式。调度使用系统时区，不接受表达式内的时区覆盖。
- `timeout` 默认为 5 秒，包含脚本本身和异步回调的总运行时间。
- `requires-body` 默认为 `false`。未启用时不向脚本提供 body，也不接受 `$done()` 返回的 body 修改。
- `max-body-size` 的单位是 KiB，默认为 `1024`，`-1` 表示无限制。超过限制时跳过当前脚本。
- `binary-body-mode` 为 `true` 时 body 是 `Uint8Array`；否则是 UTF-8 字符串。
- `indirect-eval` 默认为 `false`。启用后，加载时会将语法上的直接 `eval(...)` 调用改为间接全局 eval，用于兼容会触发 Sobek 直接 eval 词法作用域问题的打包脚本。间接 eval 无法读取调用函数的局部变量。
- `argument` 原样作为字符串放入 `$argument`。

Body 会依照 `Content-Encoding` 自动解压和重新压缩 `gzip`、`deflate` 与 `br`。脚本执行完毕后释放解码缓冲；转发所需的最终 body 会保留到 HTTP 层消费完毕。

## 脚本来源与更新

- 只有 `path`：本地脚本，不自动更新，`interval` 被忽略。
- 只有 `url`：加载时下载脚本，并保存到 `scripts/<URL 的 MD5>`。
- 同时提供 `url` 与 `path`：从 URL 下载，保存到指定路径。
- 两者都没有：配置错误。

同一份配置中解析后的保存路径不可重复。远端加载失败时会尝试使用已有且可编译的缓存；定时更新下载或编译失败时继续使用当前版本。

## HTTP 输入与 `$done()`

请求脚本提供：

```javascript
$request.url
$request.method
$request.headers
$request.body // 仅 requires-body=true 且 body 非空
$request.id   // 同一请求的 request/response 阶段保持一致
```

响应脚本同时提供 `$request` 与：

```javascript
$response.status
$response.headers
$response.body // 仅 requires-body=true 且 body 非空
```

普通 Header 对象会覆盖原有 Header；`Content-Length`、`Transfer-Encoding` 与 `Trailer` 由 HTTP 层管理。请求 URL 改为其他主机时不会自动修改 `Host`，脚本需要同时返回修改后的 Header。

```javascript
const headers = $request.headers;
headers.Host = "new.example.com";
$done({url: "https://new.example.com/path", headers});
```

请求脚本可返回 `url`、`headers`、`body`、`response` 与 `abort`；响应脚本可返回 `status`、`headers`、`body` 与 `abort`。`response` 会直接生成本地响应，不连接上游。

```javascript
$done({response: {status: 200, headers: {"Content-Type": "application/json"}, body: "{}"}});
$done({abort: true});
$done({}); // 不修改
$done();   // 不修改
```

## 其他全局 API

- `$argument`：配置中的 argument 字符串。
- `$cronexp`：当前 Cron 表达式，仅 Cron 脚本存在。
- `$script`：包含 `name`、`type`、`startTime` 与 `binaryBodyMode`。
- `$persistentStore.read([key])` 与 `$persistentStore.write(data[, key])`：持久化字符串。省略 key 时，同一路径的脚本共享默认存储区；显式 key 可跨脚本共享。
- `setTimeout(callback, delay[, ...args])` 与 `clearTimeout(id)`：延迟单位为毫秒，计时器受脚本总 `timeout` 限制，脚本结束时自动取消。
- `console.log/info/warn/error`：写入 mihomo 日志。

`$httpClient` 提供 `get`、`post`、`put`、`delete`、`head`、`options` 与 `patch`：

```javascript
$httpClient.get({
  url: "https://example.com/data",
  headers: {Accept: "application/json"},
  timeout: 5,
  insecure: false,
  "auto-cookie": true,
  "auto-redirect": true,
  "binary-mode": false,
  policy: "DIRECT",
}, function (error, response, data) {
  if (error) throw new Error(error);
  $done({headers: {...$request.headers, "X-Status": String(response.status)}});
});
```

请求参数也可直接使用 URL 字符串。Object body 会编码为 JSON，并在缺失时补充 `Content-Type: application/json`；TypedArray body 按二进制发送。回调签名为 `callback(error, response, data)`。

不提供 `$notification`。

## 错误处理

脚本编译错误会使配置加载或远端更新失败。单次执行超时、JavaScript 异常、无效 `$done()` 结果或异步回调异常会写入日志；当前请求或响应保持执行该脚本前的状态，并继续运行后续匹配脚本。显式 `{abort: true}` 和本地 `response` 属于成功结果，会停止当前方向的后续处理。
