# PROTOCOL 规则

`PROTOCOL` 根据连接首包中的应用层特征匹配流量，与 `sniffer` 的域名嗅探和 `override-destination` 相互独立。

```yaml
rules:
  - PROTOCOL,BITTORRENT,DIRECT
  - PROTOCOL,STUN,DIRECT
  - PROTOCOL,QUIC,Proxy
  - PROTOCOL,TLS,Proxy
  - PROTOCOL,HTTP,Proxy
  - MATCH,DIRECT
```

当前支持以下名称：

| 名称 | 网络 | 检测内容 |
| --- | --- | --- |
| `HTTP` | TCP | HTTP/1.0、HTTP/1.1 和明文 HTTP/2 客户端前言 |
| `TLS` | TCP | TLS ClientHello；不要求存在 SNI |
| `QUIC` | UDP | QUIC v1、v2 和 draft-29 Initial 包 |
| `STUN` | TCP、UDP | 使用 magic cookie 的 STUN/TURN 控制消息 |
| `BITTORRENT` | TCP、UDP | Peer Wire、明文 HTTP Tracker、UDP Tracker、DHT 和 µTP 初始包 |

这里使用 `TLS` 而不是 `HTTPS`，因为仅凭 ClientHello 无法确认加密后的应用一定是 HTTP；同样，`QUIC` 也不等同于 HTTP/3。

## 自动启用

协议检测没有单独的配置开关。mihomo 会扫描普通规则、逻辑规则、子规则及被引用的 classical `RULE-SET`，只启用其中 `PROTOCOL` 规则需要的检测器。远程规则集更新后，检测器集合也会自动更新。

识别结果会出现在连接 API 的 `metadata.protocol` 字段中。未被任何已加载规则引用的协议不会执行检测，也不会填充该字段。

## 限制

- 路由选择前只能可靠检测客户端先发送数据的协议；服务器先发数据的协议不能用这种方式匹配。
- 加密或混淆后的 BitTorrent Peer Wire 无法保证识别；HTTPS Tracker 只会显示为 `TLS`。
- 明文 HTTP Tracker 带有 `info_hash` 参数时优先归类为 `BITTORRENT`。
- `PROTOCOL,...,DIRECT` 仍然是在流量进入 TUN 后建立直连出站，并不会让原连接绕过 TUN。需要在进入 TUN 前排除流量时，应使用 UID、包名、地址、端口或接口等 TUN 选项。
