# SafeXcel EIP-93 Hybrid AES-GCM Acceleration

Mihomo provides a package-private Go AES-GCM overlay for Linux ARM64. Eligible
Go AES-GCM records use Linux AF_ALG with the composite driver
`gcm_base(ctr(aes-eip93),ghash-generic)`: offloading compute-heavy AES-CTR
streaming to the SafeXcel EIP-93 hardware engine while computing GHASH on the
CPU. Neither wire protocols nor the shared Go toolchain are modified.

## Selection and Fallback

The overlay is generated only for Linux ARM64 builds. At runtime:

- **CPU Native AES Bypass**: Excludes CPUs with native ARMv8 Crypto Extensions
  (`cpu.ARM64HasAES`, e.g. Airoha AN7583, MediaTek MT7981 / MT7986, Rockchip RK3588,
  Apple Silicon). These CPUs run Go's native assembly AES-GCM at 2+ Gbps and
  bypass EIP-93 with zero syscall overhead.
- **Dynamic Kernel Probe**: Dynamically probes the Linux kernel `AF_ALG` skcipher
  subsystem for `gcm_base(ctr(aes-eip93),ghash-generic)` via an ephemeral socket bind.
  No hardcoded device-tree SoC whitelists are used, automatically detecting and
  activating on any ARM64 SoC with an active EIP-93 driver (such as Airoha AN7581 / EN7581).
- **Payload Bounds**: Only handles standard 12-byte nonces, 16-byte tags, 4–32 KiB payloads,
  and at most 256 bytes of associated data (AAD). Other requests transparently use
  standard Go software crypto to avoid syscall overhead on small packets.
- **Key Self-Verification**: Verifies each newly derived key against Go generic
  encryption/decryption and an invalid tag before offloading. Bounds operational
  sockets to 128 globally, serializes operations per key, and sets 1-second socket timeouts.
- **Resilient Fallback**: Falls back to Go standard software crypto on missing modules,
  failed verification, closed sockets, or operational errors. FIPS mode excludes
  hardware offload.
- **Runtime Opt-Out**: Set `MIHOMO_EIP93=off` in the process environment to disable
  hardware acceleration.

The first verified activation logs:

```
Mihomo EIP93: SafeXcel EIP-93 AES-GCM hybrid backend enabled (AF_ALG CTR + CPU GHASH)
```

Correlate traffic with `1fb70000.crypto` (or equivalent crypto IRQ) in `/proc/interrupts`.
Firmware requires a working EIP-93 kernel driver (`kmod-crypto-hw-eip93`) with AF_ALG,
GCM, and GHASH enabled. A missing driver safely leaves Mihomo on standard software crypto.

TLS/uTLS and protocols using standard Go AES-GCM can utilize this path. Cipher
negotiation remains unchanged: ChaCha20 connections or records outside the supported
payload range remain on standard software crypto.

## Build and Regression Checks

The generator checks the Go source layout and prepares a private Go overlay.
Files under `tools/eip93` use `.go.in` templates so recursive Go package discovery
does not compile them as an application package importing Go internals. The generated
overlay maps these templates into the private standard-library build via `-overlay`.

To test or build with the overlay:

```sh
python3 tools/eip93/overlay.py --go go --out "$PWD/.eip93-overlay"
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test \
  -overlay="$PWD/.eip93-overlay/overlay.json" -c \
  -o gcm.test crypto/internal/fips140/aes/gcm
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test \
  -overlay="$PWD/.eip93-overlay/overlay.json" -c -o cipher.test crypto/cipher
```

Run `gcm.test -test.run TestEIP93 -test.v` and `cipher.test -test.short`.
Both pass on AN7581, covering AES-128/192/256, payload boundaries, in-place buffers,
invalid tags, concurrency, closed-socket fallback, and nonstandard nonce/tag fallback.
Host AMD64 cipher tests also pass with the overlay present and the hardware path excluded.

## Performance Benchmark

Measured on Nokia XG-040G-MD (Airoha AN7581 at 1.2 GHz, Gigabit LAN).
Single-stream application payload throughput (Shadowsocks AES-256-GCM):

| Router role | Standard Go Crypto | EIP-93 Accelerated | Speedup |
| :--- | :---: | :---: | :---: |
| Client Send | 34.2 Mbps | **146.0 Mbps** | ~4.3x |
| Client Receive | 32.1 Mbps | **100.0 Mbps** | ~3.1x |
| Server Send | 35.4 Mbps | **149.1 Mbps** | ~4.2x |
| Server Receive | 34.5 Mbps | **109.5 Mbps** | ~3.2x |

Shadowsocks 2022 (`2022-blake3-aes-256-gcm`):

| Router role | Standard Go Crypto | EIP-93 Accelerated |
| :--- | :---: | :---: |
| Client (Send / Recv) | 35.9 / 36.2 Mbps | **159.0 / 113.0 Mbps** |
| Server (Send / Recv) | 35.8 / 35.8 Mbps | **154.5 / 120.9 Mbps** |

Accelerated runs show active hardware interrupt growth on the crypto engine, with host
CPU load dramatically reduced compared to pure software AES.
