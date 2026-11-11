#!/usr/bin/env python3
"""Generate a Mihomo-only Go overlay; never modify the shared Go toolchain."""
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--go', required=True)
parser.add_argument('--out', required=True)
args = parser.parse_args()
root = Path(subprocess.check_output([args.go, 'env', 'GOROOT'], text=True).strip()).resolve()
source = root / 'src/crypto/internal/fips140/aes/gcm'
out = Path(args.out).resolve()
out.mkdir(parents=True, exist_ok=True)
replacements = {}


def change(name, edits):
    text = (source / name).read_text(encoding='utf-8')
    for old, new in edits:
        if text.count(old) != 1:
            raise SystemExit('EIP93 overlay: unsupported Go source in ' + name + '; review before enabling acceleration')
        text = text.replace(old, new, 1)
    dst = out / name
    dst.write_text(text, encoding='utf-8')
    replacements[str(source / name)] = str(dst)


change('gcm.go', [('\tgcmPlatformData\n', '\tgcmPlatformData\n\teip93 *eip93State\n')])
change('gcm_asm.go', [
    ('func initGCM(g *GCM) {\n\tif !supportsAESGCM {\n\t\treturn',
     'func initGCM(g *GCM) {\n\tif !supportsAESGCM {\n\t\tg.eip93 = newEIP93State(g)\n\t\treturn'),
    ('\t\tsealGeneric(out, g, nonce, plaintext, data)',
     '\t\tif g.eip93 != nil {\n\t\t\tif handled, _ := g.eip93.crypt(g, out, nonce, plaintext, data, false); handled { return }\n\t\t}\n\t\tsealGeneric(out, g, nonce, plaintext, data)'),
    ('\t\treturn openGeneric(out, g, nonce, ciphertext, data)',
     '\t\tif g.eip93 != nil {\n\t\t\tif handled, valid := g.eip93.crypt(g, out, nonce, ciphertext, data, true); handled {\n\t\t\t\tif !valid { return errOpen }; return nil\n\t\t\t}\n\t\t}\n\t\treturn openGeneric(out, g, nonce, ciphertext, data)')
])

script_dir = Path(__file__).resolve().parent
for name in ['eip93_linux_arm64.go', 'eip93_stub.go', 'eip93_linux_arm64_test.go']:
    dst = out / name
    dst.write_text((script_dir / (name + '.in')).read_text(encoding='utf-8'), encoding='utf-8')
    replacements[str(source / name)] = str(dst)

(out / 'overlay.json').write_text(json.dumps({'Replace': replacements}, indent=2) + '\n', encoding='utf-8')
