<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Credentials (specification F, item 6)

Sparrow has one credential: the CHAP shared secret of the public TCP listener.
It never enters the tree. The operator makes it on the host
(`deploy/README.md`, "Install"), as `/etc/sparrow/tcp.secret`, mode 0600,
owned by root. systemd hands it to the daemon as
`/run/credentials/sparrow.service/tcp.secret` (`LoadCredential=`), which is the
only path `deploy/config/server.toml.example` names. The daemon refuses a
secret file that has any group or other permission bit.

## Scans of the deploy tree, run 2026-11-01

Run from the repository root over `deploy/`, `tools/` and `.config/`. Exit 1
from grep and empty output from find mean nothing matched.

```text
$ grep -rniE '(password|passwd|secret|token|api[_-]?key|private[_ ]key)[[:space:]]*[:=][[:space:]]*[^[:space:]]' deploy tools .config | grep -v '^deploy/evidence/credentials.md' | grep -viE 'secret_file|LoadCredential|tcp\.secret'
deploy/install.sh:106:        note "MISSING $secret: the public listener needs it (README.md, step 4); the unit will not start without it"
[exit 0]

$ grep -rn 'BEGIN [A-Z ]*PRIVATE KEY' deploy tools .config | grep -v '^deploy/evidence/credentials.md'
[exit 1]

$ find deploy tools .config \( -name '*.secret' -o -name '.env*' -o -name '*.key' -o -name '*.pem' -o -name '*.p12' -o -name 'id_*' \) -print
[exit 0]

$ grep -rnoE '[A-Za-z0-9+=]{32,}' deploy tools .config | grep -v '^deploy/evidence/'
[exit 1]

$ grep -rn 'secret' deploy/config/server.toml.example deploy/systemd/sparrow.service
deploy/config/server.toml.example:5:# deploy/install.sh when that file is absent. It holds no secret: rpc_tcp_secret_file names the
deploy/config/server.toml.example:6:# credential systemd hands the unit (LoadCredential=tcp.secret:/etc/sparrow/tcp.secret), so the
deploy/config/server.toml.example:7:# secret itself stays in /etc/sparrow/tcp.secret, root 0600. Unknown keys are errors.
deploy/config/server.toml.example:13:# everything else needs a CHAP session with this secret.
deploy/config/server.toml.example:15:rpc_tcp_secret_file = "/run/credentials/sparrow.service/tcp.secret"
deploy/systemd/sparrow.service:34:# The CHAP secret is root's (/etc/sparrow/tcp.secret, mode 0600) and reaches the daemon only as a
deploy/systemd/sparrow.service:35:# credential: /run/credentials/sparrow.service/tcp.secret, mode 0400, owned by sparrow, which is
deploy/systemd/sparrow.service:36:# what rpc_tcp_secret_file names. The daemon refuses a secret with any group or other bit.
deploy/systemd/sparrow.service:37:LoadCredential=tcp.secret:/etc/sparrow/tcp.secret
[exit 0]

```

The first scan excludes the lines that name the secret's path (the
configuration key `rpc_tcp_secret_file`, `LoadCredential=` and the file name
`tcp.secret`); the last command lists them, so the exclusion hides no value.
Its one remaining hit is the shell variable that holds that path in
`install.sh`'s message, not a value. The find matched nothing (find exits 0
either way). The token scan looks for 32 or more characters of the base64 or
hex alphabet without a separator, the shape of a generated secret; nothing in
the tree has it.

## The live secret

The current secret is at `~/.config/sparrow/tcp.secret` under the user unit.
The cutover copies it, it is not regenerated (issue #22's acceptance steps,
step 7):

```bash
sudo install -m 0600 -o root -g root ~/.config/sparrow/tcp.secret /etc/sparrow/tcp.secret
```

`~/.config/sparrow/tcp.secret` stays, mode 0600, as the operator's copy for
the CLI over TCP (README.md, "Secrets").

Not run: it needs root and belongs to the cutover window.
