# Independent review

The final tree is `ebc8e6cabb4b01d77935fbed7910121c9810cd03` at
`0e6f79eaaaeadca1d54e46413a3240819be16c25`.

The review checked ledger parity, the pinned PurRDF lock implementation,
RPC error code and message preservation, the inotify descriptor borrow,
and the staged install and relocation. It identified a risk when reloading
the stock profile over a daemon using a different store layout.

The follow-up review confirmed that install preflight refuses incompatible
layouts before changing files and that changing an installed AppArmor
profile requires an inactive or failed unit. The rehearsal covers fresh
staging, relocation, repeat installation and refusal with the original
files intact. The live-host guard was reviewed by inspecting the source.

Verdict: pass.
