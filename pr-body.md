Closes #315.

## Ruling

Coordination, binding: "the inode alone identifies a file; no stored-format change."

## Change

The device number (st_dev) is no longer persisted as identity and is never compared with a stored or persisted value. It changes across a reboot on btrfs subvolumes, network, overlay and re-enumerated mounts.

- `CompletedFileIdentity` equality ignores the device (the field stays on the struct and the wire, informational only). This covers claims, finish intents and the ingest status check.
- `ConsumptionGeneration`, `ConsumptionDirectories`, `TailStart`: equality ignores the device.
- Core tail and consumption checks compare the inode only (finish intent on decode and finish, "already active" conflict, claim declaration against journal).
- `ClaimRoot::from_root` binds by inode. The "root device changed" log line and the unused `binding` field are removed.
- `TailRoot` keys held generations by inode and compares the root inode, revision, source and format. #313's `DeviceRenumbered` gap path is removed: a device-only change is a plain match, so a restart resumes the checkpoint.
- Kept, same-boot checks against live descriptors: the root binding and mount guard in `source_file.rs`, `private_namespace`, the rename-cookie and private-intake alias checks in `watches.rs`, `local_endpoint.rs`, and the archive mount check.

Two live same-boot device checks were restored after review:
- Tail admission (`attach`) compares the device and inode of the observed and opened file, so a path re-resolved into a nested btrfs subvolume cannot take another file's checkpoint.
- Handoff preparation (`prepare_source`) refuses a source file that is not on the watch root's filesystem. A saved handoff's device is no longer compared with the live directories, so a retry across a reboot is admitted.

Shipper: #314 already keys the ledger by `inode:size:mtime_ns`, reading old four-field lines with the device ignored. Unchanged here.

## Tests

- `completed_file_identity_ignores_the_device_and_nothing_else` (client)
- `journal_identity_ignores_device_numbers_and_keeps_inodes` (core unit)
- `tail_generation_identity_ignores_a_renumbered_device_and_keeps_the_inode` (core integration)
- `tail_open_ignores_a_renumbered_device_and_refuses_a_changed_inode` (converted from the #313 gap test)
- `consume_root_binds_by_inode_across_a_renumbered_device_and_refuses_a_changed_inode` (converted; now also admits a handoff saved under the old device and retries it as the same journal)
- `shipper_never_reships_a_file_whose_device_number_alone_changed` (existing, rpc)

## Not tested

- The tail admission race (a directory rename between observe and open) needs a nested subvolume and timing control; not driven.
- Refusal of a source on another device at prepare needs a second filesystem inside the root; not driven.

## Known limits (documented)

- Inode reuse after delete and create: a new larger file that reuses an inode is read from the old checkpoint. This was the weakness #313 already had; closing it needs a stored prefix digest, a format change, not proposed.
- Journal request ids still hash the device (changing the hash would change stored ids). A fresh reserve after a renumber gets a new id; pending journals are recovered, not re-reserved.
- Private-intake mode: the root inode alone decides whether a generation belongs to the original root, so an inode collision with the private root is treated as the private root's own and fails loudly.

## Evidence

Main checkout `.stage/evidence-keep/315/`: hooks commit.log (9108f41, EXIT=0), commit2.log (0adc582, EXIT=0), full `make check` on the merged tree 31c654b (check-31c654b.log, EXIT=0). Review `.stage/archive/review-315.md`: BLOCKED at 9108f41 on two device checks (R1 tail admission, R2 handoff retry); both fixed at 0adc582 and read by the primary. The coder's merge of main is 31c654b.
