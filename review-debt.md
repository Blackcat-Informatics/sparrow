# Issue 160 review debt

- Live acceptance (24 hours after D147) is still open: count stop and slowdown lines in
  `/SPARROW/store/LOG*` and confirm that pending compaction bytes go down.
- No MCP tool for the claim listing. The issue scope names RPC and CLI.
- The scratch measurement used a 9.7 GB store with a one-time debt, not the live 37.6 GB store.
