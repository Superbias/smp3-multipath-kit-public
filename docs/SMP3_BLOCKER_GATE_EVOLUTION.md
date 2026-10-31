# Gate evolution proposal — POLICY_APPROVAL_REQUIRED

> Historical gate inventory from the earlier reviewed8 candidate. Its 31/51/current counts are superseded by the final B0/C0/C1 comparison (31/57/59) in docs/SMP3_RELEASE_GATE_DIFFERENCES.md. The proposal itself did not approve an exception.

The historical BEFORE and current raw checker remain unchanged. Original 31 and input 51 failures, current failures and complete path hashes remain evidence. Raw FAIL is not waived by this proposal.

A maintainer may separately review the exact path inventory in `artifacts/blocker-closure-20261013/gate-evolution-proposal-final8.json`: old/new SHA256, addition/deletion, concrete change reason, test evidence and named approval responsibility. Newly added files are exact paths; no directory exceptions or blanket allowlists. Unknown differences fail. No approval has been granted and no checker governance code was changed.

Use three independent decisions: immutable historical migration identity; explicitly approved version increment; current source/build/tests. Record CRLF/LF normalization separately from raw identity. An approved later evolution is not proof that the historical snapshot was identical. Transport fixes continue independently; release cannot turn the raw checker into PASS without governance.
