# SMP3 v2.7.0-selfuse.2 Phase32 Release Notes

**Self-use / risk accepted build, not GA.** This bundle is for owner-controlled installation and observation. It adds configurable N-leg B0 Client/Landing support through the shared Core and same-listener Session/Leg attachment.

The local loopback suite exercises N=3, 4, 8 and 16 on one Landing listener with unique application DATA contribution per configured Leg, a shared Session, SHA-256 verification, strict EOF, ACK coverage and TX/RX ledger cleanup. This is local integration evidence, not WAN performance certification.

Known release limits: P0-C and General Release remain FAIL / NOT READY until the actual gate report closes them; Windows RTT test failures (if present in the frozen run index) remain explicitly recorded; Linux kernel TCP blackhole, Native Mihomo runtime, real WAN, long-haul physical path independence and Go 1.25.5 cross-platform race behavior are not implied by local builds. Strict Zero Retry/Rescue gates are not declared passed. B1 remains experimental/unexposed and disabled in these examples; 300 bp natural-budget behavior is not validated here.

No production server was contacted or changed. Review `KNOWN_ISSUES.md`, `SECURITY_NOTES.md`, and the engineering evidence package before owner self-deployment.
