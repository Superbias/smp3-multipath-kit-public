# Security Notes

- Never put a production password, certificate private key, or private CA key in this package or source repository.
- Public Landing listeners require TLS certificate and private-key paths. Client certificate verification remains enabled; set `server_name` to the certificate identity and add only a trusted CA when needed.
- Each HELLO carries protocol version, uint8 Leg ID, Session ID, timestamp, nonce and destination under an HMAC-SHA256 keyed by the shared SMP3 password. The server checks nonce replay and session/leg admission. TLS authenticates the Landing endpoint; the password authenticates SMP3 HELLOs.
- Restrict public TCP listener exposure with firewall allowlists when possible. A shared password is a session admission credential, not per-user authorization or a protection against password disclosure.
- The local telemetry HTTP endpoint has no built-in authentication. Bind it to loopback or a protected management network; do not expose it directly to the Internet.
- Realm is an untrusted byte-stream forwarder and must not terminate end-to-end TLS. It sees routing metadata and traffic timing/volume but should not receive TLS plaintext.
- Sample passwords and example domains are placeholders. Replace before startup; config-check success only checks syntax/validation, not credentials or remote reachability.
