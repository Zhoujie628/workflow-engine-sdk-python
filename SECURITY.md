# Security Policy

## Supported Versions

The latest published release and the current `main` branch receive security
fixes. Pre-release APIs may change when required to close a vulnerability.

## Reporting a Vulnerability

Do not open a public issue. Email **security@openan.com** with a description,
reproduction steps, affected versions, and potential impact. Do not include
real production credentials or customer data.

We aim to acknowledge reports within 48 hours and provide a fix or mitigation
within 90 days. Coordinated disclosure timing will be agreed with the reporter.

## Security Defaults

- TLS certificate verification is enabled by default.
- Encrypted credentials fail closed when the key is absent or invalid.
- Full protocol payload logging is not intended for normal production use.
- `.env`, credentials, private keys, and generated distributions are excluded
  from source control.
