# Security Policy

## Supported version

Security fixes target the current `main` branch. This repository is a local
reference implementation and does not claim production hardening.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting flow under the repository
**Security** tab when it is available. Do not publish exploit details,
credentials, personal data or sensitive environment information in a public
issue.

Include:

- affected file or component;
- reproduction steps with synthetic data;
- expected and observed behavior;
- impact and any safe mitigation already tested.

General bugs without sensitive details may use public issues.

## Secret handling

Never commit real tokens, passwords, private keys or cloud credentials.
Examples must use placeholders or locally generated development Secrets.
If a real secret is exposed, revoke it outside this repository before
submitting the cleanup.
