# Security Policy

## Supported versions

Only the latest released line receives security fixes.

| Version | Supported |
| ------- | --------- |
| 0.2.x   | Yes       |
| < 0.2   | No        |

## Reporting a vulnerability

Please report security issues **privately**. Do not open a public issue for
anything you believe is a vulnerability.

Preferred channel: GitHub Security Advisories — use the **"Report a
vulnerability"** button on this repository's *Security* tab. This keeps the
report private until a fix is available.

Fallback: email **dev@mmmbuto.com**.

When reporting, include the affected version, your platform/target, and the
steps to reproduce.

### Response times

This is a single-maintainer project, so handling is **best-effort**. Expect an
initial acknowledgement within a few days and a fix on a timeline that depends
on severity and available time. Please be patient.

## Scope notes

The optional HTTP plane is an **admin plane**, authenticated with a bearer
token. It is intended to run on loopback only, or reached through an SSH tunnel.
It must **never** be exposed to a public network, even with the bearer token
set. Treat any public exposure of the HTTP plane as a misconfiguration on the
deployer's side; the token is a second layer, not a substitute for keeping the
plane private.
