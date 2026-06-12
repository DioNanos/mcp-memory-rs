# Contributing

Thanks for your interest in improving this project.

## Development setup

```sh
cargo build
cargo test
```

Lint must be clean with warnings treated as errors:

```sh
cargo clippy --all-targets -- -D warnings
```

## Guidelines

- **Conventional commits** for commit messages (e.g. `fix:`, `feat:`,
  `docs:`, `refactor:`).
- **Modular by construction**: keep modules small and single-purpose rather
  than refactoring large files after the fact.
- For substantial changes, please **open an issue to discuss first** before
  sending a large pull request. Small fixes can go straight to a PR.

## License

This project is licensed under **Apache-2.0**. By contributing, you agree that
your contributions are licensed under the same terms.
