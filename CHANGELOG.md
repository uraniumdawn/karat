# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Copy Consumer Group offsets** (`Ctrl+e` on consumer group detail page): opens a modal with a
  single input field where the user enters a new consumer group name. On `Ctrl+Enter` the current
  committed offsets of the source group are copied to the new group, implicitly creating it.

## [0.2.2] - 2026-06-01

### Added
- Theme previews and a style gallery in `examples/style/README.md` for all built-in themes.

### Fixed
- `default_config.yaml` and `default_style.yaml` are now embedded in the binary via `//go:embed`.
  Previously the app read them from the working directory at startup, causing it to crash when
  installed via Homebrew (or run from any directory other than the source root).

## [0.2.1] - 2026-05-30

### Added
- GitHub Actions release workflow (`.github/workflows/release.yml`) using GoReleaser.

## [0.2.0] - 2026-05-30

### Added
- Initial public release.

[Unreleased]: https://github.com/uraniumdawn/karat/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/uraniumdawn/karat/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/uraniumdawn/karat/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/uraniumdawn/karat/releases/tag/v0.2.0
