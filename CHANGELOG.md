# Changelog

All notable changes to this project will be documented in this file.

---

## [Unreleased]

---

## [0.2.3] - 2026-06-02

## Features

* **Latest version check**: on startup karat checks GitHub for the latest release and notifies the user if a newer version is available.

## Bug Fixes

* Remove invalid `RESTART` action from the connector actions modal for `PAUSED` connectors — only `RESUME` is valid in that state.

---

## [0.2.2] - 2026-06-01

## Features

* **Copy Consumer Group offsets** (`Ctrl+e` on consumer group detail page): opens a modal to enter a target group name; on `Ctrl+Enter` copies the committed offsets of the source group to the new group, implicitly creating it.
* Add theme previews and style gallery to `examples/style/README.md`.

## Bug Fixes

* Embed `default_config.yaml` and `default_style.yaml` in the binary via `//go:embed` — fixes crash on startup when installed via Homebrew or run outside the source root.

---

## [0.2.1] - 2026-05-30

## Maintenance

* Add GitHub Actions release workflow (`.github/workflows/release.yml`) using GoReleaser.

---

## [0.2.0] - 2026-05-30

* Initial public release.

---

[Unreleased]: https://github.com/uraniumdawn/karat/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/uraniumdawn/karat/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/uraniumdawn/karat/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/uraniumdawn/karat/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/uraniumdawn/karat/releases/tag/v0.2.0
