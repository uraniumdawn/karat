# Changelog

All notable changes to this project will be documented in this file.

---

## [Unreleased]

---

## [0.2.6] - 2026-06-14

## Enhancements

* Borders now omit vertical line characters, giving panels a cleaner, more minimal look (back to how it looks in 0.2.4).

---

## [0.2.5] - 2026-06-14

## Features

* **Karat format for Avro schemas**: on the Schema Registry "Schema" description page, press `1` to view the schema in Karat's compact, hierarchical text format (`<name>:<type>:<extra>` per line), or `2` to switch back to pretty-printed JSON (default).

## Enhancements

* Stopping consumption (`t`) now reacts immediately, even on hot topics — the consumer goroutine and UI no longer block on a backlog of in-flight messages/draws.

---

## [0.2.4] - 2026-06-13

## Features

* **Transactions page**: browse cluster-wide Kafka transactions (transactional ID, producer ID, state, timeout, partitions), with sorting (`1`/`2`) and search (`/`). Requires franz-go connectivity for the cluster.
* **ACLs page**: view cluster ACLs (principal, resource type/name, pattern type, operation, permission), with sorting and search. Requires franz-go connectivity for the cluster.
* **Topic Producers**: view active producers and in-flight transactions for each partition of a topic, via the new "Extra Actions" modal.
* **Extra Actions modal** (`>` on the Topics list and the Topic description page): consolidates secondary actions — "Consume", "CLI commands", and "Producers".
* **Hide internal topics** (`i` on the Topics list): toggle hiding topics matching configurable regex patterns. Defaults (`^__.*`, `.*-changelog$`, `.*-repartition$`) can be overridden via `karat.ui.internal_topic_patterns` in the config file.
* **Connector Offsets**: new `o` keybinding on the Connector description page opens an offsets view; `Ctrl+d` deletes the connector's offsets (connector must be stopped first), and `c` copies offsets to another connector.
* Topic description page now shows the topic's actual on-disk size, aggregated across replicas.
* Connector actions: added `STOP` for running connectors and `RESUME` for stopped connectors.

## Enhancements

* Auto-update keybinding changed from `Ctrl+g` to `g`, and removed from list pages (Topics, Consumer Groups, Subjects, Versions, Connectors, Connector Details) — still available on detail/description pages.
* Auto-update intervals changed from 1s/3s/5s/10s/20s to 1s/5s/10s/30s/60s.
* Consumer groups: "Copy to..." keybinding changed from `Ctrl+e` to `c`.
* Connectors list loads faster — connector statuses are now fetched concurrently (up to 8 in parallel) instead of sequentially.

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

[Unreleased]: https://github.com/uraniumdawn/karat/compare/v0.2.6...HEAD
[0.2.6]: https://github.com/uraniumdawn/karat/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/uraniumdawn/karat/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/uraniumdawn/karat/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/uraniumdawn/karat/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/uraniumdawn/karat/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/uraniumdawn/karat/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/uraniumdawn/karat/releases/tag/v0.2.0
