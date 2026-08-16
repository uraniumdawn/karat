# Changelog

All notable changes to this project will be documented in this file.

---

## [0.3.1] - 2026-08-16

## Features

* **A key reference on `?`**: the bindings that work everywhere, followed by the ones the page in front adds. Both sections are read from the same tables the bottom bar renders, so a binding cannot be documented in one and missing from the other. `Esc` or `?` puts it away, and it opens over anything — including a confirmation page, which it only reads.

## Enhancements

A pass over every keybinding in the application, so that a key means one thing wherever it appears.

* **`Ctrl+D` deletes in Kafka and nothing else.** It deleted a topic, a consumer group, a connector and connector offsets — and also closed the consume-output and CLI-execute pages, which the bar itself spelled out as "Remove page". Closing a page is `x` now, the key the opened-pages modal already used for it.
* **`Enter` runs a CLI template** instead of `e`. `e` means "edit" on every other page, which is the wrong promise for the key that runs a shell command.
* **`.` opens the actions for whatever is under the cursor, on connectors too.** Connector actions and task actions were on `a`, the only list pages that did not use `.`.
* **Resetting consumer group offsets is `e`/`E`**, by topic and by partition, instead of `o`/`O`. Both open a document in your editor, which is what `e` means everywhere else; `o` is left to mean "show me the offsets", as it does on a connector.
* **`Enter` opens the row under the cursor on every list**, alongside `d`: topics, consumer groups, connectors, nodes and transactions. Subjects and versions, which only answered to `Enter`, now take `d` as well. The Clusters page is the exception and says so in the bar — there `Enter` selects the cluster karat works against, and only `d` describes it.
* **`y` copies**, in the CLI templates modal and the connector offsets view, leaving `c` to mean "consume" wherever it appears.
* **`hjkl` moves within a page and nothing else.** Page history moved from `h`/`l` to `b`/`f` — the first letters of what the bar has always called them — and horizontal scrolling came down from `H`/`L` to `h`/`l`. Those are the keys tview scrolls a description or a table with in the first place; karat was intercepting them, which is why scrolling had to be invented on the shifted pair. Descriptions keep their five-column step rather than tview's one.
* **Auto-update mode is `a`** instead of `g`, which a description page needs for something else — see below.

## Fixes

* **A connector task could not be restarted without guessing.** The Action column on the task actions modal started blank, so `Ctrl+Enter` refused the row and asked for a `Tab` — to set the only action the Connect REST API offers for a task. Worse, a second `Tab` cleared the column again, leaving a row that could not be submitted at all. The action is filled in from the start and `Tab` now only cycles.
* **There was no way to back out of the opened-pages modal.** `Esc` and `Enter` both switched to the highlighted page, and moving the cursor already switched the page underneath, so opening the list to look at it left you somewhere else. `Enter` takes the highlighted page; `Esc` returns to the page the modal was opened from.
* **A long description could be scrolled to the bottom but not back to the top.** `G` jumps to the end, `g` to the beginning — except `g` was bound to auto-update mode, which swallowed it. Auto-update mode is `a` now and `g` does what it does everywhere else.
* **A wide list could not be scrolled sideways.** The Size, Lag and connector columns run past the edge of a narrow terminal, and the keys that would have moved the view — `h` and `l` — were taken by the page history before the table saw them. They reach the table now.

## [0.3.0] - 2026-08-15

## Features

* **Topics as a YAML document in your editor** (`n`, `e` on the Topics page): `name`, `replication_factor`, `partitions` and a `configs` block. Settings at their cluster default come along as commented-out lines, so overriding one means uncommenting it. Parsed strictly — bad YAML, an unknown key, a renamed topic or a decreased partition count is reported with its line number and nothing is applied. Saving opens a confirmation page listing the exact changes; nothing hits the cluster until `Ctrl+Enter`.
* **Clone Topic in the same document** (`.` → "Clone topic"): the source topic's definition opens in your editor pre-filled with its partition count, replication factor and overrides — change `name` and save. Same strict parsing and same confirmation page as create; a document that still carries the source topic's name is refused rather than left to fail against the broker as "already exists". Sensitive config entries, which `DescribeConfigs` reports with an empty value, are no longer carried into the clone as blanks, and the source topic's cluster defaults come along as commented-out lines.
* **Consumer group offsets in your editor** (on the Consumer Group description): `o` opens the group's topics as YAML, one value each, applied to every partition of the topic and starting at `none`; `O` opens its committed offsets one value per partition — a granularity the old Reset Offsets form could not reach. A value is an absolute offset, `earliest`/`latest`, `@<timestamp>` (formatted, RFC3339 or unix ms), or `none` to skip; top-level `all:` covers every topic. Confirmation page, then a single `AlterConsumerGroupOffsets` call. Deleting a partition leaves it untouched.
* **Seed offsets for a topic a group has never consumed**: name an unlisted topic in the document — partitions come from cluster metadata, and show as `(none) -> <offset>`. This writes a starting position only; nothing consumes the topic until a member subscribing to it joins.
* **Reset a topic config to the cluster default**: deleting a config line — or clearing its value — now sends an `IncrementalAlterConfigs` DELETE instead of being silently ignored.

* **A local Kafka to test against** (`docker-compose.yml`): a single-broker KRaft cluster on Kafka 4.0 with a Schema Registry and a Connect worker beside it, seeded on startup with topics of every shape karat has a view for — live traffic, compacted, short-retention, internal-looking, empty — plus six ACLs, four Avro subjects, three connectors (one with a deliberately failing task) and two consumer groups, one of them lagging on purpose. A data generator writes Avro records in the Confluent wire format, keys and values both, so the Schema Registry views have real payloads behind them rather than registered schemas nothing uses. Transactions come with `--profile txn`, which starts a transactional producer. `examples/config.local.yaml` points karat at all of it; `KARAT_CONFIG_DIR` keeps it out of your real config.
* **A test checklist** (`TESTING.md`): 121 numbered cases across every page and mode, written to be walked by hand and split across sessions, with the environment setup — including how much memory the stack wants — at the top.

## Enhancements

* **The Create/Edit/Clone Topic and Reset Offsets form modals are gone**: `n`, `e`, `.` → "Clone topic" and `o` open a document in your editor instead — everything the forms did, plus per-partition offsets and resetting a topic config to the cluster default. `Ctrl+O` inside a form is gone with them; the consume-parameters editor and the connector-config editor are unchanged.
* **The selected cluster, Schema Registry and Connect instance are marked in their tables**: a new `Active` column carries a `✓` against the entry currently in use, so which one `Enter` last selected is visible on the page itself instead of only in the header. The marker moves as soon as you select another entry, without leaving the page.
* **Shorter modifier notation in the keybinding bar**: `Ctrl+` is rendered as `C-` (`<C-u>`, `<C-Enter>`), the vim/kubectl convention.
* **`<j/↓,k/↑>` in the keybinding bar reads `Move` instead of `Selection`**: the keys move the cursor; `<Enter>`, listed next to them as `Select`, is what selects.
* **Configurable editor** (`karat.editor` in `config.yaml`, default `vim`): used by every editor-backed view, split on whitespace so flags come along (`"code --wait"`).

* **The default consume parameters decode keys as well as values**, and carry the JSON envelope again: `-o 100 -d key=avro -d value=avro -r <sr> -f '{"Key":"%k","Value":%s,…}'` with a registry selected, plain `-o 100 -f '{…}'` without one. Key and value are spelled out rather than written as the equivalent bare `-d avro`, so a topic with a string key is one deleted flag away instead of a rewrite. An Avro string key prints unquoted, the way a raw one does.
* **Consume history is per topic**: `Ctrl+R` lists what you ran on *this* topic, newest first, and nothing else — a single Params column, no topic or timestamp to read past. Each topic keeps its own 30 entries, so consuming one topic all day no longer evicts what you ran on another yesterday; the file as a whole is capped at 300.
* **A new topic starts at one partition and one replica** instead of zero of each. Zero failed validation on both counts, so every new topic had to be edited on three lines before it could be submitted; now only `name` is.
* **Confirmation pages stand outside the page history**: a topic, offsets or connector-config review is never listed in the opened-pages modal and `h`/`l` never step into it. Since there is no way back to one, `Ctrl+P` and `:` are refused while it stands — the status line says so — rather than leaving it hidden with its pending action stranded.
* **The cursor survives a refresh**: sorting, filtering, hiding internal topics or a `Ctrl+U` refresh all rebuild the table under the cursor, which used to drop it on the first row — and the next key acted on a topic you had not picked. The selected row is now remembered by name and put back.
* **A key pressed with nothing selected says so** instead of reaching the cluster with an empty topic or connector name: an empty list, a filter that matched nothing, or a selection a rebuild left past the end of the table.
* **The status line is down to two colours.** Warnings — "nothing selected", "no changes detected", the editor hints — carried a hardcoded yellow that no style file could reach and that was barely legible on the light themes. They now use the status colour the style sets, leaving red for failures alone. The update arrow in the version hint (`κεράτιον v0.3.0 → v0.3.1`) drops its yellow too and takes the hint's own label colour.
* **`karat.style` takes a filename, not just a path.** A relative `style:` is read from the config directory — where the style file is copied — instead of from whatever directory karat happened to be launched in, which is what the documentation had claimed all along. An absolute path, or one starting with `~/`, is still taken as written, and a style that cannot be read stops karat naming the resolved path.
* **The mode a confirmation is applied under is the one it was reviewed under.** Every operation reads the mode when it runs, not when the editor or the review page was opened, so a switch to read-only is honoured by anything still on its way. The other side of that window is closed too: `<Tab>` cycles the mode on the Clusters page, which a confirmation page cannot be left for, and the switch itself is refused while one stands.
* **The subject and version lists carry column headers** (`Subject`, `Version`), like every other list page.
* **A connector action refreshes when the worker has acted on it**: pause, resume, restart and stop are accepted by the REST API before the worker moves, so the list came back showing the state the connector was in a moment ago. karat now waits for the target state, up to five seconds, before refreshing.

## Fixes

* **Resetting offsets on a topic the group has never consumed works now**: partitions were taken solely from the group's committed offsets, so such a topic resolved to none — failing outright, or being silently dropped while the status line reported success. Partitions now come from cluster metadata, and an unknown topic name is reported.
* **The reset success message counted the wrong topics**: resetting one topic out of five reported "[5 topics]". It now reports the partitions actually committed.
* **A failed editor is reported instead of swallowed**: a missing binary surfaced later as "document is empty". It now reports `editor 'vim': executable file not found in $PATH — set karat.editor`. A non-zero exit (`:cq`) is treated as a deliberate abort — `nothing applied` — rather than applying whatever the file held.
* **A GUI editor without a wait flag is detected**: `editor: "code"` returned immediately and reported "no changes" with the tab still open. VS Code, Sublime Text and TextMate invoked without `--wait`/`-w` are now recognised and the flag is named.
* **Out-of-range offsets are refused**: the broker accepts them and the consumer then silently overrides via `auto.offset.reset`, so a mistyped offset used to vanish without a trace. It is now reported with the bounds it had to fall between.
* **The `-g <group>` consume flag is gone.** It set the client's `group.id` and nothing else: karat assigns partitions explicitly and never commits, so the group was never created, never joined, and never moved an offset — the flag looked like kcat's `-G` and did none of what that does. Karat consumes under its own ephemeral id instead, and `-g` is now rejected as an unknown flag.
* **Removed the dead command handling in the resource menu** — a `q!` case and short resource aliases (`cl`, `tps`, `grs`, …) left over from a command line that no longer exists; the menu publishes the event type of the row that was picked, so nothing could ever reach them. `Ctrl+C` quits, as it did before.
* **karat starts without a config file.** A first run had nothing to read and exited with `error reading config file`. It now comes up on the built-in defaults and writes them to `config.yaml`, so there is a file to edit and it says what karat is running.
* **`Esc` clears a search filter.** It used to hide the search bar and leave the rows filtered, with only the `/text` in the page title to say so — and reopening `/` appended to the text that was still there, which silently matched nothing. `Esc` now drops the filter and brings the whole list back, from inside the field and from the list itself; `Enter` is what keeps a filter and leaves the field.
* **`Enter` in the opened-pages modal works straight after typing a filter.** Filtering left the selection past the last match, and both `Enter` and `Esc` read that selection to decide where to go, so they closed the modal and went nowhere. The cursor is now put back in range after a rebuild — and only then, so it never overrides a selection the user made.
* **A deleted topic takes its own pages with it.** Its description, producers and consume-output pages stayed in the opened-pages list and still opened, showing what the topic looked like before it was deleted. The same now applies to a deleted consumer group and connector. Pages nobody is looking at are removed without moving the user.
* **The topic description reports the real size, or none.** "Estimated Size" came from a heuristic — `segment.bytes × 0.7 / 10000` as the average message size — which reads 75 KB per message at the default 1 GiB segment: it claimed 327 MB for a topic that was 728 KB. The line now shows the on-disk size karat already fetches with DescribeLogDirs, and says the size is unavailable when it has none.
* **The Nodes list marks the controller** in a `Role` column, rather than leaving it to the cluster description page.
* **Find-by-schema-id names where the schema is used.** The page showed the schema alone; the title now carries the subjects and versions the id is registered under.
* **The log follows `KARAT_CONFIG_DIR`.** It was hardcoded to `$HOME/.config/karat/karat.log`, so a session pointed elsewhere wrote into the default instance's log. It now sits beside the config it belongs to.
* **Error messages no longer repeat themselves** ("failed to create topic: failed to create topic '...'", same for connectors and transactions), and a timeout reads "timeout while listing topics" rather than "timeout while to list topics".
* **Consuming an Avro topic took the whole process down.** The Confluent generic deserializer unmarshals into a value from a caller-supplied `MessageFactory` and dereferences it unconditionally; karat never set one, so any payload with the Confluent magic byte panicked with a nil pointer dereference. Avro decoding no longer goes through it: schemas are fetched by the id in the payload, parsed once, cached, and decoded generically — no Go type per schema, which is the right shape for a tool that decodes whatever the topic happens to carry.
* **Environment variable references were erased from `config.yaml` on save.** Two ways: a reference to a variable that is not set expanded to nothing, which left the field empty, which `omitempty` dropped from the file altogether; and a reference in a field that is not a string — `api.timeout` — was written back as the number it expanded to. Since the config file is the only place those references are written down, either one lost them for good. Both are restored on write-back now.
* **The opened-pages modal opened on the wrong page.** It read the current page after bringing itself to the front, so it always saw itself, and the list kept whatever was selected last — the Clusters page, on a first open. `Esc`, which switches to the highlighted row, then jumped there instead of leaving you where you were.
* **`d` and `.` on a subject or a version reported "nothing selected".** Both lists start their rows at the top, having no header, and the selection guard read row 0 as one. A subject with a single version could not be opened at all.
* **The mode badge slid into the border's corner whenever a modal was open.** It anchors to the page title, and a modal is a page of its own with no title of its own; it now anchors to the title actually drawn on that border line.
* **The topic list refreshes after an edit.** A topic edited through the document editor left the list showing the old partition count until a manual `Ctrl+U`. A refused update no longer overwrites the broker's rejection with a refresh, either.

---

## [0.2.9] - 2026-08-09

## Features

* **Consume with one key** (`c` on the Topics list): starts consuming the selected topic immediately, with the parameters last used on it — no modal, no retyping. A topic that has never been consumed on the cluster falls back to the built-in defaults (`-o 100 [-r <sr>] -f '{…}'`). The parameters that ran are echoed in the status bar; a parse or schema-registry error is reported as usual and nothing starts.
* **Consume parameters history**: every parameter string that actually started a consume is remembered per cluster and topic in `~/.config/karat/history.yaml` (`KARAT_CONFIG_DIR` honoured), newest first, capped at 30 entries and de-duplicated. `Ctrl+R` in the Consume parameters modal opens a picker — the current topic's entries first, then the rest of the cluster's — where `Enter` fills the parameters in and `Ctrl+Enter` runs them straight away. The modal itself now opens prefilled with the topic's last-used parameters instead of the defaults. A missing or malformed history file is not an error: it simply starts empty.

## Enhancements

* **Consumer groups by topic from the Topics pages**: `.` → "Consumer groups" on the Topics list and the Topic description now opens the consumer groups that read the selected topic. Same page as the Consumer Groups list's "Find by topic", with the full consumer-groups functionality behind it (describe, delete, sort, search, `Ctrl+U` refresh, clone), without having to retype the topic name into a modal.
* **Edit consume parameters in your editor** (`Ctrl+O` on the Consume parameters modal): hands the current parameters to the editor, with the full flag reference — the same one `F1` shows — inlined below them as `#` comments, so the flags stay at hand while editing. Comment lines are dropped when the editor exits and the edited parameters go back into the modal. Uses the same suspend-the-TUI mechanism as the connector config editor.

---

## [0.2.8] - 2026-07-25

## Features

* **Topic size column**: the Topics list now shows each topic's actual on-disk size (summed across all replicas of all partitions). Sizes are fetched with a single `DescribeLogDirs` request per broker (via franz-go), filled in asynchronously so the list renders immediately, cached for 5 minutes, and refreshable with `Ctrl+U`. While the sizes are still on their way the header reads `Size…`. Sort by size with `3`. Requires franz-go connectivity — the column shows `-` when unavailable, and prefixes `~` when some replicas did not report (possible undercount).
* **Connector health column**: the Connectors list now shows a derived health per connector — `HEALTHY`, `DEGRADED` (some tasks failed), `UNHEALTHY` (connector failed, or running with no tasks / all tasks failed), plus `PAUSED` / `STOPPED` / `RESTARTING` / `UNASSIGNED` / `UNKNOWN`. Reuses the already-fetched connector statuses (no extra API calls). Sort by health with `4`.
* **Consumer-group lag column**: the Consumer Groups list (and the find-by-topic view) now shows each group's total lag — the sum over its committed partitions of (log-end − committed) offsets. Fetched with one `ListConsumerGroupOffsets` per group plus a single `ListOffsets` for all partitions, filled in asynchronously, cached for 5 minutes, and refreshable with `Ctrl+U`. While the lags are still on their way the header reads `Lag…`. Sort by lag with `3`.

## Enhancements

* **Configurable features** (`karat.features` in `config.yaml`): the columns that cost extra Kafka API calls can now be switched off individually — `topic_size` (Topics `Size` column and the topic description's actual size) and `consumer_group_lag` (Consumer Groups `Lag` column). Both default to `true`; setting one to `false` hides the column, drops its `3` sort key, and stops the extra cluster requests it needs. Useful on large or slow clusters.
* **Clone Subject** now copies the subject's entire schema version history — every version is fetched and re-registered oldest-first — instead of only the latest schema, preserving the full version lineage under the new subject.
* **Clone Consumer Group** (`.` → "Clone consumer group" on the Consumer Groups list): copies the source group's committed offsets to a new consumer group. Previously triggered by the `c` key on the Consumer Group description page; now consolidated into the Extra Actions modal to align with Clone Topic and Clone Subject.

---

## [0.2.7] - 2026-06-21

## Features

* **Clone Topic** (`.` → "Clone topic" on Topics list or Topic description): fetches the source topic's partition count, replication factor, and non-default config entries, then opens a pre-filled modal to create a new topic with the same configuration.
* **Clone Subject** (`.` → "Clone subject" on Subjects list): fetches the source subject's latest schema and compatibility level, then opens a modal to register the schema under a new subject name with the same compatibility.
* **Delete Subject** (`.` → "Delete subject" on Subjects list): soft-deletes all versions of a subject, with a confirmation modal.
* **Delete Subject Version** (`.` → "Delete version" on Versions page): soft-deletes a specific version of a subject, with a confirmation modal.
* **Find Schema by ID** (`.` → "Find schema by ID" on Subjects list): enter a global schema ID to view the schema, with JSON (`2`) / Karat (`1`) format toggle.
* **Extra Actions for Subjects and Versions**: `.` key now opens the Extra Actions modal on the Subjects list and Versions pages.
* **Extra Actions for Consumer Groups**: `.` key opens the Extra Actions modal on the Consumer Groups page, starting with "Find by topic".

## Enhancements

* Schema description page title now shows `<subject> [v:<version>] [id:<schema_id>]`.
* Karat format is now the default view for schema descriptions (both by subject/version and by ID), falling back to JSON when Karat formatting fails. Toggle with `1` (karat) / `2` (JSON).
* Extra Actions keybinding changed from `>` to `.` across all pages (Topics, Consumer Groups, Subjects, Versions).
* "Find consumer group by topic" moved from the `f` keybinding into the Extra Actions modal and simplified to a single input field with `Ctrl+Enter` to submit.
* Read-only mode is now enforced for Schema Registry mutating operations (clone, delete) — blocked with a status message when the registry is marked read-only.

## Bug Fixes

* Fixed swapped `Partitions` / `ReplicationFactor` arguments in `CreateTopicResultHandler` call.

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

[Unreleased]: https://github.com/uraniumdawn/karat/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/uraniumdawn/karat/compare/v0.2.9...v0.3.0
[0.2.9]: https://github.com/uraniumdawn/karat/compare/v0.2.8...v0.2.9
[0.2.8]: https://github.com/uraniumdawn/karat/compare/v0.2.7...v0.2.8
[0.2.7]: https://github.com/uraniumdawn/karat/compare/v0.2.6...v0.2.7
[0.2.6]: https://github.com/uraniumdawn/karat/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/uraniumdawn/karat/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/uraniumdawn/karat/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/uraniumdawn/karat/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/uraniumdawn/karat/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/uraniumdawn/karat/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/uraniumdawn/karat/releases/tag/v0.2.0
