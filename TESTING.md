# Manual test checklist

A pass over every user-visible feature of karat, run against the local environment in
`docker-compose.yml`. It is a checklist, not a script: each item names what to do and what
counts as correct, so a run can be split across sessions and handed to someone else.

## Setting up

The stack needs about 4 GiB to itself: Kafka, Schema Registry and a Connect worker are three
JVMs, and the `txn` profile adds a fourth. On colima, give the VM room before starting it —
2 GiB is not enough, and the Connect worker is the one the kernel kills first:

```bash
colima stop && colima start --cpu 4 --memory 8
```

```bash
docker-compose up -d                    # core stack
docker-compose --profile txn up -d      # + transactional producer (Transactions page)
mkdir -p .local/.config/karat
cp examples/config.local.yaml .local/.config/karat/config.yaml
CGO_ENABLED=1 go build -o karat
KARAT_CONFIG_DIR=$PWD/.local ./karat
```

The seeded cluster holds: `stream-alpha` and `stream-beta` (live Avro traffic), `txn-events`
(transactional, with the `txn` profile), `connect-file-lines`, the compacted `user-profiles`,
`audit-log`, `orders-changelog`, `orders-repartition`, `empty-topic`, two consumer groups
(`stream-alpha-live` at ~zero lag, `stream-alpha-lagging` lagging), four Avro subjects,
three connectors (one with a FAILED task), and six ACLs.

Log to check while testing: `karat.log` in the config directory — under `KARAT_CONFIG_DIR`
when it is set, `~/.config/karat/` otherwise. A page that suddenly reports a
connection error is worth checking against `docker-compose ps` first — a container the VM
killed for memory (`docker inspect <name> --format '{{.State.OOMKilled}}'`) looks like a karat
bug from the inside.

## 1. Startup and configuration

- [ ] 1.1 Starts with no config file at all: writes a default `config.yaml`, comes up on an
      empty Clusters page.
- [ ] 1.2 Starts with `examples/config.local.yaml`: cluster, Schema Registry and Connect from
      the config are auto-selected (`selected: true`), each shown in the header.
- [ ] 1.3 The merged config is written back on start, keeping `${VAR}` references unexpanded —
      including a reference to a variable that is not exported, and one in a non-string field
      (`api.timeout`).
- [ ] 1.4 A `style:` path that does not exist refuses to start, with the reason on stderr.
- [ ] 1.5 Each theme in `examples/style/` loads and repaints every page.
- [ ] 1.6 `karat --version` prints version, commit and build date.
- [ ] 1.7 `F12` opens the cluster config view; `Esc`/`F12` closes it.
- [ ] 1.8 A newer release available upstream shows the update hint next to the version.

## 2. Modes

- [ ] 2.1 `Tab` on the Clusters page cycles read-only → confirm → yolo, badge follows.
- [ ] 2.2 The badge stays beside the page title when a modal is open (consume parameters,
      opened pages, extra actions), rather than sliding into the border's left corner.
- [ ] 2.3 The chosen mode is written back to `config.yaml` and survives a restart.
- [ ] 2.4 read-only: every modifying action is refused with a status-line message, and nothing
      reaches the cluster.
- [ ] 2.5 confirm: a modifying action asks in the status line; `Y` runs it, `N` and `Esc`
      abandon it; while the question stands no other key does anything.
- [ ] 2.6 yolo: a modifying action runs with no question.
- [ ] 2.7 A mode switched while an editor or a confirmation page is open is honoured at apply
      time, not at open time.

## 3. Navigation

- [ ] 3.1 `:` opens the resource menu, a second `:` opens its search field, typing filters it,
      and `Enter` (twice: once to leave the field, once to select) opens the resource.
- [ ] 3.2 Every resource opens: Clusters, Schema-registries, Connect, Nodes, Topics, Consumer
      groups, Transactions, ACLs, Subjects, Connectors.
- [ ] 3.3 `l` / `h` walk forward and backward through the opened pages in the order they were
      opened, a newly opened page sitting right after the page that opened it.
- [ ] 3.4 `Ctrl+P` lists the opened pages; `/` filters them fuzzily; `Enter` switches — including
      straight after typing a filter, without moving the cursor first.
- [ ] 3.5 `x` in the opened-pages modal removes a page, and the selection lands on a sane
      neighbour.
- [ ] 3.6 The opened-pages modal opens with the current page selected, so `Esc` without moving
      the cursor leaves you on that page instead of jumping to the first one in the list.
- [ ] 3.7 A confirmation page (topic create/update, offsets, connector config/create) never
      appears in the opened-pages list and `h`/`l` never step into it.
- [ ] 3.8 `Ctrl+P` and `:` on a confirmation page are refused with `finish the open
      confirmation first`, and both work again once it is applied or abandoned.
- [ ] 3.9 Re-opening an already open page reuses it (served from cache) instead of refetching.
- [ ] 3.10 `Ctrl+U` forces a refresh of the current page, bypassing the cache.
- [ ] 3.11 `Ctrl+C` quits from anywhere; `Esc` closes a modal rather than quitting. No other
      key quits.

## 4. Clusters, nodes, cluster config

- [ ] 4.1 Clusters page lists every configured cluster with its bootstrap servers, the active
      one marked.
- [ ] 4.2 `Enter` switches cluster; pages of the previous cluster stay addressable and the new
      cluster's pages are keyed separately.
- [ ] 4.3 `d` shows cluster details.
- [ ] 4.4 Nodes lists the broker(s); the `Role` column reads `controller` for the one running
      it.
- [ ] 4.5 `d` on a node shows its configuration.
- [ ] 4.6 A cluster that cannot be reached reports the connection error on the status line and
      leaves the UI usable.

## 5. Topics list

- [ ] 5.1 Lists every topic with partitions, replication and, when
      `features.topic_size` is on, the Size column filled in asynchronously.
- [ ] 5.2 `features.topic_size: false` drops both the column and the DescribeLogDirs call.
- [ ] 5.3 `1`/`2`/`3` sort by name, partitions and size, ascending then descending.
- [ ] 5.4 `i` hides and shows internal topics, matching `ui.internal_topic_patterns`
      (`__consumer_offsets`, `orders-changelog`, `orders-repartition`, the connect topics).
- [ ] 5.5 `/` filters the list; `Esc` clears the filter and the full list returns, both from
      inside the field and from the list itself.
- [ ] 5.6 The filter is remembered per page while navigating away and back.
- [ ] 5.7 `Ctrl+U` refreshes the list.

## 6. Topic create, edit, delete, recreate

- [ ] 6.1 `n` opens the new-topic document in the editor.
- [ ] 6.2 Submitting it opens the confirmation page showing name, partitions, replication and
      configs.
- [ ] 6.3 `Ctrl+Enter` creates the topic; **the topics list then shows it without a manual
      refresh**.
- [ ] 6.4 `Esc` on the confirmation page creates nothing.
- [ ] 6.5 A document that changes nothing reports "no changes detected" and opens no page.
- [ ] 6.6 `e` edits a topic: partition increase and config changes are shown as a diff and
      applied on `Ctrl+Enter`.
- [ ] 6.7 A partition *decrease* is refused with a clear message.
- [ ] 6.8 `Ctrl+D` deletes a topic after the mode's confirmation; **the topics list then drops
      it without a manual refresh**.
- [ ] 6.9 Deleting a topic drops its own pages (description, producers, consume output) from
      the opened-pages list, without moving the user off the page they are on. Same for a
      deleted consumer group and connector.
- [ ] 6.10 Extra actions (`.`) → Clone topic: copies partitions, replication and configs to a
      new name.
- [ ] 6.11 Extra actions → Recreate topic: same name, same settings, no records, and the wait
      for the deletion to propagate is handled (no "topic already exists" error).
- [ ] 6.12 Creating a topic that already exists reports the broker's error.

## 7. Topic description and producers

- [ ] 7.1 `d` shows partitions, leaders, replicas, ISR, offsets and the effective config; the
      Size line carries the real on-disk size, or says it is unavailable — never a guess.
- [ ] 7.2 `H`/`L` scroll a wide description horizontally.
- [ ] 7.3 The producers view lists the producers writing to the topic. Only idempotent and
      transactional producers register with the broker, so check it against `txn-events` with
      the `txn` profile running — the data generator writes without idempotence and never
      appears there.
- [ ] 7.4 Extra actions (`.`) → CLI commands opens the templates for the topic; `c` copies a
      command, `e` executes it and streams the output; `t` terminates and `Ctrl+K` kills the
      process.

## 8. Consume

- [ ] 8.1 `c` on a topic starts consuming straight away, with the parameters last used on it or
      the defaults. `.` → Consume opens the parameters instead, showing those defaults: `-o 100`,
      avro key and value plus `-r <registry>` when one is selected, and the JSON `-f` format.
- [ ] 8.2 `Ctrl+Enter` starts consuming; records stream into the output page and the title
      counts them.
- [ ] 8.3 Avro values are decoded through the Schema Registry (`stream-alpha`, key and value), and a
      non-Avro payload falls back to raw instead of erroring out. An Avro string key prints
      unquoted, and decoding never takes the process down.
- [ ] 8.4 `-o beginning`, `-o end`, `-o <n>`, `-s:<offset>`, `-s@<ts>`, `-e:`, `-e@` and `-e`
      each select the range they claim.
- [ ] 8.5 `-p` restricts to a partition, repeatable.
- [ ] 8.6 There is no `-g` flag: karat consumes under its own ephemeral group id and never
      commits, so browsing a topic cannot move anyone's offsets. `-g whatever` is rejected as an
      unknown flag.
- [ ] 8.7 `-d key=<pack>` decodes a binary key (`>i`, `>qs`).
- [ ] 8.8 `-d avro` without `-r` is refused, and an unknown registry name is refused.
- [ ] 8.9 `-f` renders the format string, `| <pattern>` filters the output lines.
- [ ] 8.10 `F1` shows the flag reference, `F2` the consume statistics (per-partition counts and
      offset ranges).
- [ ] 8.11 `Ctrl+O` edits the parameters in the external editor, with the reference commented
      out below them, and the comments are dropped on the way back.
- [ ] 8.12 `Ctrl+R` shows the history for **this topic only**, newest first, params column
      only; `Enter` fills them in, `Ctrl+Enter` runs them.
- [ ] 8.13 The history survives a restart and stays scoped per cluster and topic; consuming
      other topics never evicts it.
- [ ] 8.14 `t` stops an active consume; the record count freezes and the status says so.
- [ ] 8.15 Consuming the same topic twice reuses one output page rather than stacking them.

## 9. Consumer groups

- [ ] 9.1 Lists groups with state, members and, when `features.consumer_group_lag` is on, lag
      (`stream-alpha-lagging` visibly lagging).
- [ ] 9.2 `features.consumer_group_lag: false` drops both the column and the extra calls.
- [ ] 9.3 `1`/`2`/`3` sort by name, state and lag.
- [ ] 9.4 `d` describes a group: members, assignments, committed offsets, lag per partition.
- [ ] 9.5 `g` enters auto-update mode, `Tab` sets the interval, `Esc` leaves it, and the page
      refreshes on that interval.
- [ ] 9.6 `o` resets offsets by topic and `O` by partition: the editor opens, the confirmation
      page lists the pending changes, `Ctrl+Enter` commits them.
- [ ] 9.7 A target outside the partition's range is refused before anything is committed.
- [ ] 9.8 Resetting offsets for a live group behaves (either refused or applied once the group
      is empty — whichever karat claims).
- [ ] 9.9 `Ctrl+D` deletes a group; the list drops it without a manual refresh.
- [ ] 9.10 Extra actions → Clone consumer group: copies the committed offsets to a new group.

## 10. Transactions

- [ ] 10.1 Lists the transactional IDs with state, producer id and coordinator
      (`karat-txn-1` while the `txn` profile runs).
- [ ] 10.2 `2` sorts by state.
- [ ] 10.3 `d` describes a transaction: state, timeout, topics involved.
- [ ] 10.4 A cluster with no franz-go connectivity says so instead of showing an empty page.

## 11. ACLs

- [ ] 11.1 Lists the seeded ACLs: principal, host, operation, permission, resource type and
      pattern (literal and prefixed both shown).
- [ ] 11.2 `2` sorts by the second column.
- [ ] 11.3 `/` filters.
- [ ] 11.4 (No local coverage: the stack always runs an authorizer. The SECURITY_DISABLED
      branch is pinned by `TestACLsFromResultsReportsADisabledAuthorizer`.)

## 12. Schema Registry

- [ ] 12.1 Schema-registries page lists the configured registries; `Enter` selects one.
- [ ] 12.2 Subjects lists `stream-alpha-key`, `stream-alpha-value`, `stream-beta-key` and
      `stream-beta-value`.
- [ ] 12.3 `Enter` on a subject lists its versions; `d` shows a version's schema — including a
      subject with a single version, where the only row is the first one.
- [ ] 12.4 Both lists carry a column header (`Subject`, `Version`), the header survives a
      filter and a refresh, and the cursor never rests on it.
- [ ] 12.5 `1` shows the karat format, `2` raw JSON.
- [ ] 12.6 Find by schema id resolves an id to its schema, with the subjects and versions it is
      registered under in the page title.
- [ ] 12.7 Extra actions → Clone subject: registers the schema under a new subject name.
- [ ] 12.8 Extra actions → Delete subject and Delete version, each behind the mode's
      confirmation, and the list updates afterwards.
- [ ] 12.9 A registry that is unreachable reports the error and leaves the UI usable.

## 13. Kafka Connect

- [ ] 13.1 Connect page lists the configured Connect clusters; `Enter` selects one.
- [ ] 13.2 Connectors lists `file-source`, `file-sink` and `file-sink-broken` with type, state
      and task state; the broken one shows FAILED.
- [ ] 13.3 `1`/`2`/`3` sort; `/` filters.
- [ ] 13.4 `d` describes a connector: config, tasks, and the failure trace for the broken one.
- [ ] 13.5 `a` on a connector: pause, resume, restart — the state on the list follows.
- [ ] 13.6 `a` on a running connector's task: restart the task.
- [ ] 13.7 `e` edits the config in the editor; the confirmation page shows the new JSON;
      `Ctrl+Enter` applies it and the description reflects it.
- [ ] 13.8 `n` creates a connector from `{"name": ..., "config": {...}}`; invalid JSON, an
      empty name and an empty config are each refused with a reason.
- [ ] 13.9 A config the worker rejects (bad converter class) surfaces the worker's 400 message.
- [ ] 13.10 `Ctrl+D` deletes a connector; the list drops it without a manual refresh.
- [ ] 13.11 `o` shows connector offsets; `c` copies them; `Ctrl+D` deletes them.
- [ ] 13.12 A Connect cluster that is down reports it on the status line.

## 14. Search, status line, editor

- [ ] 14.1 `/` on every searchable page (topics, groups, subjects, connectors, transactions,
      ACLs) filters as you type and `Esc` restores.
- [ ] 14.2 A search matching nothing shows an empty table, not a stale one.
- [ ] 14.3 The status line shows the spinner while a call is in flight and clears afterwards.
- [ ] 14.4 An error message is shown in red and expires on its own.
- [ ] 14.5 A standing confirmation outlives background messages.
- [ ] 14.6 The editor from `karat.editor` is used everywhere (topic configs, offsets, connector
      configs, consume params); an editor that exits non-zero cancels the operation.
- [ ] 14.7 Quitting the editor without saving changes nothing.

## 15. Failure handling

- [ ] 15.1 Stopping the broker mid-session: pages report the error, and the UI recovers once it
      is back.
- [ ] 15.2 `api.timeout` elapsing on a call reports a timeout instead of hanging.
- [ ] 15.3 Stopping Schema Registry or Connect degrades only their pages.
- [ ] 15.4 Nothing panics on an empty topic, an empty group list or an empty registry.
- [ ] 15.5 `karat.log` carries the errors, and the UI never prints a raw stack trace.
