# Syncing with danzig-crawler

`db-schema-tool` is the **de-branded-for-work** fork of `danzig-crawler`
(in the `danzig` monorepo at `../danzig/danzig-crawler`). They share one
codebase. The *only* intentional differences are branding + the portal
webhook layer — **all crawl logic must stay byte-identical**.

## Files that MUST stay identical (the actual crawler logic)

`ai_client.py`, `constants.py`, `field_matcher.py`, `json_parser.py`,
`stage1.py`, `stage2.py`, `stage3.py`, `report_html.py`, `types.py`

If you improve any of these here, copy them over verbatim.

## Files that are intentionally DIFFERENT — do NOT blindly overwrite

| File            | Difference                                                        |
|-----------------|------------------------------------------------------------------|
| `connection.py` | app tag: `db-schema-tool` ↔ `danzig-crawler` (lines ~84, ~106)   |
| `memory.py`     | `CONTAINER_TAG` value (line ~20)                                  |
| `cli.py`        | prog name (`dbscan`↔`danzig-crawl`) + portal webhook POST in `full` |
| `report.py`     | danzig fetches SQL from / POSTs results to the portal             |
| `transform.py`  | **danzig-only** — portal payload transform; not present here      |

## The sync procedure

Run this diff from `Claude_Projects/`:

```sh
for f in ai_client constants field_matcher json_parser stage1 stage2 stage3 report_html types; do
  diff -q "db-schema-tool/crawler/$f.py" "danzig/danzig-crawler/crawler/$f.py"
done
```

Any output = drift in shared logic → reconcile. The five files in the table
above are *expected* to differ; review them by hand, never copy wholesale.

> NOTE: the `danzig` monorepo is normally hands-off from local Claude Code
> chats (remote-agent SessionStart hooks). Confirm no remote agent is running
> and work on a branch before editing `danzig/danzig-crawler`.

_Last verified in sync: 2026-06-17 (all 9 shared files byte-identical)._
