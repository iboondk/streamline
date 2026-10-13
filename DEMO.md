# Demo script: recording a GIF or walkthrough

This script records the pipeline end to end: events go in, deduplicated counts come out in ClickHouse and Grafana, and a full replay leaves those counts unchanged. It is a script to follow, not a recording. No demo has been recorded yet, and the authoring machine has no Docker, so the steps below have not been run.

Target length: about 90 seconds for a GIF, or 3 to 4 minutes for a Loom video.

## Before you record

1. Docker Desktop is running with at least 4 GB of memory for containers.
   Node.js 20+ is installed, and `make install-producer` (the `npm ci` in `tools/producer`) has been run once. Step 5 runs the producer on the host.
2. A clean state. Run `make down` to remove the containers and their data, then continue.
3. Terminal font at 16 pt or larger, dark theme, window about 1280 px wide.
4. Browser at 1280 x 800 with Grafana open on a blank tab.
5. Screen recorder ready. Use QuickTime or Loom on macOS, or `ffmpeg` on the command line.
6. Keep the recording outputs in `docs/media/`, inside the project folder.

## Script

| Step | On screen | Command or action | Say or show |
|---|---|---|---|
| 1 | Terminal at the project root | `cat .env.example && cp .env.example .env` | Local dummy values only. The copy is optional for `up`, but step 7 reads it. |
| 2 | Terminal | `docker compose up -d` | One command starts Kafka, Schema Registry, ClickHouse, Grafana, both consumers, and the one-shot `init-topics` and `init-schemas` services. |
| 3 | Terminal | `docker compose ps` | Wait until the services show `healthy`. `init-topics` and `init-schemas` show `Exited (0)`. |
| 4 | Terminal | `docker compose logs init-topics init-schemas` | Shows the four topics created and the Avro schemas registered with BACKWARD compatibility. `make bootstrap` does the same by hand and is optional. |
| 5 | Terminal | `make produce ARGS="--rate 200 --duration 30"` | A seeded generator produces orders and inventory events through the registry. |
| 6 | Browser | Grafana, `Orders per minute` and `Exact revenue (ORDER_PAID, FINAL)` panels | Counts rise as the events land. The exact revenue panel reads `orders_raw FINAL`. |
| 7 | Terminal | `docker compose exec clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamline --query "SELECT count(), uniqExact(event_id) FROM orders_raw"` | Raw rows and unique events. A difference means duplicates are waiting for a merge. Run `set -a; . ./.env; set +a` first so the variables are set. |
| 8 | Terminal | `make replay` | Resets the orders group to the earliest offset and reprocesses the topic. |
| 9 | Terminal | Read the `verdict` block | `PASS` means the unique and FINAL counts are the same before and after the replay. |
| 10 | Terminal | Read the reconciliation line | MV revenue minus raw FINAL revenue. A non-zero value is the known limit in `docs/ADR-0005-sum-based-revenue-mv-limit.md`. Say so on camera. |
| 11 | Editor or browser | Open `docs/FAILURE_HANDLING.md` | The failure modes and recovery steps behind the numbers. |
| 12 | Terminal | `make down` | Tear down. This also removes the data. |

## Things to avoid on camera

- Do not quote throughput, lag or revenue figures unless they come from a run you just made. Use `make measure` to produce them; the output is written to `results/`.
- Do not show `.env` with real values. Only show `.env.example`.
- Do not claim exactly-once. Say effectively-once and point to the three dedupe layers.

## Turning the recording into a GIF

With `ffmpeg` (install from your package manager):

```bash
ffmpeg -i docs/media/demo.mov -vf "fps=12,scale=1280:-1:flags=lanczos,palettegen" -y docs/media/palette.png
ffmpeg -i docs/media/demo.mov -i docs/media/palette.png \
  -lavfi "fps=12,scale=1280:-1:flags=lanczos [x]; [x][1:v] paletteuse" -y docs/media/demo.gif
```

Keep the GIF under about 10 MB so it renders in the README. For a long clip, a Loom link is easier to share. Either way, record the steps in the order above.
