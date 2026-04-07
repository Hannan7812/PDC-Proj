# Distributed Document Processing (PDC Project)

## Run

1. Build:
   - `mvn -q -DskipTests compile`
2. Start master:
   - `mvn -q exec:java -Dexec.mainClass=pdc.AppMaster`
3. Start 3+ workers (separate terminals):
   - `mvn -q exec:java -Dexec.mainClass=pdc.AppWorker`

## Data

Put `.txt` files in `/data` relative to project root (or absolute `/data` if present).

## Modes

- `WORD_COUNT`
- `INVERTED_INDEX`
