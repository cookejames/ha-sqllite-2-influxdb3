# Home Assistant SQLite to InfluxDB 3 Migration Script

This script migrates historical data from a Home Assistant SQLite database to **InfluxDB 3**.

It is based on [eldigo/ha-sqllite-2-influxdb](https://github.com/eldigo/ha-sqllite-2-influxdb) and has been updated to target **InfluxDB 3** using the [`influxdb3-python`](https://github.com/InfluxCommunity/influxdb3-python) client and **InfluxQL** instead of the InfluxDB 2 client and Flux.

**Repository:** [cookejames/ha-sqllite-2-influxdb3](https://github.com/cookejames/ha-sqllite-2-influxdb3)

## How it works

The script connects to both your Home Assistant SQLite database and your InfluxDB 3 instance, then:

1. Discovers all existing measurements (tables) in InfluxDB.
2. For each measurement, finds the oldest existing record.
3. Queries SQLite for matching records **older** than that timestamp:
   - Measurements named like an entity ID (e.g. `sensor.living_room_temp`) are matched by `entity_id`.
   - Measurements named like a unit of measurement (e.g. `°C`, `%`, `kWh`) are matched by the `unit_of_measurement` attribute.
4. Writes the historical rows into the corresponding InfluxDB measurement.
5. If `IMPORT_STATISTICS_DATA=true`, it will also perform a second pass to import long-term data from the `statistics` table for each valid entity.

This means the script safely backfills data without overwriting anything already in InfluxDB.

## Prerequisites

- Python 3.9 or higher
- A Home Assistant SQLite database file (`home-assistant_v2.db`)
- An InfluxDB 3 instance running and accessible with existing measurements

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/cookejames/ha-sqllite-2-influxdb3
cd ha-sqllite-2-influxdb3
```

### Step 2: Create a Virtual Environment

```bash
python3 -m venv .venv
```

### Step 3: Activate the Virtual Environment

```bash
source .venv/bin/activate
```

### Step 4: Install Requirements

```bash
pip install -r requirements.txt
```

### Step 5: Configure Environment Variables

Copy `.env.example` to `.env` and fill in the required values:

```bash
cp .env.example .env
```

```plaintext
SQLITE_DB=/path/to/home-assistant_v2.db
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_token
INFLUXDB_DATABASE=your_database
BATCH_SIZE=10000
DEBUG_MODE=false
DRY_RUN=false
IMPORT_STATISTICS_DATA=true
```

### Environment variable reference

| Variable | Required | Description |
|---|---|---|
| `SQLITE_DB` | ✅ | Path to the Home Assistant SQLite database |
| `INFLUXDB_URL` | ✅ | InfluxDB 3 URL (e.g. `http://localhost:8086`) |
| `INFLUXDB_TOKEN` | ✅ | InfluxDB auth token |
| `INFLUXDB_DATABASE` | ✅ | InfluxDB database/bucket name |
| `BATCH_SIZE` | ❌ | Rows processed per batch (default: `10000`) |
| `DEBUG_MODE` | ❌ | Write points one-by-one for easier debugging (default: `false`) |
| `DRY_RUN` | ❌ | Print line protocol to stdout instead of writing (default: `false`) |
| `IMPORT_STATISTICS_DATA` | ❌ | Import long-term data from the Home Assistant `statistics` table (default: `false`) |

## Usage

Run the script:

```bash
python3 sqllite2influxdb.py
```

### Dry run

To preview what would be written without touching InfluxDB:

```bash
DRY_RUN=true python3 sqllite2influxdb.py
```

You can pipe the output to a file to inspect it:

```bash
DRY_RUN=true python3 sqllite2influxdb.py > preview.lp 2>run.log
```

## Differences from the original

| | Original ([eldigo](https://github.com/eldigo/ha-sqllite-2-influxdb)) | This fork |
|---|---|---|
| InfluxDB version | 2.x | 3.x |
| Query language | Flux | InfluxQL |
| Client library | `influxdb-client` | `influxdb3-python` |
| Config | `INFLUXDB_ORG` + `INFLUXDB_BUCKET` | `INFLUXDB_DATABASE` |
| Import strategy | Single pass, oldest global timestamp | Per-entity grouping, targeted SQLite queries |
| Dry run | ❌ | ✅ |
| Long-term Statistics | ❌ | ✅ (Optional) |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
