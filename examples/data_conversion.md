# Data Conversion Examples

These examples convert local Parquet datasets and DuckDB databases to TSV or CSV without loading the complete table into memory.

Install the optional dependencies:

```bash
python -m pip install pyarrow duckdb
```

The examples write gzip-compressed output by default. Remove `COMPRESSION GZIP` from the DuckDB examples, or use a destination without `.gz` for the PyArrow example, when uncompressed output is required.

## Parquet to TSV or CSV with DuckDB

This version accepts either a single Parquet file or a directory containing Parquet shards.

```python
from pathlib import Path

import duckdb


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parquet_to_delimited_duckdb(
    source: str | Path,
    destination: str | Path,
    delimiter: str = "\t",
) -> int:
    source_path = Path(source)
    parquet_source = (
        source_path / "*.parquet" if source_path.is_dir() else source_path
    )
    destination_path = Path(destination)
    temporary_path = destination_path.with_name(
        destination_path.name + ".part"
    )

    source_sql = sql_string(str(parquet_source))
    temporary_path_sql = sql_string(str(temporary_path))
    delimiter_sql = sql_string(delimiter)

    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit='1GB'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        result = connection.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({source_sql})
            )
            TO {temporary_path_sql}
            (
                FORMAT CSV,
                HEADER,
                DELIMITER {delimiter_sql},
                COMPRESSION GZIP
            )
            """
        ).fetchone()
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    finally:
        connection.close()

    temporary_path.replace(destination_path)

    if result is None:
        raise RuntimeError("DuckDB did not return a row count")

    return int(result[0])


rows = parquet_to_delimited_duckdb(
    source="dataset-directory/",
    destination="output.tsv.gz",
    delimiter="\t",
)
print(f"Wrote {rows} rows")

# Use delimiter="," and a .csv.gz destination for CSV.
```

## Parquet to TSV or CSV with PyArrow

For wide datasets, iterating over each Parquet file separately avoids the extra buffering that can occur with a dataset-level scanner.

```python
from pathlib import Path
import gzip
from typing import BinaryIO

import pyarrow.csv as csv_writer
import pyarrow.parquet as pq


def open_output(path: Path) -> BinaryIO:
    if path.name.endswith(".gz"):
        return gzip.open(path, "wb", compresslevel=1)

    return path.open("wb")


def parquet_to_delimited_pyarrow(
    source: str | Path,
    destination: str | Path,
    delimiter: str = "\t",
    batch_size: int = 512,
) -> int:
    source_path = Path(source)
    destination_path = Path(destination)
    temporary_path = destination_path.with_name(
        destination_path.name + ".part"
    )

    if source_path.is_dir():
        parquet_files = sorted(source_path.glob("*.parquet"))
    else:
        parquet_files = [source_path]

    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found: {source_path}")

    rows_written = 0
    first_batch = True

    try:
        with open_output(temporary_path) as output:
            for parquet_path in parquet_files:
                parquet_file = pq.ParquetFile(parquet_path)

                for batch in parquet_file.iter_batches(
                    batch_size=batch_size,
                    use_threads=False,
                ):
                    options = csv_writer.WriteOptions(
                        include_header=first_batch,
                        delimiter=delimiter,
                        batch_size=batch_size,
                    )
                    csv_writer.write_csv(
                        batch,
                        output,
                        write_options=options,
                    )
                    first_batch = False
                    rows_written += batch.num_rows
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    temporary_path.replace(destination_path)
    return rows_written


rows = parquet_to_delimited_pyarrow(
    source="dataset-directory/",
    destination="output.tsv.gz",
    delimiter="\t",
)
print(f"Wrote {rows} rows")

# Use delimiter="," and a .csv.gz destination for CSV.
```

## DuckDB database to TSV or CSV

This version selects one table from a DuckDB `.db` file and opens the database read-only.

```python
from pathlib import Path

import duckdb


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def database_to_delimited(
    database: str | Path,
    table: str,
    destination: str | Path,
    delimiter: str = "\t",
) -> int:
    database_path = Path(database)
    destination_path = Path(destination)
    temporary_path = destination_path.with_name(
        destination_path.name + ".part"
    )

    temporary_path_sql = sql_string(str(temporary_path))
    table_sql = sql_identifier(table)
    delimiter_sql = sql_string(delimiter)

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        connection.execute("SET memory_limit='512MB'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        result = connection.execute(
            f"""
            COPY (
                SELECT *
                FROM {table_sql}
            )
            TO {temporary_path_sql}
            (
                FORMAT CSV,
                HEADER,
                DELIMITER {delimiter_sql},
                COMPRESSION GZIP
            )
            """
        ).fetchone()
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    finally:
        connection.close()

    temporary_path.replace(destination_path)

    if result is None:
        raise RuntimeError("DuckDB did not return a row count")

    return int(result[0])


rows = database_to_delimited(
    database="data.db",
    table="table_name",
    destination="output.tsv.gz",
    delimiter="\t",
)
print(f"Wrote {rows} rows")

# Use delimiter="," and a .csv.gz destination for CSV.
```
