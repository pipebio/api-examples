# Data Conversion Examples

These examples convert local Parquet datasets and DuckDB databases to TSV or CSV without loading the complete table into memory.

Install the optional dependencies:

```bash
python -m pip install pyarrow duckdb
```

The examples write gzip-compressed output by default. Remove `COMPRESSION GZIP` from the DuckDB examples, or use a destination without `.gz` for the PyArrow example, when uncompressed output is required.

PipeBio TSV exports use an unquoted dialect: no quote character, no escape character, and blank fields for both `NULL` and empty strings. Tabs, newlines, and carriage returns inside string values are replaced with spaces so row alignment is preserved. Nested columns containing strings are serialized to text before the same sanitization is applied. CSV output keeps standard quoting.

PipeBio's TSV and CSV exports additionally apply spreadsheet-injection sanitization, so their cell values are not always byte-identical to the stored data: a value starting with `=`, `+`, `@`, or a non-numeric `-` is prefixed with an apostrophe, and a whitespace-only value becomes empty. Parquet and DuckDB exports are exempt. The examples below reproduce the unquoted TSV dialect but deliberately not this rewriting, so their output preserves such values verbatim. Add the apostrophe prefix yourself when the goal is to match a PipeBio TSV export byte-for-byte, or when the result will be opened in a spreadsheet.

## Parquet to TSV or CSV with DuckDB

This version accepts either a single Parquet file or a directory containing Parquet shards.

```python
from pathlib import Path

import duckdb


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_tsv_safe_sql(connection: duckdb.DuckDBPyConnection, local_sql: str) -> str:
    """Sanitize VARCHAR columns for PipeBio's unquoted TSV dialect."""
    tab = "chr(9)"
    line_feed = "chr(10)"
    carriage_return = "chr(13)"
    columns_info = connection.execute(f"DESCRIBE ({local_sql})").fetchall()
    sanitized_cols = []
    for col_name, col_type, *_ in columns_info:
        escaped_name = col_name.replace('"', '""')
        upper_type = col_type.upper()
        if "VARCHAR" in upper_type:
            value_expression = f'"{escaped_name}"'
            if upper_type != "VARCHAR":
                value_expression = f'CAST({value_expression} AS VARCHAR)'
            sanitized_cols.append(
                f"REPLACE(REPLACE(REPLACE({value_expression}, {tab}, ' '), "
                f"{line_feed}, ' '), {carriage_return}, ' ') "
                f'AS "{escaped_name}"'
            )
        else:
            sanitized_cols.append(f'"{escaped_name}"')
    return f"SELECT {', '.join(sanitized_cols)} FROM ({local_sql})"


def duckdb_copy_options(delimiter: str) -> str:
    delimiter_sql = sql_string(delimiter)
    if delimiter == "\t":
        return f"""
            FORMAT CSV,
            HEADER,
            DELIMITER {delimiter_sql},
            QUOTE '',
            ESCAPE '',
            COMPRESSION GZIP
        """
    return f"""
        FORMAT CSV,
        HEADER,
        DELIMITER {delimiter_sql},
        COMPRESSION GZIP
    """


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
    inner_sql = f"SELECT * FROM read_parquet({source_sql})"

    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit='1GB'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        if delimiter == "\t":
            inner_sql = build_tsv_safe_sql(connection, inner_sql)
        result = connection.execute(
            f"""
            COPY (
                {inner_sql}
            )
            TO {temporary_path_sql}
            (
                {duckdb_copy_options(delimiter)}
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
import csv
import gzip
import io
from typing import BinaryIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv_writer
import pyarrow.parquet as pq


def open_output(path: Path) -> BinaryIO:
    if path.name.removesuffix(".part").endswith(".gz"):
        return gzip.open(path, "wb", compresslevel=1)

    return path.open("wb")


def sanitize_batch_for_unquoted_tsv(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Replace tabs and line breaks in string columns for unquoted TSV."""
    columns = []
    for field in batch.schema:
        column = batch.column(field.name)
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            for char in ("\t", "\n", "\r"):
                column = pc.replace_substring(column, char, " ")
        columns.append(column)
    return pa.RecordBatch.from_arrays(columns, names=batch.schema.names)


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
                    if delimiter == "\t":
                        batch = sanitize_batch_for_unquoted_tsv(batch)
                        chunk_as_df = batch.to_pandas(integer_object_nulls=True)
                        chunk_as_df.columns = [
                            str(column)
                            .replace("\t", " ")
                            .replace("\n", " ")
                            .replace("\r", " ")
                            for column in chunk_as_df.columns
                        ]
                        buffer = io.StringIO()
                        chunk_as_df.to_csv(
                            buffer,
                            sep="\t",
                            header=first_batch,
                            index=False,
                            quoting=csv.QUOTE_NONE,
                            quotechar=None,
                        )
                        output.write(buffer.getvalue().encode("utf-8"))
                    else:
                        options = csv_writer.WriteOptions(
                            include_header=first_batch,
                            delimiter=delimiter,
                            batch_size=batch_size,
                            quoting_style="needed",
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


def build_tsv_safe_sql(connection: duckdb.DuckDBPyConnection, local_sql: str) -> str:
    """Sanitize VARCHAR columns for PipeBio's unquoted TSV dialect."""
    tab = "chr(9)"
    line_feed = "chr(10)"
    carriage_return = "chr(13)"
    columns_info = connection.execute(f"DESCRIBE ({local_sql})").fetchall()
    sanitized_cols = []
    for col_name, col_type, *_ in columns_info:
        escaped_name = col_name.replace('"', '""')
        upper_type = col_type.upper()
        if "VARCHAR" in upper_type:
            value_expression = f'"{escaped_name}"'
            if upper_type != "VARCHAR":
                value_expression = f'CAST({value_expression} AS VARCHAR)'
            sanitized_cols.append(
                f"REPLACE(REPLACE(REPLACE({value_expression}, {tab}, ' '), "
                f"{line_feed}, ' '), {carriage_return}, ' ') "
                f'AS "{escaped_name}"'
            )
        else:
            sanitized_cols.append(f'"{escaped_name}"')
    return f"SELECT {', '.join(sanitized_cols)} FROM ({local_sql})"


def duckdb_copy_options(delimiter: str) -> str:
    delimiter_sql = sql_string(delimiter)
    if delimiter == "\t":
        return f"""
            FORMAT CSV,
            HEADER,
            DELIMITER {delimiter_sql},
            QUOTE '',
            ESCAPE '',
            COMPRESSION GZIP
        """
    return f"""
        FORMAT CSV,
        HEADER,
        DELIMITER {delimiter_sql},
        COMPRESSION GZIP
    """


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
    inner_sql = f"SELECT * FROM {table_sql}"

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        connection.execute("SET memory_limit='512MB'")
        connection.execute("SET threads=1")
        connection.execute("SET preserve_insertion_order=false")
        if delimiter == "\t":
            inner_sql = build_tsv_safe_sql(connection, inner_sql)
        result = connection.execute(
            f"""
            COPY (
                {inner_sql}
            )
            TO {temporary_path_sql}
            (
                {duckdb_copy_options(delimiter)}
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
