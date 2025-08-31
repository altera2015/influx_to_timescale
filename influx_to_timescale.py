#!/usr/bin/env python3
"""
InfluxDB 2.7 to TimescaleDB Migration Script

This script migrates data from InfluxDB 2.7 to TimescaleDB by:
1. Reading data from InfluxDB using Flux queries
2. Creating corresponding TimescaleDB tables with hypertables
3. Inserting data in batches for optimal performance
"""

import os
import sys
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import argparse

try:
    import influxdb_client
    from influxdb_client.client.query_api import QueryApi
    import psycopg2
    import psycopg2.extras
    import psycopg2.extensions
    import pandas as pd
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Install with: pip install influxdb-client psycopg2-binary pandas")
    sys.exit(1)


class InfluxToTimescaleMigrator:
    def __init__(self, influx_config: Dict[str, str], timescale_config: Dict[str, str],
                 chunk_days: int = 1, state_file: str = "migration_state.json"):
        self.influx_config = influx_config
        self.timescale_config = timescale_config
        self.chunk_days = chunk_days
        self.state_file = state_file
        self.logger = self._setup_logger()

        # Initialize connections
        self.influx_client = None
        self.timescale_conn = None

        # Load migration state
        self.migration_state = self._load_migration_state()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _load_migration_state(self) -> Dict[str, Any]:
        """Load migration state from file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load migration state: {e}")
        return {}

    def _save_migration_state(self):
        """Save migration state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.migration_state, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save migration state: {e}")

    def _get_data_time_range(self, bucket: str, measurement: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the earliest and latest timestamps for a measurement"""
        try:
            query_api = self.influx_client.query_api()

            # Get earliest timestamp
            earliest_query = f'''
            from(bucket: "{bucket}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> first()
              |> keep(columns: ["_time"])
            '''

            # Get latest timestamp
            latest_query = f'''
            from(bucket: "{bucket}")
              |> range(start: 0)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> last()
              |> keep(columns: ["_time"])
            '''

            earliest_result = query_api.query(earliest_query)
            latest_result = query_api.query(latest_query)

            earliest_time = None
            latest_time = None

            for table in earliest_result:
                for record in table.records:
                    earliest_time = record.get_time()
                    break

            for table in latest_result:
                for record in table.records:
                    latest_time = record.get_time()
                    break

            return earliest_time, latest_time

        except Exception as e:
            self.logger.error(f"Failed to get time range for {measurement}: {e}")
            return None, None

    def _generate_time_chunks(self, start_time: datetime, end_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Generate time chunks for migration"""
        chunks = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + timedelta(days=self.chunk_days), end_time)
            chunks.append((current_start, current_end))
            current_start = current_end

        return chunks

    def _convert_flux_result_to_dataframe(self, query_result) -> pd.DataFrame:
        """Convert Flux query result to pandas DataFrame"""
        rows = []

        for table in query_result:
            for record in table.records:
                row_data = {
                    '_time': record.get_time(),
                    '_measurement': record.get_measurement(),
                    '_field': record.get_field(),
                    '_value': record.get_value()
                }

                # Add tags
                for key, value in record.values.items():
                    if not key.startswith('_') and key not in ['result', 'table']:
                        row_data[key] = value

                rows.append(row_data)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Pivot the data to match expected format
        if '_field' in df.columns and '_value' in df.columns:
            # Group by time and other non-field columns, then pivot
            id_cols = [col for col in df.columns if col not in ['_field', '_value']]
            df_pivoted = df.pivot_table(
                index=id_cols,
                columns='_field',
                values='_value',
                aggfunc='first'
            ).reset_index()

            # Flatten column names
            df_pivoted.columns.name = None
            return df_pivoted

        return df

    def connect_influxdb(self):
        """Connect to InfluxDB"""
        try:
            self.influx_client = influxdb_client.InfluxDBClient(
                url=self.influx_config['url'],
                token=self.influx_config['token'],
                org=self.influx_config['org'],
                timeout=10000000000
            )
            # Test connection
            health = self.influx_client.health()
            if health.status != "pass":
                raise Exception(f"InfluxDB health check failed: {health.status}")
            self.logger.info("Connected to InfluxDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to InfluxDB: {e}")
            raise

    def connect_timescaledb(self):
        """Connect to TimescaleDB"""
        try:
            self.timescale_conn = psycopg2.connect(**self.timescale_config)
            self.timescale_conn.autocommit = False
            self.logger.info("Connected to TimescaleDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise

    def get_measurements(self, bucket: str) -> List[str]:
        """Get all measurements from InfluxDB bucket"""
        try:
            query_api = self.influx_client.query_api()

            # Flux query to get all measurements
            flux_query = f'''
            import "influxdata/influxdb/schema"

            schema.measurements(bucket: "{bucket}")
            '''

            result = query_api.query(flux_query)
            measurements = []

            for table in result:
                for record in table.records:
                    measurements.append(record.get_value())

            self.logger.info(f"Found {len(measurements)} measurements in bucket '{bucket}'")
            return measurements

        except Exception as e:
            self.logger.error(f"Failed to get measurements: {e}")
            raise

    def create_timescale_table(self, table_name: str, fields: Dict[str, str]):
        """Create TimescaleDB hypertable"""
        try:
            cursor = self.timescale_conn.cursor()

            # Sanitize table name
            table_name = table_name.replace('-', '_').replace(' ', '_')

            # Build column definitions
            columns = ['time TIMESTAMPTZ NOT NULL']

            for field_name, field_type in fields.items():
                # Map InfluxDB types to PostgreSQL types
                pg_type = self._map_influx_type_to_postgres(field_type)
                columns.append(f"{field_name} {pg_type}")

            # Add tags as text columns (assuming all tags are strings)
            columns.append("tags JSONB")

            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            );
            '''

            cursor.execute(create_table_sql)

            # Create hypertable
            cursor.execute(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")

            self.timescale_conn.commit()
            cursor.close()

            self.logger.info(f"Created hypertable: {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            self.timescale_conn.rollback()
            raise

    def _map_influx_type_to_postgres(self, influx_type: str) -> str:
        """Map InfluxDB field types to PostgreSQL types"""
        type_mapping = {
            'float': 'DOUBLE PRECISION',
            'integer': 'BIGINT',
            'string': 'TEXT',
            'boolean': 'BOOLEAN',
            'unsignedInteger': 'BIGINT'
        }
        return type_mapping.get(influx_type, 'TEXT')

    def get_field_schema(self, bucket: str, measurement: str) -> Dict[str, str]:
        """Get field schema from InfluxDB measurement"""
        try:
            query_api = self.influx_client.query_api()

            flux_query = f'''
            import "influxdata/influxdb/schema"

            schema.fieldKeys(
                bucket: "{bucket}",
                predicate: (r) => r._measurement == "{measurement}"
            )
            '''

            result = query_api.query(flux_query)
            fields = {}

            # Get field types by sampling data
            sample_query = f'''
            from(bucket: "{bucket}")
              |> range(start: -30d)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> limit(n: 1)
            '''

            sample_result = query_api.query(sample_query)

            for table in sample_result:
                for record in table.records:
                    field_name = record.get_field()
                    value = record.get_value()

                    # Determine type based on value
                    if isinstance(value, bool):
                        fields[field_name] = 'boolean'
                    elif isinstance(value, int):
                        fields[field_name] = 'integer'
                    elif isinstance(value, float):
                        fields[field_name] = 'float'
                    else:
                        fields[field_name] = 'string'

            return fields

        except Exception as e:
            self.logger.error(f"Failed to get field schema for {measurement}: {e}")
            return {}

    def migrate_measurement_chunk(self, bucket: str, measurement: str, start_time: datetime,
                                 end_time: datetime, batch_size: int = 10000):
        """Migrate a time chunk of a measurement from InfluxDB to TimescaleDB"""
        try:
            chunk_key = f"{measurement}_{start_time.isoformat()}_{end_time.isoformat()}"

            # Check if this chunk was already migrated
            measurement_state = self.migration_state.get(measurement, {})
            if chunk_key in measurement_state.get('completed_chunks', []):
                self.logger.info(f"Chunk already migrated: {chunk_key}")
                return 0

            self.logger.info(f"Migrating chunk: {measurement} from {start_time} to {end_time}")

            # Get field schema (only once per measurement)
            fields = self.get_field_schema(bucket, measurement)
            if not fields:
                self.logger.warning(f"No fields found for measurement {measurement}, skipping")
                return 0

            # Create TimescaleDB table (if not exists)
            table_name = measurement.replace('-', '_').replace(' ', '_')
            self.create_timescale_table(table_name, fields)

            # Query chunk data from InfluxDB
            query_api = self.influx_client.query_api()

            flux_query = f'''
            from(bucket: "{bucket}")
              |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            '''

            # Execute query and get results as DataFrame
            try:
                result = query_api.query_data_frame(flux_query)

                # Handle case where query_data_frame returns a list of DataFrames
                if isinstance(result, list):
                    if len(result) == 0:
                        self.logger.info(f"No data found for chunk {chunk_key}")
                        self._mark_chunk_completed(measurement, chunk_key)
                        return 0
                    # Concatenate all DataFrames in the list
                    df = pd.concat(result, ignore_index=True)
                else:
                    df = result

                if df.empty:
                    self.logger.info(f"No data found for chunk {chunk_key}")
                    # Mark chunk as completed even if empty
                    self._mark_chunk_completed(measurement, chunk_key)
                    return 0

            except Exception as e:
                self.logger.error(f"Failed to execute query for chunk {chunk_key}: {e}")
                # Try alternative query method
                try:
                    query_result = query_api.query(flux_query)
                    if not query_result:
                        self.logger.info(f"No data found for chunk {chunk_key}")
                        self._mark_chunk_completed(measurement, chunk_key)
                        return 0

                    # Convert query result to DataFrame manually
                    df = self._convert_flux_result_to_dataframe(query_result)

                    if df.empty:
                        self.logger.info(f"No data found for chunk {chunk_key}")
                        self._mark_chunk_completed(measurement, chunk_key)
                        return 0

                except Exception as e2:
                    self.logger.error(f"Both query methods failed for chunk {chunk_key}: {e2}")
                    raise

            total_rows = len(df)
            self.logger.info(f"Found {total_rows} rows in chunk")

            # Process in batches
            cursor = self.timescale_conn.cursor()
            migrated_rows = 0

            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i+batch_size]
                self._insert_batch(cursor, table_name, batch_df, fields)
                migrated_rows += len(batch_df)

                self.logger.info(f"Chunk progress: {min(i+batch_size, total_rows)}/{total_rows} rows")

            self.timescale_conn.commit()
            cursor.close()

            # Mark chunk as completed
            self._mark_chunk_completed(measurement, chunk_key)

            self.logger.info(f"Successfully migrated chunk: {chunk_key} ({migrated_rows} rows)")
            return migrated_rows

        except Exception as e:
            self.logger.error(f"Failed to migrate chunk {chunk_key}: {e}")
            self.timescale_conn.rollback()
            raise

    def migrate_measurement(self, bucket: str, measurement: str, batch_size: int = 10000):
        """Migrate a single measurement from InfluxDB to TimescaleDB using time chunks"""
        try:
            self.logger.info(f"Starting migration of measurement: {measurement}")

            # Get data time range
            start_time, end_time = self._get_data_time_range(bucket, measurement)
            if not start_time or not end_time:
                self.logger.warning(f"No data found for measurement {measurement}")
                return

            self.logger.info(f"Data range: {start_time} to {end_time}")

            # Generate time chunks
            chunks = self._generate_time_chunks(start_time, end_time)
            total_chunks = len(chunks)

            self.logger.info(f"Will migrate {total_chunks} chunks of {self.chunk_days} day(s) each")

            total_migrated = 0
            completed_chunks = 0

            for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
                try:
                    migrated_rows = self.migrate_measurement_chunk(
                        bucket, measurement, chunk_start, chunk_end, batch_size
                    )
                    total_migrated += migrated_rows
                    completed_chunks += 1

                    self.logger.info(f"Completed chunk {i}/{total_chunks} - Total migrated: {total_migrated} rows")

                except Exception as e:
                    self.logger.error(f"Failed to migrate chunk {i}/{total_chunks}: {e}")
                    # Continue with next chunk
                    continue

            self.logger.info(f"Successfully migrated measurement {measurement}: {completed_chunks}/{total_chunks} chunks, {total_migrated} total rows")

        except Exception as e:
            self.logger.error(f"Failed to migrate measurement {measurement}: {e}")
            raise

    def _mark_chunk_completed(self, measurement: str, chunk_key: str):
        """Mark a chunk as completed in migration state"""
        if measurement not in self.migration_state:
            self.migration_state[measurement] = {'completed_chunks': []}

        if 'completed_chunks' not in self.migration_state[measurement]:
            self.migration_state[measurement]['completed_chunks'] = []

        if chunk_key not in self.migration_state[measurement]['completed_chunks']:
            self.migration_state[measurement]['completed_chunks'].append(chunk_key)
            self._save_migration_state()

    def _insert_batch(self, cursor, table_name: str, df: pd.DataFrame, fields: Dict[str, str]):
        """Insert a batch of data into TimescaleDB"""
        try:
            # Prepare data for insertion
            rows = []

            for _, row in df.iterrows():
                # Extract timestamp
                timestamp = row['_time']

                # Extract field values
                field_values = []
                for field_name in fields.keys():
                    value = row.get(field_name)
                    if pd.isna(value):
                        value = None
                    field_values.append(value)

                # Extract tags as JSON
                tags = {}
                for col in df.columns:
                    if col not in ['_time', '_measurement'] + list(fields.keys()):
                        if not col.startswith('_'):  # Exclude InfluxDB system columns
                            tags[col] = row[col]

                # Convert tags dict to JSON string for JSONB column
                tags_json = json.dumps(tags) if tags else '{}'
                rows.append([timestamp] + field_values + [tags_json])

            # Build insert query
            field_names = list(fields.keys())
            columns = ['time'] + field_names + ['tags']
            placeholders = ', '.join(['%s'] * len(columns))

            insert_sql = f'''
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            '''

            cursor.executemany(insert_sql, rows)

        except Exception as e:
            self.logger.error(f"Failed to insert batch: {e}")
            raise

    def migrate_bucket(self, bucket: str, measurements: Optional[List[str]] = None):
        """Migrate entire bucket or specific measurements"""
        try:
            self.connect_influxdb()
            self.connect_timescaledb()

            if measurements is None:
                measurements = self.get_measurements(bucket)

            self.logger.info(f"Starting migration of {len(measurements)} measurements")

            for measurement in measurements:
                try:
                    self.migrate_measurement(bucket, measurement)
                except Exception as e:
                    self.logger.error(f"Failed to migrate {measurement}: {e}")
                    continue

            self.logger.info("Migration completed")

        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise
        finally:
            self._close_connections()

    def _close_connections(self):
        """Close database connections"""
        if self.influx_client:
            self.influx_client.close()
        if self.timescale_conn:
            self.timescale_conn.close()


def main():
    parser = argparse.ArgumentParser(description='Migrate data from InfluxDB 2.7 to TimescaleDB')
    parser.add_argument('--influx-url', required=True, help='InfluxDB URL')
    parser.add_argument('--influx-token', required=True, help='InfluxDB token')
    parser.add_argument('--influx-org', required=True, help='InfluxDB organization')
    parser.add_argument('--influx-bucket', required=True, help='InfluxDB bucket to migrate')

    parser.add_argument('--ts-host', required=True, help='TimescaleDB host')
    parser.add_argument('--ts-port', default=5432, type=int, help='TimescaleDB port')
    parser.add_argument('--ts-database', required=True, help='TimescaleDB database')
    parser.add_argument('--ts-user', required=True, help='TimescaleDB username')
    parser.add_argument('--ts-password', required=True, help='TimescaleDB password')

    parser.add_argument('--measurements', nargs='*', help='Specific measurements to migrate (all if not specified)')
    parser.add_argument('--chunk-days', default=1, type=int, help='Number of days per time chunk (default: 1)')
    parser.add_argument('--state-file', default='migration_state.json', help='File to store migration progress (default: migration_state.json)')
    parser.add_argument('--resume', action='store_true', help='Resume from previous migration state')

    args = parser.parse_args()

    # Configuration
    influx_config = {
        'url': args.influx_url,
        'token': args.influx_token,
        'org': args.influx_org
    }

    timescale_config = {
        'host': args.ts_host,
        'port': args.ts_port,
        'database': args.ts_database,
        'user': args.ts_user,
        'password': args.ts_password
    }

    # Create migrator and run migration
    migrator = InfluxToTimescaleMigrator(
        influx_config,
        timescale_config,
        chunk_days=args.chunk_days,
        state_file=args.state_file
    )
    migrator.migrate_bucket(args.influx_bucket, args.measurements)


if __name__ == '__main__':
    main()