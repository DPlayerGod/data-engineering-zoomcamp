from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    # =========================
    # 1. ENVIRONMENT
    # =========================
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # IMPORTANT

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # =========================
    # 2. SOURCE (KAFKA)
    # =========================
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            PULocationID INT,

            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'test-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    t_env.execute_sql(source_ddl)

    # =========================
    # 3. SINK (POSTGRES)
    # =========================
    sink_ddl = """
        CREATE TABLE trips_windowed (
            window_start TIMESTAMP,
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trips_windowed',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """

    t_env.execute_sql(sink_ddl)

    # =========================
    # 4. QUERY (TUMBLING WINDOW)
    # =========================
    insert_sql = """
        INSERT INTO trips_windowed
        SELECT
            window_start,
            PULocationID,
            COUNT(*) as num_trips
        FROM TABLE(
            TUMBLE(
                TABLE green_trips,
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTES
            )
        )
        GROUP BY
            window_start,
            PULocationID
    """

    t_env.execute_sql(insert_sql)


if __name__ == "__main__":
    main()