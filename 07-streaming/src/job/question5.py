from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

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
            'properties.group.id' = 'question5-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    sink_ddl = """
        CREATE TABLE trips_sessionized (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trips_sessionized',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """

    insert_sql = """
        INSERT INTO trips_sessionized
        SELECT
            SESSION_START(event_timestamp, INTERVAL '5' MINUTES) AS window_start,
            SESSION_END(event_timestamp, INTERVAL '5' MINUTES) AS window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        GROUP BY
            SESSION(event_timestamp, INTERVAL '5' MINUTES),
            PULocationID
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.execute_sql(insert_sql)


if __name__ == "__main__":
    main()