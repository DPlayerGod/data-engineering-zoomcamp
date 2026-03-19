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
            tip_amount DOUBLE,

            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'question6-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    sink_ddl = """
        CREATE TABLE tips_hourly (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            total_tip DOUBLE,
            PRIMARY KEY (window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'tips_hourly',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """

    insert_sql = """
        INSERT INTO tips_hourly
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tip
        FROM TABLE(
            TUMBLE(
                TABLE green_trips,
                DESCRIPTOR(event_timestamp),
                INTERVAL '1' HOUR
            )
        )
        GROUP BY
            window_start,
            window_end
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.execute_sql(insert_sql)


if __name__ == "__main__":
    main()