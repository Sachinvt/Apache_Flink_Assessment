from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, DataTypes
from pyflink.table.descriptors import Schema, FileSystem
from pyflink.table.udf import udf

# Define a UDF to format names
@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def format_name(name):
    return name.lower().title()

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Define schema for CSV file
schema = Schema().field("person_ID", DataTypes.INT()) \
    .field("name", DataTypes.STRING()) \
    .field("first", DataTypes.STRING()) \
    .field("last", DataTypes.STRING()) \
    .field("middle", DataTypes.STRING()) \
    .field("email", DataTypes.STRING()) \
    .field("phone", DataTypes.STRING()) \
    .field("fax", DataTypes.STRING()) \
    .field("title", DataTypes.STRING())

# Define file system connector
t_env.connect(FileSystem()
    .path('/path/to/csv/file')
    .with_format(Csv()
        .field('person_ID', DataTypes.INT())
        .field('name', DataTypes.STRING())
        .field('first', DataTypes.STRING())
        .field('last', DataTypes.STRING())
        .field('middle', DataTypes.STRING())
        .field('email', DataTypes.STRING())
        .field('phone', DataTypes.STRING())
        .field('fax', DataTypes.STRING())
        .field('title', DataTypes.STRING())
        .line_delimiter('\n')
        .field_delimiter(',')
        .ignore_parse_errors()
    )
    .with_schema(schema)
    .create_temporary_table('source_table')
)

# Define Paimon table sink
t_env.execute_sql("""
    CREATE TABLE paimon_table (
        person_ID INT,
        name STRING,
        first STRING,
        last STRING,
        middle STRING,
        email STRING,
        phone STRING,
        fax STRING,
        title STRING
    ) WITH (
        'connector' = 'paimon',
        'url' = 'paimon://localhost:9092',
        'table-name' = 'paimon_table',
        'format' = 'json'
    )
""")

# Define query to insert into Paimon table and format name
t_env.from_path('source_table') \
    .select("person_ID, format_name(name), first, last, middle, email, phone, fax, title") \
    .insert_into('paimon_table')

# Execute the streaming job
env.execute("Stream to Paimon Table")

