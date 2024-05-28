# Data-Transformation-and-Update
This project involves transforming and updating data from a CSV file, applying transformations to certain columns, and then appending the transformed data into a Delta table in a Databricks environment.

# Prerequisites
- Python 3.x
- Pandas
- PySpark
- Delta Lake

# Configure Spark and Delta Lake
Ensure that you have a Spark and Delta Lake setup in your environment. This typically involves configuring Spark with Delta Lake libraries.

# Who
Who would benefit from this code?
- Data engineers and analysts who need to transform and update data in Delta Lake tables within a Databricks environment.

# What
What does this code do?
- Data Transformation: Converts multiple columns into single columns with categorical values based on specific conditions.
- Data Cleaning: Removes unnecessary columns after transformation.
- Data Filtering: Filters out rows based on date criteria.
- Data Integration: Merges and appends data into a Delta table for further analysis.lta tables.

# When
When would someone use this code?
- Regular Data Updates: When data is periodically updated and needs to be processed before analysis.
- Batch Processing: During scheduled batch processing jobs for large datasets.
- Data Migration: While migrating data from one storage solution to another.
- Preprocessing for Analytics: Before conducting exploratory data analysis or building dashboards.TL process.

# Where
Where can this code be applied?
- ETL Processes: Extract, Transform, Load processes in data pipelines.
- Data Warehousing: Storing and managing large datasets in a structured way.
- Reporting and Analytics: Preparing data for reporting tools like Tableau, PowerBI, etc.
- Machine Learning Pipelines: Preparing and cleaning data before feeding it into machine learning models

# Why
Why use this code?
- Efficiency: Automates repetitive tasks, reducing manual effort.
- Accuracy: Ensures consistent data transformation and cleaning.
- Scalability: Handles large datasets efficiently using Spark and Delta Lake.
- Flexibility: Easily adaptable to different datasets and transformation rules. Lake.

# Example Scenarios

# Scenario 1: ETL Process in Data Engineering
Benefit: Automates the extraction, transformation, and loading of data into a data warehouse.
- Who: Data Engineers
- What: Transforms and integrates data from CSV files into Delta Lake tables.
- When: During nightly ETL jobs to ensure data is ready for analysis the next day.
- Where: In a cloud-based data warehouse using Databricks.
- Why: Reduces manual data handling, ensures data integrity, and prepares data for downstream analytics.

# Scenario 2: Data Cleaning for Machine Learning
Benefit: Preprocesses raw data by cleaning and transforming it, making it suitable for machine learning models.
- Who: Data Scientists
- What: Converts categorical columns, filters data by date, and prepares datasets for modeling.
- When: Before each model training session to ensure the latest data is used. 
- Where: In a data science environment, using Databricks and Delta Lake.
- Why: Automates data preprocessing steps, ensuring consistency and saving time.

# Usage
- Step 1: Drop and Create Table in Delta Lake.

      # SECTION 1: Drop and Create Table in Delta Lake
      # Drop existing table if it exists
      %sql
      DROP TABLE IF EXISTS data_base_name.table1_name

      # Create a new table by selecting from a source table
      %sql
      CREATE TABLE data_base_name.table1_name AS SELECT * FROM data_base_name.source_table1_name

- Step 2: Load data from a CSV file, apply a transformation function to create new columns, and drop the original columns.

      # SECTION 2: Load and Transform Data

      # Load data from a CSV file
      df = pd.read_csv('file_path_for_data_source')

      # Transformation function to create new columns based on conditions
      def transform(row, col1, col2, col3, col4, col5):
          if row[col1] == 1:
              return 1
          elif row[col2] == 1:
              return 2
          elif row[col3] == 1:
              return 3
          elif row[col4] == 1:
              return 4
          elif row[col5] == 1:
              return 5
          else:
              return 0

      # Apply transformation to create new columns and drop original columns
      df['column_name_a'] = df.apply(transform, args=('col_a1_name', 'col_a2_name', 'col_a3_name', 'col_a4_name', 'col_a5_name'), axis=1)
      df = df.drop(['col_a1_name', 'col_a2_name', 'col_a3_name', 'col_a4_name', 'col_a5_name'], axis=1)

      df['column_name_b'] = df.apply(transform, args=('col_b1_name', 'col_b2_name', 'col_b3_name', 'col_b4_name', 'col_b5_name'), axis=1)
      df = df.drop(['col_b1_name', 'col_b2_name', 'col_b3_name', 'col_b4_name', 'col_b5_name'], axis=1)

      df['column_name_c'] = df.apply(transform, args=('col_c1_name', 'col_c2_name', 'col_c3_name', 'col_c4_name', 'col_c5_name'), axis=1)
      df = df.drop(['col_c1_name', 'col_c2_name', 'col_c3_name', 'col_c4_name', 'col_c5_name'], axis=1)

      df['column_name_d'] = df.apply(transform, args=('col_d1_name', 'col_d2_name', 'col_d3_name', 'col_d4_name', 'col_d5_name'), axis=1)
      df = df.drop(['col_d1_name', 'col_d2_name', 'col_d3_name', 'col_d4_name', 'col_d5_name'], axis=1)

      df['column_name_e'] = df.apply(transform, args=('col_e1_name', 'col_e2_name', 'col_e3_name', 'col_e4_name', 'col_e5_name'), axis=1)
      df = df.drop(['col_e1_name', 'col_e2_name', 'col_e3_name', 'col_e4_name', 'col_e5_name'], axis=1)

      df['column_name_f'] = df.apply(transform, args=('col_f1_name', 'col_f2_name', 'col_f3_name', 'col_f4_name', 'col_f5_name'), axis=1)
      df = df.drop(['col_f1_name', 'col_f2_name', 'col_f3_name', 'col_f4_name', 'col_f5_name'], axis=1)

- Step 3: Filter the DataFrame based on a date condition and split the data into two separate DataFrames for further processing.

      # SECTION 3: Filter and Split Data
      # Get minimum date from Delta table
      min_data_custom1 = spark.sql('select min(data_custom1) from data_base_name.table1_name').toPandas().iloc[0].values[0]
      df['DateTime'] = pd.to_datetime(df['DateTime'])
      min_data_custom1 = pd.to_datetime(min_data_custom1)

      # Filter data based on date condition
      df = df[df['DateTime'] < min_data_custom1]

      # Split data into two DataFrames for further processing
      df_data1 = df[['DateTime', 'ID1', 'column_name_f']]
      df_data1['data_custom2'] = df_data1['DateTime']
      df_data1[['column_name_b', 'column_name_c', 'column_name_d', 'column_name_e']] = None
      df_data1['data_type_of_status'] = 'Type1'
      df_data1 = df_data1[['DateTime', 'data_custom2', 'ID1', 'column_name_b', 'column_name_c', 'column_name_d', 'column_name_e', 'column_name_f', 'data_type_of_status']]
      df_data1.columns = list(spark.sql('select * from data_base_name.table1_name limit 5').toPandas().columns)

      df_non_data1 = df[['DateTime', 'ID1', 'column_name_a']]
      df_non_data1['data_custom2'] = df_non_data1['DateTime']
      df_non_data1[['column_name_b', 'column_name_c', 'column_name_d', 'column_name_e']] = df_data1[['column_name_b', 'column_name_c', 'column_name_d', 'column_name_e']]
      df_non_data1['data_type_of_status'] = 'Combo'
      df_non_data1 = df_non_data1[['DateTime', 'data_custom2', 'ID1', 'column_name_b', 'column_name_c', 'column_name_d', 'column_name_e', 'column_name_a', 'data_type_of_status']]
      df_non_data1.columns = list(spark.sql('select * from data_base_name.table1_name limit 5').toPandas().columns)

- Step 4: Convert the transformed DataFrames to Spark DataFrames with the appropriate schema and append them to the Delta table.

      # SECTION 4: Append Transformed Data to Delta Table
      # Define schema for data1
      data1_schema = StructType.fromJson({
          'fields': [
              {'metadata': {}, 'name': 'data_custom1', 'nullable': True, 'type': 'date'},
              {'metadata': {}, 'name': 'data_custom2', 'nullable': True, 'type': 'date'},
              {'metadata': {}, 'name': 'data_ID1', 'nullable': True, 'type': 'string'},
              {'metadata': {}, 'name': 'column_name_b', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_c', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_d', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_e', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_combo', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'data_type_of_status', 'nullable': True, 'type': 'string'}
          ],
          'type': 'struct'
      })

      # Convert df_data1 to Spark DataFrame and append to Delta table
      updates_data1 = spark.createDataFrame(data=df_data1, schema=data1_schema)
      updates_data1.write.format('delta').mode('append').insertInto("data_base_name.table1_name")

      # Define schema for non_data1
      non_data1_schema = StructType.fromJson({
          'fields': [
              {'metadata': {}, 'name': 'data_custom1', 'nullable': True, 'type': 'date'},
              {'metadata': {}, 'name': 'data_custom2', 'nullable': True, 'type': 'date'},
              {'metadata': {}, 'name': 'data_ID1', 'nullable': True, 'type': 'string'},
              {'metadata': {}, 'name': 'column_name_b', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_c', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_d', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_e', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'column_name_combo', 'nullable': True, 'type': 'integer'},
              {'metadata': {}, 'name': 'data_type_of_status', 'nullable': True, 'type': 'string'}
          ],
          'type': 'struct'
      })

      # Convert df_non_data1 to Spark DataFrame and append to Delta table
      updates_non_data1 = spark.createDataFrame(data=df_non_data1, schema=non_data1_schema)
      updates_non_data1.write.format('delta').mode('append').insertInto("data_base_name.table1_name")


