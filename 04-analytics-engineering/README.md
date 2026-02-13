# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](../../../04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---
> **Note:** I use Dbt Core + Big Query 

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)


  <details>
  <summary><strong>Answer</strong></summary>

  - `int_trips_unioned` only
  </details>
---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

  <details>
  <summary><strong>Answer</strong></summary>

  - dbt will fail the test, returning a non-zero exit code
  </details>
---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- 12,184
- 15,421

  <details>
  <summary><strong>Answer</strong></summary>

  - 12,184
  </details>

  <details>
  <summary><strong>Solution</strong></summary>

  ```sql 
  SELECT COUNT(*) FROM `kestra-486406.raw.fct_monthly_zone_revenue`;
  ```
  </details>
---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

  <details>
  <summary><strong>Answer</strong></summary>

  - East Harlem North
  </details>

  <details>
  <summary><strong>Solution</strong></summary>

  ```sql 
  SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS revenue 
  FROM `kestra-486406.raw.fct_monthly_zone_revenue`
  WHERE service_type = 'Green'
    AND EXTRACT(YEAR FROM DATE(revenue_month)) = 2020
  GROUP BY pickup_zone
  ORDER BY revenue DESC
  LIMIT 1;
  ```
  </details>
---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- 384,624
- 421,509


  <details>
  <summary><strong>Answer</strong></summary>

  - 384,624
  </details>

  <details>
  <summary><strong>Solution</strong></summary>

  ```sql 
  SELECT
    SUM(total_monthly_trips) AS total
  FROM `kestra-486406.raw.fct_monthly_zone_revenue`
  WHERE service_type = 'Green'
    AND EXTRACT(YEAR FROM DATE(revenue_month)) = 2019
    AND EXTRACT(MONTH FROM DATE(revenue_month)) = 10;
  ```
  </details>
---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- 43,244,693
- 22,998,722
- 44,112,187

  <details>
  <summary><strong>Answer</strong></summary>

  - 43,244,693  
  </details>

  <details>
  <summary><strong>Solution</strong></summary>

  ```sql 
  SELECT COUNT(*) from `kestra-486406.raw.stg_fhv_tripdata`;
  ```
  </details>
---
