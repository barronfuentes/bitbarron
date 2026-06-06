-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW sample_zones_college_scorecard_pipeline AS
SELECT
    pickup_zip,
    SUM(fare_amount) AS total_fare
FROM sample_trips_college_scorecard_pipeline
GROUP BY pickup_zip
