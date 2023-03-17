# snowflake-airflow-emr-lambda-superset

This architecture mimics some companies’ ETL processes. For instance, this example illustrates where data files are sent directly to the company’s data lake (s3 bucket). For example, every day at 2am, data is dumped from the transactional database to s3 bucket(s), then an EMR cluster will execute the processed data. The processed data will then be stored in an output s3 bucket for the next day's usage. The processed data can then be used to power certain BI tools for analytical or statistical reporting.

This architecture consists of the following:

![alt text](https://s3.amazonaws.com/weclouddata/images/data_engineer/architecture1.png)

### Detailed Steps

1. Load raw data into a transactional database (Snowflake).
    - Schedule snowflake stage and tasks to dump data into s3 input bucket, everyday at 2am EST.
2. Cloudwatch + Lambda
    - Cloudwatch is used to trigger lambda. We set cloudwatch triggers at 3am & 3:30am EST.
    - Lambda will do several tasks.
        * Check if ‘todays’ files are ready.
        * If ready, send a signal / request to Airflow to start EMR.
        * If not ready, it will send an automated email informing the recipient that the files are not ready.
3. Airflow
    - When Airflow receives the request from lambda, it will unpack the request. The request will contain parameters for EMR.
4. EMR
    - EMR will be triggered by Airflow. EMR will use the files in the s3 input bucket where the raw data is dumped.
    - Once EMR finds the raw data files, it will perform the following tasks:
        * Read data from S3.
        * Do data transformation to generate a dataframe to meet the following metrics:
            1. total sales quantity of a product : Sum(sales_qty)
            2. total sales amount of a product : Sum(sales_amt)
            3. average sales Price: Sum(sales_amt)/Sum(sales_qty)
            4. stock level by then end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
            5. store on Order level by then end of the week: ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week)
            6. total cost of the week: Sum(cost_amt)
            7. the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
            8. total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)
            9. potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
            10. no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
            11. low Stock Instances: Calculate how many times of Low_Stock_Flg in a week
            13. no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week
            14. how many weeks the on hand stock can supply: (stock_on_hand_qty at the end of the week) / sum(sales_qty)
        * After the above transformation, EMR will save the final dataframe as a parquet file to a new output s3 bucket.
        * EMR will also copy the files of store, product, and calendar (previously dumped to s3 input bucket) to the new s3 output bucket.
5. Athena & Glue
    - Athena & Glue will be connected to the s3 output bucket for storing final fact and dimension tables.
6. BI tools (Superset)
    - Superset will be connected to Athena to generate visualized reports for the fact and dimension tables.




