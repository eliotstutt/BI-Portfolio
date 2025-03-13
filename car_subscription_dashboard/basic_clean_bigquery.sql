CREATE TABLE `personal-projects-453004.bi_portfolio.car_subscription_service` as (
    SELECT
        CAST(REGEXP_REPLACE(Customer_ID, r'[^0-9]', '') AS INT64) AS Customer_ID,
        Subscription_Type,
        Start_Date,
        End_Date,
        Vehicle_Utilization,
        Idle_Days,
        Weekly_Revenue_AUD,
        Churn,
        Swap_Frequency
    FROM `personal-projects-453004.bi_portfolio.car_subscription_service_staging`
)