CREATE SCHEMA gold;
GO

CREATE SCHEMA mart;
GO

CREATE TABLE gold.customer_360
(
    customer_id              VARCHAR(30) NOT NULL,
    first_name               VARCHAR(100),
    last_name                VARCHAR(100),
    gender                   VARCHAR(10),
    age                      INT,
    province                 VARCHAR(20),
    customer_segment         VARCHAR(50),
    risk_tier                VARCHAR(30),
    credit_score             INT,
    credit_score_band        VARCHAR(30),
    account_count            INT,
    total_account_balance    DECIMAL(18,2),
    card_count               INT,
    total_credit_limit       DECIMAL(18,2),
    loan_count               INT,
    total_loan_balance       DECIMAL(18,2),
    total_products           INT
)
WITH
(
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE gold.daily_transaction_summary
(
    transaction_date             DATE,
    transaction_count            BIGINT,
    total_transaction_amount     DECIMAL(18,2),
    avg_rule_fraud_score         DECIMAL(10,2),
    confirmed_fraud_count        BIGINT,
    ml_predicted_fraud_count     BIGINT
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE gold.fraud_risk_summary
(
    fraud_rule_band              VARCHAR(30),
    risk_tier                    VARCHAR(30),
    merchant_category            VARCHAR(100),
    transaction_count            BIGINT,
    total_transaction_amount     DECIMAL(18,2),
    confirmed_fraud_count        BIGINT,
    avg_ml_fraud_probability     FLOAT,
    avg_rule_score               DECIMAL(10,2)
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
);
GO

CREATE TABLE mart.customer_360_mart
WITH
(
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM gold.customer_360;
GO

CREATE TABLE mart.fraud_analytics_mart
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * FROM gold.fraud_risk_summary;
GO
