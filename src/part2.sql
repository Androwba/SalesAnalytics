DROP MATERIALIZED VIEW IF EXISTS Customers;
CREATE MATERIALIZED VIEW Customers AS
WITH Subquery AS (
    SELECT
        p.Customer_ID,
        COALESCE(SUM(t.Transaction_Summ) / NULLIF(COUNT(t.Transaction_ID), 0), 0) AS Customer_Average_Check,
        CASE
            WHEN PERCENT_RANK() OVER (ORDER BY COALESCE(SUM(t.Transaction_Summ) / NULLIF(COUNT(t.Transaction_ID), 0), 0) DESC) <= 0.25 THEN 'High'
            WHEN PERCENT_RANK() OVER (ORDER BY COALESCE(SUM(t.Transaction_Summ) / NULLIF(COUNT(t.Transaction_ID), 0), 0) DESC) <= 0.10 THEN 'Medium'
            ELSE 'Low'
        END AS Customer_Average_Check_Segment,
        CASE
            WHEN COUNT(t.Transaction_ID) <= 1 THEN 0
            ELSE ROUND(AVG(interval_days), 2)
        END AS Customer_Frequency,
        CASE
            WHEN PERCENT_RANK() OVER (ORDER BY ROUND(AVG(interval_days), 2)) <= 0.10 THEN 'Often'
            WHEN PERCENT_RANK() OVER (ORDER BY ROUND(AVG(interval_days), 2)) <= 0.25 THEN 'Occasionally'
            ELSE 'Rarely'
        END AS Customer_Frequency_Segment,
        COALESCE(ROUND(EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, MAX(t.Transaction_DateTime))) / (60 * 60 * 24), 2), 0) AS Customer_Inactive_Period,
        CASE
            WHEN COUNT(t.Transaction_ID) <= 1 THEN 0
            ELSE ROUND(EXTRACT(EPOCH FROM AGE(CURRENT_TIMESTAMP, MAX(t.Transaction_DateTime))) / (60 * 60 * 24), 2) / ROUND(AVG(interval_days), 2)
        END AS Customer_Churn_Rate
    FROM
        Personal_Information p
    LEFT JOIN
        Cards c ON p.Customer_ID = c.Customer_ID
    LEFT JOIN
        Transactions t ON c.Customer_Card_ID = t.Customer_Card_ID
    LEFT JOIN (
        SELECT
            t1.Customer_Card_ID,
            EXTRACT(EPOCH FROM AGE(MAX(t1.Transaction_DateTime), MIN(t1.Transaction_DateTime))) / NULLIF(COUNT(t1.Transaction_ID) - 1, 0) / (60 * 60 * 24) AS interval_days
        FROM
            Transactions t1
        GROUP BY
            t1.Customer_Card_ID
    ) AS avg_interval ON c.Customer_Card_ID = avg_interval.Customer_Card_ID
    GROUP BY
        p.Customer_ID
),
Subquery_With_Churn_Segment AS (
    SELECT
        *,
        CASE
            WHEN Customer_Churn_Rate >= 0 AND Customer_Churn_Rate <= 2 THEN 'Low'
            WHEN Customer_Churn_Rate > 2 AND Customer_Churn_Rate <= 5 THEN 'Medium'
            ELSE 'High'
        END AS Customer_Churn_Segment
    FROM Subquery
),
Recent_Transactions AS (
    SELECT
        t.Customer_Card_ID,
        t.Transaction_Store_ID,
        t.Transaction_DateTime,
        ROW_NUMBER() OVER (PARTITION BY t.Customer_Card_ID ORDER BY t.Transaction_DateTime DESC) AS rn
    FROM Transactions t
),
Store_Share AS (
    SELECT
        rt.Customer_Card_ID,
        rt.Transaction_Store_ID,
        COUNT(DISTINCT rt.Transaction_Store_ID) AS Store_Transaction_Count,
        COUNT(DISTINCT rt.Transaction_Store_ID)::NUMERIC / tc.Total_Customer_Transactions AS Store_Transaction_Share
    FROM Recent_Transactions rt
    JOIN (
        SELECT
            Customer_Card_ID,
            COUNT(DISTINCT Transaction_Store_ID) AS Total_Customer_Transactions
        FROM Recent_Transactions
        GROUP BY Customer_Card_ID
    ) tc ON tc.Customer_Card_ID = rt.Customer_Card_ID
    GROUP BY rt.Customer_Card_ID, rt.Transaction_Store_ID, tc.Total_Customer_Transactions
),
Customer_Primary_Store_CTE AS (
    SELECT
        rt.Customer_Card_ID,
        ARRAY_AGG(rt.Transaction_Store_ID ORDER BY rt.Transaction_DateTime DESC) AS recent_stores,
        ARRAY_AGG(rt.Transaction_Store_ID ORDER BY ss.Store_Transaction_Share DESC, rt.Transaction_DateTime DESC) AS sorted_stores
    FROM Recent_Transactions rt
    JOIN Store_Share ss ON rt.Customer_Card_ID = ss.Customer_Card_ID
    GROUP BY rt.Customer_Card_ID
)
SELECT
    sc.*,
    (CASE
        WHEN sc.Customer_Average_Check_Segment = 'Low' THEN 0
        WHEN sc.Customer_Average_Check_Segment = 'Medium' THEN 9
        WHEN sc.Customer_Average_Check_Segment = 'High' THEN 18
    END
    +
    CASE
        WHEN sc.Customer_Frequency_Segment = 'Rarely' THEN 0
        WHEN sc.Customer_Frequency_Segment = 'Occasionally' THEN 3
        WHEN sc.Customer_Frequency_Segment = 'Often' THEN 6
    END
    +
    CASE
        WHEN sc.Customer_Churn_Segment = 'Low' THEN 0
        WHEN sc.Customer_Churn_Segment = 'Medium' THEN 1
        WHEN sc.Customer_Churn_Segment = 'High' THEN 2
    END) + 1 AS Customer_Segment,
    COALESCE(ps.Transaction_Store_ID, 0) AS Customer_Primary_Store
FROM Subquery_With_Churn_Segment sc
LEFT JOIN (
    SELECT
        c.Customer_Card_ID,
        CASE
        -- Condition that checks whether the first three entries in the recent_stores array are all the same
            WHEN array_length(cps.recent_stores, 1) >= 3 AND cps.recent_stores[1] = cps.recent_stores[2] AND cps.recent_stores[1] = cps.recent_stores[3] THEN cps.recent_stores[1]
            ELSE cps.sorted_stores[1]
        END AS Transaction_Store_ID
    FROM Customer_Primary_Store_CTE cps
    JOIN Cards c ON cps.Customer_Card_ID = c.Customer_Card_ID
) ps ON sc.Customer_ID = ps.Customer_Card_ID;

REFRESH MATERIALIZED VIEW Customers;

SELECT * FROM Customers;

-- 2 --
DROP MATERIALIZED VIEW IF EXISTS Purchase_history CASCADE;
CREATE MATERIALIZED VIEW Purchase_history AS
SELECT
    General.Customer_ID,
    General.Transaction_ID,
    General.Transaction_DateTime,
    General.Group_ID,
    COALESCE(ROUND(SUM(General.SKU_Purchase_Price * General.SKU_Amount), 2), 0) AS Group_Cost,
    ROUND(SUM(General.SKU_Summ), 2) AS Group_Summ,
    ROUND(SUM(General.SKU_Summ_Paid), 2) AS Group_Summ_Paid
FROM (
    SELECT
        cards.Customer_ID,
        transactions.Transaction_id,
        transactions.Transaction_datetime,
        checks.SKU_ID,
        sku_group.Group_ID,
        group_name,
        SKU_Amount,
        SKU_Summ,
        SKU_Summ_Paid,
        SKU_Discount,
        SKU_Purchase_Price,
        SKU_Retail_Price
    FROM transactions
    JOIN cards ON transactions.customer_card_id = cards.customer_card_id
    JOIN checks ON transactions.transaction_id = checks.transaction_id
    JOIN product_grid ON checks.sku_id = product_grid.sku_id
    JOIN stores ON product_grid.sku_id = stores.sku_id AND transactions.transaction_store_id = stores.transaction_store_id
    JOIN sku_group ON product_grid.group_id = sku_group.group_id
) AS General
GROUP BY General.Customer_ID, General.Transaction_id, General.Transaction_datetime, General.Group_ID;

REFRESH MATERIALIZED VIEW Purchase_history;

-- Tests for PurchaseHistory VIEW --
INSERT INTO Personal_Information (Customer_Name, Customer_Surname, Customer_Primary_Email, Customer_Primary_Phone)
VALUES
    ('John-Doe', 'Smithson', 'john.doe7@example.com', '+71234507890'),
    ('Анна-Луиза', 'Иванова Петрова', 'ivan7@example.com', '+71230567891');

INSERT INTO Cards (Customer_ID)
VALUES
    (1),
    (2);

INSERT INTO Transactions (Customer_Card_ID, Transaction_Summ, Transaction_DateTime, Transaction_Store_ID)
VALUES
    (1, 100.00, (TO_TIMESTAMP('21-08-2023 10:15:00', 'DD.MM.YYYY HH24:MI:SS')), 1),
    (2, 150.00, (TO_TIMESTAMP('21-08-2023 14:30:00', 'DD.MM.YYYY HH24:MI:SS')), 2);

INSERT INTO SKU_Group (Group_Name)
VALUES
    ('Yogurt'),
    ('Milk');

INSERT INTO Product_Grid (SKU_Name, Group_ID)
VALUES
    ('Vanilla Yogurt', 1),
    ('Strawberry Yogurt', 1),
    ('Whole Milk', 2);

INSERT INTO Stores (Transaction_Store_ID, SKU_ID, SKU_Purchase_Price, SKU_Retail_Price)
VALUES
    (1, 1, 2.50, 3.00),
    (2, 2, 2.75, 3.25),
    (1, 3, 1.50, 2.00);

INSERT INTO Checks (Transaction_ID, SKU_ID, SKU_Amount, SKU_Summ, SKU_Summ_Paid, SKU_Discount)
VALUES
    (1, 1, 2, 5.00, 5.00, 0),
    (1, 2, 3, 8.25, 8.25, 0),
    (2, 2, 1, 1.50, 1.50, 0);

SELECT Customer_ID,
       Transaction_ID,
       TO_CHAR(Transaction_DateTime, 'DD.MM.YYYY HH24:MI:SS') AS "Transaction_DateTime",
       Group_ID,
       Group_Cost,
       Group_Summ,
       Group_Summ_Paid
FROM Purchase_history;

-- 3 --
DROP MATERIALIZED VIEW IF EXISTS Periods;
CREATE MATERIALIZED VIEW Periods AS
SELECT DISTINCT
    ph.Customer_ID,
    ph.Group_ID,
    MIN(ph.Transaction_DateTime) AS First_Group_Purchase_Date,
    MAX(ph.Transaction_DateTime) AS Last_Group_Purchase_Date,
    COUNT(ph.Transaction_ID) AS Group_Purchase,
    (((TO_CHAR((MAX(Transaction_DateTime)::TIMESTAMP - MIN(Transaction_DateTime)::TIMESTAMP), 'DD'))::INTEGER + 1)*1.0) / COUNT(*)*1.0 AS Group_Frequency,
    MIN(
        CASE
            WHEN c.SKU_Discount = 0 THEN 0
            ELSE c.SKU_Discount / c.SKU_Summ
        END
    ) AS Group_Min_Discount
FROM Purchase_history ph
LEFT JOIN (
    SELECT
        General.Customer_ID,
        General.Group_ID,
        checks.SKU_Discount,
        checks.SKU_Summ
    FROM (
        SELECT
            cards.Customer_ID,
            transactions.Transaction_id,
            checks.SKU_ID,
            sku_group.Group_ID
        FROM transactions
        LEFT JOIN cards ON transactions.customer_card_id = cards.customer_card_id
        LEFT JOIN checks ON transactions.transaction_id = checks.transaction_id
        LEFT JOIN product_grid ON checks.sku_id = product_grid.sku_id
        LEFT JOIN sku_group ON product_grid.group_id = sku_group.group_id
    ) AS General
    LEFT JOIN checks ON General.Transaction_id = checks.transaction_id AND General.SKU_ID = checks.SKU_ID
    GROUP BY General.Customer_ID, General.Group_ID, checks.SKU_Discount, checks.SKU_Summ
) AS c ON ph.Customer_ID = c.Customer_ID AND ph.Group_ID = c.Group_ID
GROUP BY ph.Customer_ID, ph.Group_ID;

REFRESH MATERIALIZED VIEW Periods;

SELECT customer_id,
       group_id,
       TO_CHAR(first_group_purchase_date, 'DD.MM.YYYY HH24:MI:SS') AS first_group_purchase_date,
       TO_CHAR(last_group_purchase_date, 'DD.MM.YYYY HH24:MI:SS') AS last_group_purchase_date,
       group_purchase,
       group_frequency,
       group_min_discount
FROM Periods;

-- 4 --

-- Function creation that contains a materialized view
DROP FUNCTION create_groups(material_calc_method text, material_calc_period int, material_calc_count int) CASCADE;
CREATE OR REPLACE FUNCTION create_groups(material_calc_method text, material_calc_period int, material_calc_count int)
RETURNS void AS $$
DECLARE 
    dynamic_query text;
BEGIN
    dynamic_query := '
    DROP MATERIALIZED VIEW IF EXISTS Groups;
    CREATE MATERIALIZED VIEW Groups AS
    WITH CustomerSKUs AS (
    SELECT DISTINCT
        p.Customer_ID,
        ch.SKU_ID
    FROM Purchase_history p
    JOIN Cards c ON p.Customer_ID = c.Customer_ID
    JOIN Checks ch ON p.Transaction_ID = ch.Transaction_ID
),
UniqueSKUs AS (
    SELECT DISTINCT Customer_ID, SKU_ID
    FROM CustomerSKUs
),
SKUsWithGroups AS (
    SELECT
        u.Customer_ID,
        u.SKU_ID,
        pg.Group_ID
    FROM UniqueSKUs u
    JOIN Product_Grid pg ON u.SKU_ID = pg.SKU_ID
),
ChurnDays AS (
    SELECT
        p.Customer_ID,
        p.Group_ID,
        EXTRACT(DAY FROM (NOW() - MAX(ph.Transaction_DateTime))) AS Days_Since_Last_Purchase
    FROM Periods p
    LEFT JOIN Purchase_history ph ON p.Customer_ID = ph.Customer_ID AND p.Group_ID = ph.Group_ID
    GROUP BY p.Customer_ID, p.Group_ID
),
IntervalData AS (
    SELECT
        p.Customer_ID,
        p.Group_ID,
        -- Calculate the time interval between the current transaction and the previous transaction for the same customer and group
        EXTRACT(DAY FROM (ph.Transaction_DateTime - lag(ph.Transaction_DateTime, 1, ph.Transaction_DateTime) OVER (PARTITION BY p.Customer_ID, p.Group_ID ORDER BY ph.Transaction_DateTime))) AS Interval,
        p.Group_Frequency
    FROM Periods p
    JOIN Purchase_history ph ON p.Customer_ID = ph.Customer_ID AND p.Group_ID = ph.Group_ID
),
MarginCalculationMethod AS (
        SELECT ' || quote_literal(material_calc_method) || ' AS Margin_Calculation_Method, ' || material_calc_period || ' AS Margin_Calculation_Period, ' || material_calc_count || ' AS Margin_Calculation_Count
),
DiscountedTransactions AS (
    SELECT
        p.Customer_ID,
        p.Group_ID,
        COUNT(DISTINCT c.Transaction_ID) AS Discounted_Transactions
    FROM Periods p
    LEFT JOIN Purchase_history ph ON p.Customer_ID = ph.Customer_ID AND p.Group_ID = ph.Group_ID
    LEFT JOIN Checks c ON ph.Transaction_ID = c.Transaction_ID AND c.SKU_Discount > 0
    GROUP BY p.Customer_ID, p.Group_ID
),
MinimumDiscount AS (
    SELECT
        p.Customer_ID,
        p.Group_ID,
        MIN(p.Group_Min_Discount) AS Group_Minimum_Discount
    FROM Periods p
    GROUP BY p.Customer_ID, p.Group_ID
),
GroupDiscountInfo AS (
    SELECT
        ph.Customer_ID,
        ph.Group_ID,
        SUM(ph.Group_Summ_Paid) AS Total_Group_Summ_Paid,
        SUM(ph.Group_Summ) AS Total_Group_Summ
    FROM Purchase_history ph
    GROUP BY ph.Customer_ID, ph.Group_ID
),
MarginDataWithRowNum AS (
    SELECT
        ph.Customer_ID,
        ph.Group_ID,
        ph.Transaction_DateTime,
        ph.Group_Cost,
        ph.Group_Summ_Paid,
        ROW_NUMBER() OVER (PARTITION BY ph.Customer_ID, ph.Group_ID ORDER BY ph.Transaction_DateTime DESC) AS rn
    FROM Purchase_history ph
    JOIN Periods p ON ph.Customer_ID = p.Customer_ID AND ph.Group_ID = p.Group_ID
),
GroupMarginData AS (
    SELECT
        m.Customer_ID,
        m.Group_ID,
        SUM(
            CASE
                WHEN rn <= CASE
                    WHEN mcm.Margin_Calculation_Method = ' || quote_literal('period') || ' THEN mcm.Margin_Calculation_Period
                    WHEN mcm.Margin_Calculation_Method = ' || quote_literal('transactions') || ' THEN mcm.Margin_Calculation_Count
                    ELSE 0
                END THEN m.Group_Cost - m.Group_Summ_Paid
                ELSE 0
            END
        ) AS Actual_Margin
    FROM MarginDataWithRowNum m
    CROSS JOIN MarginCalculationMethod mcm
    GROUP BY m.Customer_ID, m.Group_ID
)
SELECT DISTINCT
    c.Customer_ID,
    s.Group_ID,
    (
        CAST(p.Group_Purchase AS DECIMAL) /
        NULLIF(
            COUNT(DISTINCT ph.Transaction_ID) FILTER (
                WHERE ph.Transaction_DateTime >= p.First_Group_Purchase_Date AND ph.Transaction_DateTime <= p.Last_Group_Purchase_Date
            ), 0)
    ) AS Group_Affinity_Index,
    (
        churn.Days_Since_Last_Purchase /
        NULLIF(p.Group_Frequency, 0)
    ) AS Group_Churn_Rate,
    AVG(
        ABS((i.Interval - i.Group_Frequency) / i.Group_Frequency)
    ) AS Group_Stability_Index,
    SUM(gmd.Actual_Margin) AS Group_Margin,
    dt.Discounted_Transactions / COUNT(DISTINCT ph.Transaction_ID) AS Group_Discount_Share,
    md.Group_Minimum_Discount,
    CASE
        WHEN gd.Total_Group_Summ = 0 THEN 0
        ELSE gd.Total_Group_Summ_Paid / gd.Total_Group_Summ
    END AS Group_Average_Discount
FROM SKUsWithGroups s
LEFT JOIN Periods p ON s.Customer_ID = p.Customer_ID AND s.Group_ID = p.Group_ID
LEFT JOIN Purchase_history ph ON s.Customer_ID = ph.Customer_ID
LEFT JOIN ChurnDays churn ON s.Customer_ID = churn.Customer_ID AND s.Group_ID = churn.Group_ID
LEFT JOIN (
    SELECT
        General.Customer_ID,
        General.Group_ID,
        checks.SKU_Discount,
        checks.SKU_Summ
    FROM (
        SELECT
            cards.Customer_ID,
            transactions.Transaction_id,
            checks.SKU_ID,
            sku_group.Group_ID
        FROM transactions
        LEFT JOIN cards ON transactions.customer_card_id = cards.customer_card_id
        LEFT JOIN checks ON transactions.transaction_id = checks.transaction_id
        LEFT JOIN product_grid ON checks.sku_id = product_grid.sku_id
        LEFT JOIN sku_group ON product_grid.group_id = sku_group.group_id
    ) AS General
    LEFT JOIN checks ON General.Transaction_id = checks.transaction_id AND General.SKU_ID = checks.SKU_ID
    GROUP BY General.Customer_ID, General.Group_ID, checks.SKU_Discount, checks.SKU_Summ
) AS c ON s.Customer_ID = c.Customer_ID AND s.Group_ID = c.Group_ID
LEFT JOIN GroupMarginData gmd ON s.Customer_ID = gmd.Customer_ID AND s.Group_ID = gmd.Group_ID
LEFT JOIN DiscountedTransactions dt ON s.Customer_ID = dt.Customer_ID AND s.Group_ID = dt.Group_ID
LEFT JOIN MinimumDiscount md ON s.Customer_ID = md.Customer_ID AND s.Group_ID = md.Group_ID
LEFT JOIN GroupDiscountInfo gd ON s.Customer_ID = gd.Customer_ID AND s.Group_ID = gd.Group_ID
LEFT JOIN IntervalData i ON s.Customer_ID = i.Customer_ID AND s.Group_ID = i.Group_ID
GROUP BY c.Customer_ID, s.Group_ID, p.Group_Purchase, p.First_Group_Purchase_Date, p.Last_Group_Purchase_Date, p.Group_Frequency, churn.Days_Since_Last_Purchase, dt.Discounted_Transactions, md.Group_Minimum_Discount, gd.Total_Group_Summ_Paid, gd.Total_Group_Summ;
    ';
    EXECUTE dynamic_query;
     RAISE NOTICE 'Materialized view "Groups" with method - %, Interval % days, and count % has been created successfully.', material_calc_method, material_calc_period, material_calc_count;
END;
$$ LANGUAGE plpgsql;

-- Creation of Materialized View by calling a function with parameters for margin count
SELECT create_groups('period', 10, 0);
-- 2nd parameter - days of period and 3rd one for the number of transactions made 
SELECT create_groups('transactions', 0, 10);

SELECT * FROM Groups;
