drop TABLE sales;

CREATE TABLE sales (
    sale_id      NUMBER NOT NULL,
    customer_id  NUMBER NOT NULL,
    sale_date    DATE   NOT NULL,
    amount       NUMBER(10,2),
    region       VARCHAR2(20)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION p_before_2024 VALUES LESS THAN (DATE '2024-01-01')
);

Truncate table sales;

INSERT /*+ APPEND PARALLEL(sales 4) */ INTO sales
SELECT
    rn AS sale_id,

    MOD(ABS(ORA_HASH(rn * 13)), 100000) + 1 AS customer_id,
    DATE '2024-01-01' + MOD(ABS(ORA_HASH(rn * 7)), 790) AS sale_date,
    ROUND(MOD(rn * 17, 499000) / 100 + 10, 2) AS amount,

    CASE MOD(ABS(ORA_HASH(rn * 19)), 6)
      WHEN 0 THEN 'NORTH'
      WHEN 1 THEN 'SOUTH'
      WHEN 2 THEN 'EAST'
      WHEN 3 THEN 'WEST'
      WHEN 4 THEN 'CENTRAL'
      ELSE 'INTL'
    END AS region
FROM (
    SELECT LEVEL rn
    FROM dual
    CONNECT BY LEVEL <= 1000000
);

COMMIT;

BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname => USER,
    tabname => 'SALES',
    cascade => FALSE,
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
  );
END;
/

SELECT *
FROM sales
WHERE customer_id = 1;

SELECT SUM(amount)
FROM sales
WHERE sale_date >= DATE '2024-02-01'
  AND sale_date <  DATE '2024-03-01'
  and customer_id = 1;

CREATE INDEX idx_sales_global_cust
ON sales (customer_id)
GLOBAL;

drop index idx_sales_global_cust;

CREATE INDEX idx_sales_local_cust
ON sales (customer_id)
LOCAL;

drop index idx_sales_local_cust;