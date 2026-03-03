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

create table items (
  item_id      number primary key,
  item_no      varchar2(30) not null unique,
  item_name    varchar2(200) not null,
  description  varchar2(500),
  price        number(12,2) not null
);

insert /*+ append parallel(items 4) */ into items (item_id, item_no, item_name, description, price)
select
  level as item_id,
  'SKU-' || to_char(level, 'FM0000') as item_no,
  'Item ' || level as item_name,
  'Description for item ' || level as description,
  round(mod(level * 37, 20000) / 100 + 1, 2) as price   -- $1.00 .. ~$200.00
from dual
connect by level <= 5000;

commit;

create table sales_items (
  sale_id   number not null,
  item_id   number not null,
  quantity  number not null,
  constraint sales_items_pk primary key (sale_id, item_id),
  constraint sales_items_sale_fk foreign key (sale_id) references sales(sale_id),
  constraint sales_items_item_fk foreign key (item_id) references items(item_id)
);

insert /*+ append parallel(si 8) ignore_row_on_dupkey_index(sales_items, sales_items_pk) */
into sales_items (sale_id, item_id, quantity)
select
  s.sale_id,
  mod(abs(ora_hash(s.sale_id * 1000 + k.k * 97)), 5000) + 1 as item_id,
  mod(abs(ora_hash(s.sale_id * 33   + k.k * 11)), 5) + 1 as quantity
from sales s
cross join (
  select 1 as k from dual
  union all select 2 from dual
  union all select 3 from dual
) k;

commit;

CREATE OR REPLACE FORCE EDITIONABLE JSON RELATIONAL DUALITY VIEW "ORGIL"."SALES_DV"  AS 
  orgil.sales {
    _id : sale_id
    customerId : customer_id
    saleDate : sale_date
    amount
    region
    orgil.sales_items
             @link (to : [SALE_ID]) {
        itemId : item_id
        quantity
        orgil.items
                 @link (from : [ITEM_ID]) {
            itemId : item_id 
            itemNo : item_no
            itemName : item_name
            description
            price
        }
    }
};

GRANT SODA_APP TO ORGIL;
GRANT INHERIT PRIVILEGES ON USER SYS TO ORDS_METADATA;
EXEC ords.enable_schema(p_schema => 'ORGIL');
COMMIT;

DECLARE
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    ORDS.ENABLE_SCHEMA(p_enabled => TRUE,
                       p_schema => 'ORGIL',
                       p_url_mapping_type => 'BASE_PATH',
                       p_url_mapping_pattern => 'ORGIL',
                       p_auto_rest_auth => FALSE);

    commit;
END;

BEGIN
  ORDS.ENABLE_OBJECT(
    p_enabled      => TRUE,
    p_schema       => 'ORGIL',
    p_object       => 'SALES_DV',
    p_object_type  => 'VIEW',
    p_object_alias => 'SALES_DV'
  );
  COMMIT;
END;
/

GRANT EXECUTE ON xdb.dbms_soda_admin TO ORGIL;
GRANT EXECUTE ON xdb.dbms_soda TO ORGIL;

CREATE OR REPLACE FORCE EDITIONABLE JSON RELATIONAL DUALITY VIEW "ORGIL"."SALES_DV" AS 
  orgil.sales @insert @update @delete {
    _id : sale_id,
    customerId : customer_id,
    saleDate : sale_date,
    amount,
    region,
    orgil.sales_items @insert @update @delete
             @link (to : [SALE_ID]) {
        itemId : item_id,
        quantity,
        orgil.items @insert @update @delete
                 @link (from : [ITEM_ID]) {
            itemId : item_id,
            itemNo : item_no,
            itemName : item_name,
            description,
            price
        }
    }
};


select * FROM sales WHERE sale_id = 1015;
select * from sales_dv;