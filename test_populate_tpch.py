import duckdb


raw_connection = duckdb.connect("./tpch/db/raw_tpch.db")
materialized_connection = duckdb.connect("./tpch/db/materialized_tpch.db")

#Run simple query on the duckdb database to check wether there is any data ther
query = """select
        cast(l.data->>'l_returnflag' as char(1)),
        cast(l.data->>'l_linestatus' as char(1)),
        sum(cast(l.data->>'l_quantity' as Integer)) as sum_qty,
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2))) as sum_base_price,
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as sum_disc_price,
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2))) * (1 + cast(l.data->>'l_tax' as decimal(12,2)))) as sum_charge,
        avg(cast(l.data->>'l_quantity' as Integer)) as avg_qty,
        avg(cast(l.data->>'l_extendedprice' as decimal(12,2))) as avg_price,
        avg(cast(l.data->>'l_discount' as decimal(12,2))) as avg_disc,
        count(*) as count_order
from
        tpch l
where
        cast(l.data->>'l_shipdate' as date) <= date '1998-12-01' - interval '90' day
group by
        cast(l.data->>'l_returnflag' as char(1)),
        cast(l.data->>'l_linestatus' as char(1))
order by
        cast(l.data->>'l_returnflag' as char(1)),
        cast(l.data->>'l_linestatus' as char(1));"""

result = raw_connection.execute(query).fetchdf()

print(result)


materialized_query = """
SELECT
    l.l_returnflag,
    l.l_linestatus,
    SUM(l.l_quantity) AS sum_qty,
    SUM(l.l_extendedprice) AS sum_base_price,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS sum_disc_price,
    SUM(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge,
    AVG(l.l_quantity) AS avg_qty,
    AVG(l.l_extendedprice) AS avg_price,
    AVG(l.l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    tpch l
WHERE
    l.l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
    l.l_returnflag,
    l.l_linestatus
ORDER BY
    l.l_returnflag,
    l.l_linestatus;
"""

materialized_result = materialized_connection.execute(materialized_query).fetchdf()

print(materialized_result)


