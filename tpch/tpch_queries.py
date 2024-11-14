RAW_TPCH_QUERIES = [# Query 1: Summarizes data with aggregates like sum or avg
"""select
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
        cast(l.data->>'l_linestatus' as char(1));""",

# Query 2: Sorts data based on specific columns
"""select
        cast(s.data->>'s_acctbal' as decimal(12,2)),
        s.data->>'s_name',
        n.data->>'n_name',
        cast(p.data->>'p_partkey' as int),
        p.data->>'p_mfgr',
        s.data->>'s_address',
        s.data->>'s_phone',
        s.data->>'s_comment'
from
        tpch p,
        tpch s,
        tpch ps,
        tpch n,
        tpch r
where
        cast(p.data->>'p_partkey' as int) = cast(ps.data->>'ps_partkey' as int)
        and cast(s.data->>'s_suppkey' as int) = cast(ps.data->>'ps_suppkey' as int)
        and cast(p.data->>'p_size' as int) = 15
        and COALESCE(p.data->>'p_type', '') like '%BRASS'
        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
        and cast(n.data->>'n_regionkey' as int) = cast(r.data->>'r_regionkey' as int)
        and COALESCE(r.data->>'r_name', '') = 'EUROPE'
        and cast(ps.data->>'ps_supplycost' as decimal(12,2)) = (
                select
                        min(cast(ps.data->>'ps_supplycost' as decimal(12,2)))
                from
                        tpch s,
                        tpch ps,
                        tpch n,
                        tpch r
                where
                        cast(p.data->>'p_partkey' as int) = cast(ps.data->>'ps_partkey' as int)
                        and cast(s.data->>'s_suppkey' as int) = cast(ps.data->>'ps_suppkey' as int)
                        and cast(s.data->>'s_nationkey' as int)= cast(n.data->>'n_nationkey' as int)
                        and cast(n.data->>'n_regionkey' as int) = cast(r.data->>'r_regionkey' as int)
                        and COALESCE(r.data->>'r_name', '') = 'EUROPE'

        )
order by
        cast(s.data->>'s_acctbal' as decimal(12,2)) desc,
        n.data->>'n_name',
        s.data->>'s_name',
        cast(p.data->>'p_partkey' as int)
limit
        100;""",

# Query 3: Summarizes data with aggregates like sum or avg
"""select
        cast(l.data->>'l_orderkey' as int),
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as revenue,
        cast(o.data->>'o_orderdate' as date),
        cast(o.data->>'o_shippriority' as int)
from
        tpch c,
        tpch o,
        tpch l
where
        c.data->>'c_mktsegment' = 'BUILDING'
        and cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
        and cast(l.data->>'l_orderkey' as int) = cast(o.data->>'o_orderkey' as int)
        and cast(o.data->>'o_orderdate' as date) < date '1995-03-15'
        and cast(l.data->>'l_shipdate' as date) > date '1995-03-15'
group by
        cast(l.data->>'l_orderkey' as int),
        cast(o.data->>'o_orderdate' as date),
        cast(o.data->>'o_shippriority' as int)
order by
        revenue desc,
        cast(o.data->>'o_orderdate' as date)
limit
        10;""",

# Query 4: Groups data based on specified columns
"""select
        o.data->>'o_orderpriority',
        count(*) as order_count
from
        tpch o
where
        cast(o.data->>'o_orderdate' as date) >= date '1993-07-01'
        and cast(o.data->>'o_orderdate' as date) < date '1993-07-01' + interval '3' month
        and exists (
                select
                        *
                from
                        tpch l
                where
                        cast(l.data->>'l_orderkey' as int) = cast(o.data->>'o_orderkey' as int)
                        and cast(l.data->>'l_commitdate' as date) < cast(l.data->>'l_receiptdate' as date)
        )
group by
        o.data->>'o_orderpriority'
order by
        o.data->>'o_orderpriority';""",

# Query 5: Summarizes data with aggregates like sum or avg
"""select
        n.data->>'n_name',
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as revenue
from
        tpch c,
        tpch o,
        tpch l,
        tpch s,
        tpch n,
        tpch r
where
        cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
        and cast(l.data->>'l_orderkey' as int) = cast(o.data->>'o_orderkey' as int)
        and cast(l.data->>'l_suppkey' as int) = cast(s.data->>'s_suppkey' as int)
        and cast(c.data->>'c_nationkey' as int) = cast(s.data->>'s_nationkey' as int)
        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
        and cast(n.data->>'n_regionkey' as int) = cast(r.data->>'r_regionkey' as int)
        and COALESCE(r.data->>'r_name', '') = 'ASIA'
        and cast(o.data->>'o_orderdate' as date) >= date '1994-01-01'
        and cast(o.data->>'o_orderdate' as date) < date '1994-01-01' + interval '1' year
group by
        n.data->>'n_name'
order by
        revenue desc;""",

# Query 6: Summarizes data with aggregates like sum or avg
"""select
       sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (cast(l.data->>'l_discount' as decimal(12,2)))) as revenue
from
        tpch l
where
        cast(l.data->>'l_shipdate' as date) >= date '1994-01-01'
        and cast(l.data->>'l_shipdate' as date) < date '1994-01-01' + interval '1' year
        and cast(l.data->>'l_discount' as decimal(12,2)) between 0.06 - 0.01 and 0.06 + 0.01
        and cast(l.data->>'l_quantity' as int) < 24;""",

# Query 7: Summarizes data with aggregates like sum or avg
"""select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.data->>'n_name' as supp_nation,
                        n2.data->>'n_name' as cust_nation,
                        extract(year from cast(l.data->>'l_shipdate' as date)) as l_year,
                        cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2))) as volume
                from
                        tpch s,
                        tpch l,
                        tpch o,
                        tpch c,
                        tpch n1,
                        tpch n2
                where
                        cast(s.data->>'s_suppkey' as int) = cast(l.data->>'l_suppkey' as int)
                        and cast(o.data->>'o_orderkey' as int) = cast(l.data->>'l_orderkey' as int)
                        and cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
                        and cast(s.data->>'s_nationkey' as int) = cast(n1.data->>'n_nationkey' as int)
                        and cast(c.data->>'c_nationkey' as int) = cast(n2.data->>'n_nationkey' as int)
                        and (
                                (n1.data->>'n_name' = 'FRANCE' and n2.data->>'n_name' = 'GERMANY')
                                or (n1.data->>'n_name' = 'GERMANY' and n2.data->>'n_name' = 'FRANCE')
                        )
                        and cast(l.data->>'l_shipdate' as date) between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year;""",

# Query 8: Summarizes data with aggregates like sum or avg
"""select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from cast(o.data->>'o_orderdate' as date)) as o_year,
                        cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2))) as volume,
                        n2.data->>'n_name' as nation
                from
                        tpch p,
                        tpch s,
                        tpch l,
                        tpch o,
                        tpch c,
                        tpch n1,
                        tpch n2,
                        tpch r
                where
                        cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
                        and cast(s.data->>'s_suppkey' as int) = cast(l.data->>'l_suppkey' as int)
                        and cast(l.data->>'l_orderkey' as int) = cast(o.data->>'o_orderkey' as int)
                        and cast(o.data->>'o_custkey' as int) = cast(c.data->>'c_custkey' as int)
                        and cast(r.data->>'r_regionkey' as int) = cast(n1.data->>'n_regionkey' as int)
                        and cast(c.data->>'c_nationkey' as int) = cast(n1.data->>'n_nationkey' as int)
                        and r.data->>'r_name' = 'AMERICA'
                        and cast(s.data->>'s_nationkey' as int) = cast(n2.data->>'n_nationkey' as int)
                        and cast(o.data->>'o_orderdate' as date) between date '1995-01-01' and date '1996-12-31'
                        and COALESCE(p.data->>'p_type', '') = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;""",

# Query 9: Summarizes data with aggregates like sum or avg
"""select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n.data->>'n_name' as nation,
                        extract(year from cast(o.data->>'o_orderdate' as date)) as o_year,
                        cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2))) - cast(ps.data->>'ps_supplycost' as decimal(12,2)) * cast(l.data->>'l_discount' as decimal(12,2)) as amount
                from
                        tpch p,
                        tpch s,
                        tpch l,
                        tpch ps,
                        tpch o,
                        tpch n
                where
                        cast(s.data->>'s_suppkey' as int) = cast(l.data->>'l_suppkey' as int)
                        and cast(ps.data->>'ps_suppkey' as int) = cast(l.data->>'l_suppkey' as int)
                        and cast(ps.data->>'ps_partkey' as int) = cast(l.data->>'l_partkey' as int)
                        and cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
                        and cast(o.data->>'o_orderkey' as int) = cast(l.data->>'l_orderkey' as int)
                        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
                        and COALESCE(p.data->>'p_name', '') like '%green%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc;""",

# Query 10: Summarizes data with aggregates like sum or avg
"""select
        cast(c.data->>'c_custkey' as int),
        c.data->>'c_name',
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as revenue,
        cast(c.data->>'c_acctbal' as decimal(12,2)),
        n.data->>'n_name',
        c.data->>'c_address',
        c.data->>'c_phone',
        c.data->>'c_comment'
from
        tpch c,
        tpch o,
        tpch l,
        tpch n
where
        cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
        and cast(l.data->>'l_orderkey' as int) = cast(o.data->>'o_orderkey' as int)
        and cast(o.data->>'o_orderdate' as date) >= date '1993-10-01'
        and cast(o.data->>'o_orderdate' as date) < date '1993-10-01' + interval '3' month
        and cast(l.data->>'l_returnflag' as char(1)) = 'R'
        and cast(c.data->>'c_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
group by
        cast(c.data->>'c_custkey' as int),
        c.data->>'c_name',
        cast(c.data->>'c_acctbal' as decimal(12,2)),
        c.data->>'c_phone',
        n.data->>'n_name',
        c.data->>'c_address',
        c.data->>'c_comment'
order by
        revenue desc
limit
        20;""",

# Query 11: Summarizes data with aggregates like sum or avg
"""select
        cast(ps.data->>'ps_partkey' as int),
        sum(cast(ps.data->>'ps_supplycost' as decimal(12,2)) * cast(ps.data->>'ps_availqty' as int)) as value
from
        tpch ps,
        tpch s,
        tpch n
where
        cast(ps.data->>'ps_suppkey' as int) = cast(s.data->>'s_suppkey' as int)
        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
        and COALESCE(n.data->>'n_name', '') = 'GERMANY'
group by
        cast(ps.data->>'ps_partkey' as int) having
                sum(cast(ps.data->>'ps_supplycost' as decimal(12,2)) * cast(ps.data->>'ps_availqty' as int)) > (
                        select
                                sum(cast(ps.data->>'ps_supplycost' as decimal(12,2)) * cast(ps.data->>'ps_availqty' as int)) * 0.0001
                        from
                                tpch ps,
                                tpch s,
                                tpch n
                        where
                                cast(ps.data->>'ps_suppkey' as int) = cast(s.data->>'s_suppkey' as int)
                                and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
                                and COALESCE(n.data->>'n_name', '') = 'GERMANY'
                )
order by
        value desc;""",

# Query 12: Summarizes data with aggregates like sum or avg
"""select
        l.data->>'l_shipmode',
        sum(case
                when o.data->>'o_orderpriority' = '1-URGENT'
                        or o.data->>'o_orderpriority' = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o.data->>'o_orderpriority' <> '1-URGENT'
                        and o.data->>'o_orderpriority' <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        tpch o,
        tpch l
where
        cast(o.data->>'o_orderkey' as int) = cast(l.data->>'l_orderkey' as int)
        and l.data->>'l_shipmode' in ('MAIL', 'SHIP')
        and cast(l.data->>'l_commitdate' as date) < cast(l.data->>'l_receiptdate' as date)
        and cast(l.data->>'l_shipdate' as date) < cast(l.data->>'l_commitdate' as date)
        and cast(l.data->>'l_receiptdate' as date) >= date '1994-01-01'
        and cast(l.data->>'l_receiptdate' as date) < date '1994-01-01' + interval '1' year
group by
        l.data->>'l_shipmode'
order by
        l.data->>'l_shipmode';""",

# Query 13: Groups data based on specified columns
"""select
        c_count,
        count(*) as custdist
from
        (
                select
                        cast(c.data->>'c_custkey' as int),
                        count(cast(o.data->>'o_orderkey' as int))
                from
                        tpch c left outer join tpch o on
                                cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
                                and COALESCE(o.data->>'o_comment', '') not like '%special%requests%'
                group by
                        cast(c.data->>'c_custkey' as int)
        ) as c_orders (c_custkey, c_count)
group by
        c_count
order by
        custdist desc,
        c_count desc;""",

# Query 14: Summarizes data with aggregates like sum or avg
"""select
        100.00 * sum(case
                when p.data->>'p_type' like 'PROMO%'
                        then cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))
                else 0
        end) / sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as promo_revenue
from
        tpch l,
        tpch p
where
        cast(l.data->>'l_partkey' as int) = cast(p.data->>'p_partkey' as int)
        and cast(l.data->>'l_shipdate' as date) >= date '1995-09-01'
        and cast(l.data->>'l_shipdate' as date) < date '1995-09-01' + interval '1' month;""",

# Query 15: Summarizes data with aggregates like sum or avg
"""with revenue (supplier_no, total_revenue) as (
        select
                cast(l.data->>'l_suppkey' as int),
                sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2))))
        from
               tpch l
        where
                cast(l.data->>'l_shipdate' as date) >= date '1996-01-01'
                and cast(l.data->>'l_shipdate' as date) < date '1996-01-01' + interval '3' month
        group by
                cast(l.data->>'l_suppkey' as int))
select
        cast(s.data->>'s_suppkey' as int),
        s.data->>'s_name',
        s.data->>'s_address',
        s.data->>'s_phone',
        total_revenue
from
        tpch s,
        revenue
where
        cast(s.data->>'s_suppkey' as int) = supplier_no
        and total_revenue = (
                select
                        max(total_revenue)
                from
                        revenue
        )
order by
        cast(s.data->>'s_suppkey' as int);""",

# Query 16: Groups data based on specified columns
"""select
        p.data->>'p_brand',
        p.data->>'p_type',
        cast(p.data->>'p_size' as int),
        count(distinct cast(ps.data->>'ps_suppkey' as int)) as supplier_cnt
from
        tpch ps,
        tpch p
where
        cast(p.data->>'p_partkey' as int) = cast(ps.data->>'ps_partkey' as int)
        and COALESCE(p.data->>'p_brand', '') <> 'Brand#45'
        and COALESCE(p.data->>'p_type', '') not like 'MEDIUM POLISHED%'
        and cast(p.data->>'p_size' as int) in (49, 14, 23, 45, 19, 3, 36, 9)
        and cast(ps.data->>'ps_suppkey' as int) not in (
                select
                        cast(s.data->>'s_suppkey' as int)
                from
                        tpch s
                where
                        s.data->>'s_comment' like '%Customer%Complaints%'
        )
group by
        p.data->>'p_brand',
        p.data->>'p_type',
        cast(p.data->>'p_size' as int)
order by
        supplier_cnt desc,
        p.data->>'p_brand',
        p.data->>'p_type',
        cast(p.data->>'p_size' as int)""",

# Query 17: Summarizes data with aggregates like sum or avg
"""select
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2))) / 7.0 as avg_yearly
from
        tpch l,
        tpch p
where
        cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
        and p.data->>'p_brand' = 'Brand#23'
        and COALESCE(p.data->>'p_container', '') = 'MED BOX'
        and cast(l.data->>'l_quantity' as int) < (
                select
                        0.2 * avg(cast(l.data->>'l_quantity' as int))
                from
                        tpch l
                where
                        cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
        );""",

# Query 18: Summarizes data with aggregates like sum or avg
"""select
        c.data->>'c_name',
        cast(c.data->>'c_custkey' as int),
        cast(o.data->>'o_orderkey' as int),
        cast(o.data->>'o_orderdate' as date),
        cast(o.data->>'o_totalprice' as decimal(12,2)),
        sum(cast(l.data->>'l_quantity' as int))
from
        tpch c,
        tpch o,
        tpch l
where
        cast(o.data->>'o_orderkey' as int) in (
                select
                        cast(l.data->>'l_orderkey' as int)
                from
                        tpch l
                group by
                        cast(l.data->>'l_orderkey' as int) having
                                sum(cast(l.data->>'l_quantity' as int)) > 300
        )
        and cast(c.data->>'c_custkey' as int) = cast(o.data->>'o_custkey' as int)
        and cast(o.data->>'o_orderkey' as int) = cast(l.data->>'l_orderkey' as int)
group by
        c.data->>'c_name',
        cast(c.data->>'c_custkey' as int),
        cast(o.data->>'o_orderkey' as int),
        cast(o.data->>'o_orderdate' as date),
        cast(o.data->>'o_totalprice' as decimal(12,2))
order by
        cast(o.data->>'o_totalprice' as decimal(12,2)) desc,
        cast(o.data->>'o_orderdate' as date)
limit
        100;""",

# Query 19: Summarizes data with aggregates like sum or avg
"""select
        sum(cast(l.data->>'l_extendedprice' as decimal(12,2)) * (1 - cast(l.data->>'l_discount' as decimal(12,2)))) as revenue
from
        tpch l,
        tpch p
where
        (
                cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
                and p.data->>'p_brand' = 'Brand#12'
                and p.data->>'p_container' in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and cast(l.data->>'l_quantity' as int) >= 1 and cast(l.data->>'l_quantity' as int) <= 1 + 10
                and cast(p.data->>'p_size' as int) between 1 and 5
                and COALESCE(l.data->>'l_shipmode', '') in ('AIR', 'AIR REG')
                and COALESCE(l.data->>'l_shipinstruct', '') = 'DELIVER IN PERSON'
        )
        or
        (
                cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
                and p.data->>'p_brand' = 'Brand#23'
                and p.data->>'p_container' in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and cast(l.data->>'l_quantity' as int) >= 10 and cast(l.data->>'l_quantity' as int) <= 10 + 10
                and cast(p.data->>'p_size' as int) between 1 and 10
                and COALESCE(l.data->>'l_shipmode', '') in ('AIR', 'AIR REG')
                and COALESCE(l.data->>'l_shipinstruct', '') = 'DELIVER IN PERSON'
        )
        or
        (
                cast(p.data->>'p_partkey' as int) = cast(l.data->>'l_partkey' as int)
                and p.data->>'p_brand' = 'Brand#34'
                and p.data->>'p_container' in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and cast(l.data->>'l_quantity' as int) >= 20 and cast(l.data->>'l_quantity' as int) <= 20 + 10
                and cast(p.data->>'p_size' as int) between 1 and 15
                and COALESCE(l.data->>'l_shipmode', '') in ('AIR', 'AIR REG')
                and COALESCE(l.data->>'l_shipinstruct', '') = 'DELIVER IN PERSON'
        );""",

# Query 20: Summarizes data with aggregates like sum or avg
"""select
        s.data->>'s_name',
        s.data->>'s_address'
from
        tpch s,
        tpch n
where
        cast(s.data->>'s_suppkey' as int) in (
                select
                        cast(ps.data->>'ps_suppkey' as int)
                from
                        tpch ps
                where
                        cast(ps.data->>'ps_partkey' as int) in (
                                select
                                        cast(p.data->>'p_partkey' as int)
                                from
                                        tpch p
                                where
                                        p.data->>'p_name' like 'forest%'
                        )
                        and cast(ps.data->>'ps_availqty' as int) > (
                                select
                                        0.5 * sum(cast(l.data->>'l_quantity' as int))
                                from
                                        tpch l
                                where
                                        cast(l.data->>'l_partkey' as int) = cast(ps.data->>'ps_partkey' as int)
                                        and cast(l.data->>'l_suppkey' as int) = cast(ps.data->>'ps_suppkey' as int)
                                        and cast(l.data->>'l_shipdate' as date) >= date '1994-01-01'
                                        and cast(l.data->>'l_shipdate' as date) < date '1994-01-01' + interval '1' year
                        )
        )
        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
        and COALESCE(n.data->>'n_name', '') = 'CANADA'
order by
        s.data->>'s_name';""",

# Query 21: Groups data based on specified columns
"""select
        s.data->>'s_name',
        count(*) as numwait
from
        tpch s,
        tpch l1,
        tpch o,
        tpch n
where
        cast(s.data->>'s_suppkey' as int) = cast(l1.data->>'l_suppkey' as int)
        and cast(o.data->>'o_orderkey' as int) = cast(l1.data->>'l_orderkey' as int)
        and cast(o.data->>'o_orderstatus' as char(1)) = 'F'
        and cast(l1.data->>'l_receiptdate' as date) > cast(l1.data->>'l_commitdate' as date)
        and exists (
                select
                        *
                from
                        tpch l2
                where
                        cast(l2.data->>'l_orderkey' as int) = cast(l1.data->>'l_orderkey' as int)
                        and cast(l2.data->>'l_suppkey' as int) <> cast(l1.data->>'l_suppkey' as int)
        )
        and not exists (
                select
                        *
                from
                        tpch l3
                where
                        cast(l3.data->>'l_orderkey' as int) = cast(l1.data->>'l_orderkey' as int)
                        and cast(l3.data->>'l_suppkey' as int) <> cast(l1.data->>'l_suppkey' as int)
                        and cast(l3.data->>'l_receiptdate' as date) > cast(l3.data->>'l_commitdate' as date)
        )
        and cast(s.data->>'s_nationkey' as int) = cast(n.data->>'n_nationkey' as int)
        and COALESCE(n.data->>'n_name', '') = 'SAUDI ARABIA'
group by
        s.data->>'s_name'
order by
        numwait desc,
        s.data->>'s_name'
limit
        100;""",
# Query 22: Summarizes data with aggregates like sum or avg
"""select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c.data->>'c_phone' from 1 for 2) as cntrycode,
                        cast(c.data->>'c_acctbal' as decimal(12,2)) as c_acctbal
                from
                        tpch c
                where
                        substring(c.data->>'c_phone' from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and cast(c.data->>'c_acctbal' as decimal(12,2)) > (
                                select
                                        avg(cast(c.data->>'c_acctbal' as decimal(12,2)))
                                from
                                        tpch c
                                where
                                        cast(c.data->>'c_acctbal' as decimal(12,2)) > 0.00
                                        and substring(c.data->>'c_phone' from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        tpch o
                                where
                                        cast(o.data->>'o_custkey' as int) = cast(c.data->>'c_custkey' as int)
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode;"""]

MATERIALIZED_TPCH_QUERIES = [
    # Materialized Query 1
    """SELECT
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
""",
    # Materialized Query 2
"""SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    tpch p,
    tpch s,
    tpch ps,
    tpch n,
    tpch r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND p.p_size = 15
    AND COALESCE(p.p_type, '') LIKE '%BRASS'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND COALESCE(r.r_name, '') = 'EUROPE'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps_inner.ps_supplycost)
        FROM
            tpch s_inner,
            tpch ps_inner,
            tpch n_inner,
            tpch r_inner
        WHERE
            p.p_partkey = ps_inner.ps_partkey
            AND s_inner.s_suppkey = ps_inner.ps_suppkey
            AND s_inner.s_nationkey = n_inner.n_nationkey
            AND n_inner.n_regionkey = r_inner.r_regionkey
            AND r_inner.r_name = 'EUROPE'
    )
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    p.p_partkey
LIMIT
    100;
""",
    # Materialized Query 3
"""SELECT
    l.l_orderkey,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    o.o_orderdate,
    o.o_shippriority
FROM
    tpch c,
    tpch o,
    tpch l
WHERE
    c.c_mktsegment = 'BUILDING'
    AND c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate < DATE '1995-03-15'
    AND l.l_shipdate > DATE '1995-03-15'
GROUP BY
    l.l_orderkey,
    o.o_orderdate,
    o.o_shippriority
ORDER BY
    revenue DESC,
    o.o_orderdate
LIMIT
    10;
""",
    # Materialized Query 4
"""SELECT
    o.o_orderpriority,
    COUNT(*) AS order_count
FROM
    tpch o
WHERE
    o.o_orderdate >= DATE '1993-07-01'
    AND o.o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            tpch l
        WHERE
            l.l_orderkey = o.o_orderkey
            AND l.l_commitdate < l.l_receiptdate
    )
GROUP BY
    o.o_orderpriority
ORDER BY
    o.o_orderpriority;
""",
    # Materialized Query 5
"""SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    tpch c,
    tpch o,
    tpch l,
    tpch s,
    tpch n,
    tpch r
WHERE
    c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND l.l_suppkey = s.s_suppkey
    AND c.c_nationkey = s.s_nationkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND COALESCE(r.r_name, '') = 'ASIA'
    AND o.o_orderdate >= DATE '1994-01-01'
    AND o.o_orderdate < DATE '1995-01-01'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
""",
    # Materialized Query 6
"""SELECT
    SUM(l.l_extendedprice * l.l_discount) AS revenue
FROM
    tpch l
WHERE
    l.l_shipdate >= DATE '1994-01-01'
    AND l.l_shipdate < DATE '1995-01-01'
    AND l.l_discount BETWEEN 0.05 AND 0.07
    AND l.l_quantity < 24;
""",
    # Materialized Query 7
"""SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
            l.l_extendedprice * (1 - l.l_discount) AS volume
        FROM
            tpch s,
            tpch l,
            tpch o,
            tpch c,
            tpch n1,
            tpch n2
        WHERE
            s.s_suppkey = l.l_suppkey
            AND o.o_orderkey = l.l_orderkey
            AND c.c_custkey = o.o_custkey
            AND s.s_nationkey = n1.n_nationkey
            AND c.c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
""",
    # Materialized Query 8
"""SELECT
    o_year,
    SUM(CASE
            WHEN nation = 'BRAZIL' THEN volume
            ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
    (
        SELECT
            EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
            l.l_extendedprice * (1 - l.l_discount) AS volume,
            n2.n_name AS nation
        FROM
            tpch p,
            tpch s,
            tpch l,
            tpch o,
            tpch c,
            tpch n1,
            tpch n2,
            tpch r
        WHERE
            p.p_partkey = l.l_partkey
            AND s.s_suppkey = l.l_suppkey
            AND l.l_orderkey = o.o_orderkey
            AND o.o_custkey = c.c_custkey
            AND r.r_regionkey = n1.n_regionkey
            AND c.c_nationkey = n1.n_nationkey
            AND r.r_name = 'AMERICA'
            AND s.s_nationkey = n2.n_nationkey
            AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            AND COALESCE(p.p_type, '') = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
""",
    # Materialized Query 9
    """SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n.n_name AS nation,
            EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
            l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_discount AS amount
        FROM
            tpch p,
            tpch s,
            tpch l,
            tpch ps,
            tpch o,
            tpch n
        WHERE
            s.s_suppkey = l.l_suppkey
            AND ps.ps_suppkey = l.l_suppkey
            AND ps.ps_partkey = l.l_partkey
            AND p.p_partkey = l.l_partkey
            AND o.o_orderkey = l.l_orderkey
            AND s.s_nationkey = n.n_nationkey
            AND COALESCE(p.p_name, '') LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
""",
    # Materialized Query 10
    """SELECT
    c.c_custkey,
    c.c_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
FROM
    tpch c,
    tpch o,
    tpch l,
    tpch n
WHERE
    c.c_custkey = o.o_custkey
    AND l.l_orderkey = o.o_orderkey
    AND o.o_orderdate >= DATE '1993-10-01'
    AND o.o_orderdate < DATE '1994-01-01'
    AND l.l_returnflag = 'R'
    AND c.c_nationkey = n.n_nationkey
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    n.n_name,
    c.c_address,
    c.c_comment
ORDER BY
    revenue DESC
LIMIT
    20;
""",
    # Materialized Query 11
"""SELECT
    ps.ps_partkey,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    tpch ps,
    tpch s,
    tpch n
WHERE
    ps.ps_suppkey = s.s_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND COALESCE(n.n_name, '') = 'GERMANY'
GROUP BY
    ps.ps_partkey
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) > (
        SELECT
            SUM(ps_inner.ps_supplycost * ps_inner.ps_availqty) * 0.0001
        FROM
            tpch ps_inner,
            tpch s_inner,
            tpch n_inner
        WHERE
            ps_inner.ps_suppkey = s_inner.s_suppkey
            AND s_inner.s_nationkey = n_inner.n_nationkey
            AND COALESCE(n_inner.n_name, '') = 'GERMANY'
    )
ORDER BY
    value DESC;
""",
    # Materialized Query 12
    """SELECT
    l.l_shipmode,
    SUM(CASE
            WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1
            ELSE 0
        END) AS high_line_count,
    SUM(CASE
            WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' THEN 1
            ELSE 0
        END) AS low_line_count
FROM
    tpch o,
    tpch l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_shipmode IN ('MAIL', 'SHIP')
    AND l.l_commitdate < l.l_receiptdate
    AND l.l_shipdate < l.l_commitdate
    AND l.l_receiptdate >= DATE '1994-01-01'
    AND l.l_receiptdate < DATE '1995-01-01'
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode;
""",
    # Materialized Query 13
    """SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c.c_custkey,
            COUNT(o.o_orderkey) AS c_count
        FROM
            tpch c LEFT OUTER JOIN tpch o ON
                c.c_custkey = o.o_custkey
                AND COALESCE(o.o_comment, '') NOT LIKE '%special%requests%'
        GROUP BY
            c.c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
""",
    # Materialized Query 14
    """SELECT
    100.00 * SUM(
        CASE
            WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount)
            ELSE 0
        END
    ) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    tpch l,
    tpch p
WHERE
    l.l_partkey = p.p_partkey
    AND l.l_shipdate >= DATE '1995-09-01'
    AND l.l_shipdate < DATE '1995-10-01';
""",
    # Materialized Query 15
    """WITH revenue (supplier_no, total_revenue) AS (
    SELECT
        l.l_suppkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
    FROM
        tpch l
    WHERE
        l.l_shipdate >= DATE '1996-01-01'
        AND l.l_shipdate < DATE '1996-04-01'
    GROUP BY
        l.l_suppkey
)
SELECT
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone,
    total_revenue
FROM
    tpch s,
    revenue
WHERE
    s.s_suppkey = revenue.supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue
    )
ORDER BY
    s.s_suppkey;
""",
    # Materialized Query 16
    """SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    tpch ps,
    tpch p
WHERE
    p.p_partkey = ps.ps_partkey
    AND COALESCE(p.p_brand, '') <> 'Brand#45'
    AND COALESCE(p.p_type, '') NOT LIKE 'MEDIUM POLISHED%'
    AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.ps_suppkey NOT IN (
        SELECT
            s.s_suppkey
        FROM
            tpch s
        WHERE
            s.s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;
""",
    # Materialized Query 17
    """SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    tpch l,
    tpch p
WHERE
    p.p_partkey = l.l_partkey
    AND p.p_brand = 'Brand#23'
    AND COALESCE(p.p_container, '') = 'MED BOX'
    AND l.l_quantity < (
        SELECT
            0.2 * AVG(l_inner.l_quantity)
        FROM
            tpch l_inner
        WHERE
            p.p_partkey = l_inner.l_partkey
    );
""",
    # Materialized Query 18
    """SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity) AS total_quantity
FROM
    tpch c,
    tpch o,
    tpch l
WHERE
    o.o_orderkey IN (
        SELECT
            l_inner.l_orderkey
        FROM
            tpch l_inner
        GROUP BY
            l_inner.l_orderkey
        HAVING
            SUM(l_inner.l_quantity) > 300
    )
    AND c.c_custkey = o.o_custkey
    AND o.o_orderkey = l.l_orderkey
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate
LIMIT
    100;
""",
    # Materialized Query 19
"""SELECT
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    tpch l,
    tpch p
WHERE
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = 'Brand#12'
        AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l.l_quantity BETWEEN 1 AND 11
        AND p.p_size BETWEEN 1 AND 5
        AND COALESCE(l.l_shipmode, '') IN ('AIR', 'AIR REG')
        AND COALESCE(l.l_shipinstruct, '') = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = 'Brand#23'
        AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l.l_quantity BETWEEN 10 AND 20
        AND p.p_size BETWEEN 1 AND 10
        AND COALESCE(l.l_shipmode, '') IN ('AIR', 'AIR REG')
        AND COALESCE(l.l_shipinstruct, '') = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = 'Brand#34'
        AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l.l_quantity BETWEEN 20 AND 30
        AND p.p_size BETWEEN 1 AND 15
        AND COALESCE(l.l_shipmode, '') IN ('AIR', 'AIR REG')
        AND COALESCE(l.l_shipinstruct, '') = 'DELIVER IN PERSON'
    );
""",
    # Materialized Query 20
"""SELECT
    s.s_name,
    s.s_address
FROM
    tpch s,
    tpch n
WHERE
    s.s_suppkey IN (
        SELECT
            ps.ps_suppkey
        FROM
            tpch ps
        WHERE
            ps.ps_partkey IN (
                SELECT
                    p.p_partkey
                FROM
                    tpch p
                WHERE
                    p.p_name LIKE 'forest%'
            )
            AND ps.ps_availqty > (
                SELECT
                    0.5 * SUM(l.l_quantity)
                FROM
                    tpch l
                WHERE
                    l.l_partkey = ps.ps_partkey
                    AND l.l_suppkey = ps.ps_suppkey
                    AND l.l_shipdate >= DATE '1994-01-01'
                    AND l.l_shipdate < DATE '1995-01-01'
            )
    )
    AND s.s_nationkey = n.n_nationkey
    AND COALESCE(n.n_name, '') = 'CANADA'
ORDER BY
    s.s_name;
""",
    # Materialized Query 21
"""SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    tpch s,
    tpch l1,
    tpch o,
    tpch n
WHERE
    s.s_suppkey = l1.l_suppkey
    AND o.o_orderkey = l1.l_orderkey
    AND o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            tpch l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            tpch l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s.s_nationkey = n.n_nationkey
    AND COALESCE(n.n_name, '') = 'SAUDI ARABIA'
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name
LIMIT
    100;

""",
    # Materialized Query 22
    """SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING(c.c_phone FROM 1 FOR 2) AS cntrycode,
            c.c_acctbal
        FROM
            tpch c
        WHERE
            SUBSTRING(c.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND c.c_acctbal > (
                SELECT
                    AVG(c_inner.c_acctbal)
                FROM
                    tpch c_inner
                WHERE
                    c_inner.c_acctbal > 0.00
                    AND SUBSTRING(c_inner.c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    tpch o
                WHERE
                    o.o_custkey = c.c_custkey
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
"""
]
