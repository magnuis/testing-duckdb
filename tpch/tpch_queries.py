RAW_TPCH_QUERIES = [  # Query 1: Summarizes data with aggregates like sum or avg
    """SELECT
        CAST(l.data->>'l_returnflag' AS CHAR(1)) AS l_returnflag,
        CAST(l.data->>'l_linestatus' AS CHAR(1)) AS l_linestatus,
        SUM(CAST(l.data->>'l_quantity' AS INTeger)) AS sum_qty,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2))) AS sum_base_price,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS sum_disc_price,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2))) * (1 + CAST(l.data->>'l_tax' AS DECIMAL(12,2)))) AS sum_charge,
        avg(CAST(l.data->>'l_quantity' AS INTeger)) AS avg_qty,
        avg(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2))) AS avg_price,
        avg(CAST(l.data->>'l_discount' AS DECIMAL(12,2))) AS avg_disc,
        count(*) AS count_order
FROM
        tpch l
WHERE
        CAST(l.data->>'l_shipdate' AS DATE) <= DATE '1998-12-01' - interval '90' DAY
GROUP BY
        CAST(l.data->>'l_returnflag' AS CHAR(1)),
        CAST(l.data->>'l_linestatus' AS CHAR(1))
ORDER BY
        CAST(l.data->>'l_returnflag' AS CHAR(1)),
        CAST(l.data->>'l_linestatus' AS CHAR(1));""",

    # Query 2: Sorts data based on specific columns
    # """SELECT
    #         CAST(s.data->>'s_acctbal' AS DECIMAL(12,2)) AS s_acctbal,
    #         s.data->>'s_name' AS s_name,
    #         n.data->>'n_name' AS n_name,
    #         CAST(p.data->>'p_partkey' AS INT) AS p_partkey,
    #         p.data->>'p_mfgr' AS p_mfgr,
    #         s.data->>'s_address' AS s_address,
    #         s.data->>'s_phone' AS s_phone,
    #         s.data->>'s_comment' AS s_comment
    # FROM
    #         tpch p,
    #         tpch s,
    #         tpch ps,
    #         tpch n,
    #         tpch r
    # WHERE
    #         CAST(p.data->>'p_partkey' AS INT) = CAST(ps.data->>'ps_partkey' AS INT)
    #         AND CAST(s.data->>'s_suppkey' AS INT) = CAST(ps.data->>'ps_suppkey' AS INT)
    #         AND CAST(p.data->>'p_size' AS INT) = 15
    #         AND (p.data->>'p_type') like '%BRASS'
    #         AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
    #         AND CAST(n.data->>'n_regionkey' AS INT) = CAST(r.data->>'r_regionkey' AS INT)
    #         AND (r.data->>'r_name') = 'EUROPE'
    #         AND CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) = (
    #                 SELECT
    #                         min(CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)))
    #                 FROM
    #                         tpch s,
    #                         tpch ps,
    #                         tpch n,
    #                         tpch r
    #                 WHERE
    #                         CAST(p.data->>'p_partkey' AS INT) = CAST(ps.data->>'ps_partkey' AS INT)
    #                         AND CAST(s.data->>'s_suppkey' AS INT) = CAST(ps.data->>'ps_suppkey' AS INT)
    #                         AND CAST(s.data->>'s_nationkey' AS INT)= CAST(n.data->>'n_nationkey' AS INT)
    #                         AND CAST(n.data->>'n_regionkey' AS INT) = CAST(r.data->>'r_regionkey' AS INT)
    #                         AND (r.data->>'r_name') = 'EUROPE'

    #         )
    # ORDER BY
    #         CAST(s.data->>'s_acctbal' AS DECIMAL(12,2)) desc,
    #         n.data->>'n_name',
    #         s.data->>'s_name',
    #         CAST(p.data->>'p_partkey' AS INT)
    # LIMIT
    #         100;""",
    """
SELECT
        CAST(s.data->>'s_acctbal' AS DECIMAL(12,2)) AS s_acctbal,
        s.data->>'s_name' AS s_name,
        r_n_joined.n_name,
        p_ps_joined.p_partkey,
        p_ps_joined.p_mfgr,
        s.data->>'s_address' AS s_address,
        s.data->>'s_phone' AS s_phone,
        s.data->>'s_comment' AS s_comment
FROM
        (
                SELECT
                        CAST(ps.data->>'ps_suppkey' AS INT) AS ps_suppkey,
                        CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) AS ps_supplycost,
                        CAST(p.data->>'p_partkey' AS INT) AS p_partkey,
                        (p.data->>'p_mfgr') AS p_mfgr
                FROM
                        tpch p,
                        tpch ps
                WHERE
                        CAST(p.data->>'p_size' AS INT) = 15
                        AND (p.data->>'p_type') like '%BRASS'
                        AND CAST(p.data->>'p_partkey' AS INT) = CAST(ps.data->>'ps_partkey' AS INT)
        ) AS p_ps_joined,
        (
                SELECT
                        CAST(n.data->>'n_nationkey' AS INT) AS n_nationkey,
                        (n.data->>'n_name') AS n_name
                FROM
                        tpch r,
                        tpch n
                WHERE
                        (r.data->>'r_name') = 'EUROPE'
                        AND CAST(n.data->>'n_regionkey' AS INT) = CAST(r.data->>'r_regionkey' AS INT)
        ) AS r_n_joined,
        tpch s
WHERE
        
        CAST(s.data->>'s_suppkey' AS INT) = p_ps_joined.ps_suppkey
        AND CAST(s.data->>'s_nationkey' AS INT) = r_n_joined.n_nationkey
        AND p_ps_joined.ps_supplycost = (
                SELECT
                        min(CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)))
                FROM
                        tpch s,
                        tpch ps,
                        tpch n,
                        tpch r
                WHERE
                        p_ps_joined.p_partkey = CAST(ps.data->>'ps_partkey' AS INT)
                        AND CAST(s.data->>'s_suppkey' AS INT) = CAST(ps.data->>'ps_suppkey' AS INT)
                        AND CAST(s.data->>'s_nationkey' AS INT)= CAST(n.data->>'n_nationkey' AS INT)
                        AND CAST(n.data->>'n_regionkey' AS INT) = CAST(r.data->>'r_regionkey' AS INT)
                        AND (r.data->>'r_name') = 'EUROPE'

        )
ORDER BY
        CAST(s.data->>'s_acctbal' AS DECIMAL(12,2)) desc,
        r_n_joined.n_name,
        s.data->>'s_name',
        p_ps_joined.p_partkey
LIMIT
        100;""",
    # Query 3: Summarizes data with aggregates like sum or avg
    """SELECT
        CAST(l.data->>'l_orderkey' AS INT) AS l_orderkey,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS revenue,
        CAST(o.data->>'o_orderdate' AS DATE) AS o_orderdate,
        CAST(o.data->>'o_shippriority' AS INT) AS o_shippriority
FROM
        tpch c,
        tpch o,
        tpch l
WHERE
        (c.data->>'c_mktsegment') = 'BUILDING'
        AND CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
        AND CAST(l.data->>'l_orderkey' AS INT) = CAST(o.data->>'o_orderkey' AS INT)
        AND CAST(o.data->>'o_orderdate' AS DATE) < DATE '1995-03-15'
        AND CAST(l.data->>'l_shipdate' AS DATE) > DATE '1995-03-15'
GROUP BY
        CAST(l.data->>'l_orderkey' AS INT),
        CAST(o.data->>'o_orderdate' AS DATE),
        CAST(o.data->>'o_shippriority' AS INT)
ORDER BY
        revenue desc,
        CAST(o.data->>'o_orderdate' AS DATE)
LIMIT
        10;""",

    # Query 4: Groups data based on specified columns
    """SELECT
        o.data->>'o_orderpriority' AS o_orderpriority,
        count(*) AS order_count
FROM
        tpch o
WHERE
        CAST(o.data->>'o_orderdate' AS DATE) >= DATE '1993-07-01'
        AND CAST(o.data->>'o_orderdate' AS DATE) < DATE '1993-07-01' + interval '3' month
        AND exists (
                SELECT
                        *
                FROM
                        tpch l
                WHERE
                        CAST(l.data->>'l_orderkey' AS INT) = CAST(o.data->>'o_orderkey' AS INT)
                        AND CAST(l.data->>'l_commitdate' AS DATE) < CAST(l.data->>'l_receiptdate' AS DATE)
        )
GROUP BY
        o.data->>'o_orderpriority'
ORDER BY
        o.data->>'o_orderpriority';""",

    # Query 5: Summarizes data with aggregates like sum or avg
    """SELECT
        (n.data->>'n_name') AS n_name,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS revenue
FROM
        tpch c,
        tpch o,
        tpch l,
        tpch s,
        tpch n,
        tpch r
WHERE
        CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
        AND CAST(l.data->>'l_orderkey' AS INT) = CAST(o.data->>'o_orderkey' AS INT)
        AND CAST(l.data->>'l_suppkey' AS INT) = CAST(s.data->>'s_suppkey' AS INT)
        AND CAST(c.data->>'c_nationkey' AS INT) = CAST(s.data->>'s_nationkey' AS INT)
        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
        AND CAST(n.data->>'n_regionkey' AS INT) = CAST(r.data->>'r_regionkey' AS INT)
        AND (r.data->>'r_name') = 'ASIA'
        AND CAST(o.data->>'o_orderdate' AS DATE) >= DATE '1994-01-01'
        AND CAST(o.data->>'o_orderdate' AS DATE) < DATE '1994-01-01' + interval '1' year
GROUP BY
        (n.data->>'n_name')
ORDER BY
        revenue desc;""",

    # Query 6: Summarizes data with aggregates like sum or avg
    """SELECT
       SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS revenue
FROM
        tpch l
WHERE
        CAST(l.data->>'l_shipdate' AS DATE) >= DATE '1994-01-01'
        AND CAST(l.data->>'l_shipdate' AS DATE) < DATE '1994-01-01' + interval '1' year
        AND CAST(l.data->>'l_discount' AS DECIMAL(12,2)) BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
        AND CAST(l.data->>'l_quantity' AS INT) < 24;""",

    # Query 7: Summarizes data with aggregates like sum or avg
    """
SELECT
        supp_nation,
        cust_nation,
        l_year,
        SUM(volume) AS revenue
FROM
        (
                SELECT
                        (n1.data->>'n_name') AS supp_nation,
                        (n2.data->>'n_name') AS cust_nation,
                        extract(year FROM CAST(l.data->>'l_shipdate' AS DATE)) AS l_year,
                        CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2))) AS volume
                FROM
                        tpch s,
                        tpch l,
                        tpch o,
                        tpch c,
                        tpch n1,
                        tpch n2
                WHERE
                        CAST(s.data->>'s_suppkey' AS INT) = CAST(l.data->>'l_suppkey' AS INT)
                        AND CAST(o.data->>'o_orderkey' AS INT) = CAST(l.data->>'l_orderkey' AS INT)
                        AND CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
                        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n1.data->>'n_nationkey' AS INT)
                        AND CAST(c.data->>'c_nationkey' AS INT) = CAST(n2.data->>'n_nationkey' AS INT)
                        AND (
                                ((n1.data->>'n_name') = 'FRANCE' AND (n2.data->>'n_name') = 'GERMANY')
                                or 
                                ((n1.data->>'n_name') = 'GERMANY' AND (n2.data->>'n_name') = 'FRANCE')
                        )
                        AND CAST(l.data->>'l_shipdate' AS DATE) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS shipping
GROUP BY
        supp_nation,
        cust_nation,
        l_year
ORDER BY
        supp_nation,
        cust_nation,
        l_year;""",

    # Query 8: Summarizes data with aggregates like sum or avg
    # """SELECT
    #         o_year,
    #         SUM(case
    #                 WHEN nation = 'BRAZIL' THEN volume
    #                 ELSE 0
    #         END) / SUM(volume) AS mkt_share
    # FROM
    #         (
    #                 SELECT
    #                         extract(year FROM CAST(o.data->>'o_orderdate' AS DATE)) AS o_year,
    #                         CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2))) AS volume,
    #                         (n2.data->>'n_name') AS nation
    #                 FROM
    #                         tpch p,
    #                         tpch s,
    #                         tpch l,
    #                         tpch o,
    #                         tpch c,
    #                         tpch n1,
    #                         tpch n2,
    #                         tpch r
    #                 WHERE
    #                         CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
    #                         AND CAST(s.data->>'s_suppkey' AS INT) = CAST(l.data->>'l_suppkey' AS INT)
    #                         AND CAST(l.data->>'l_orderkey' AS INT) = CAST(o.data->>'o_orderkey' AS INT)
    #                         AND CAST(o.data->>'o_custkey' AS INT) = CAST(c.data->>'c_custkey' AS INT)
    #                         AND CAST(r.data->>'r_regionkey' AS INT) = CAST(n1.data->>'n_regionkey' AS INT)
    #                         AND CAST(c.data->>'c_nationkey' AS INT) = CAST(n1.data->>'n_nationkey' AS INT)
    #                         AND (r.data->>'r_name') = 'AMERICA'
    #                         AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n2.data->>'n_nationkey' AS INT)
    #                         AND CAST(o.data->>'o_orderdate' AS DATE) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    #                         AND (p.data->>'p_type') = 'ECONOMY ANODIZED STEEL'
    #         ) AS all_nations
    # GROUP BY
    #         o_year
    # ORDER BY
    #         o_year;""",

    """SELECT
        o_year,
        SUM(case
                WHEN nation = 'BRAZIL' THEN volume
                ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
        (
                SELECT
                        extract(year FROM CAST(o.data->>'o_orderdate' AS DATE)) AS o_year,
                        p_l_joined.l_extendedprice * (1  - p_l_joined.l_discount) AS volume,
                        (n2.data->>'n_name') AS nation
                FROM
                        (
                                SELECT
                                        CAST(p.data->>'p_partkey' AS INT) AS p_partkey,
                                        CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) AS l_extendedprice,
                                        CAST(l.data->>'l_discount' AS DECIMAL(12,2)) AS l_discount,
                                        CAST(l.data->>'l_orderkey' AS INT) AS l_orderkey,
                                        CAST(l.data->>'l_partkey' AS INT) AS l_partkey,
                                        CAST(l.data->>'l_suppkey' AS INT) AS l_suppkey
                                FROM 
                                        tpch p,
                                        tpch l
                                WHERE
                                        (p.data->>'p_type') = 'ECONOMY ANODIZED STEEL'
                                        AND CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)

                        ) AS p_l_joined,
                        (
                                SELECT
                                        CAST(r.data->>'r_regionkey' AS INT) AS r_regionkey,
                                        CAST(n1.data->>'n_nationkey' AS INT) AS n_nationkey,
                                        CAST(n1.data->>'n_regionkey' AS INT) AS n_regionkey

                                FROM
                                        tpch r,
                                        tpch n1
                                WHERE
                                        (r.data->>'r_name') = 'AMERICA'
                                        AND CAST(r.data->>'r_regionkey' AS INT) = CAST(n1.data->>'n_regionkey' AS INT)
                        ) AS r_n1_joined,

                        tpch o,
                        tpch c,
                        tpch s,
                        tpch n2
                WHERE

                        CAST(s.data->>'s_suppkey' AS INT) = p_l_joined.l_suppkey
                        AND p_l_joined.l_orderkey = CAST(o.data->>'o_orderkey' AS INT)
                        AND CAST(o.data->>'o_custkey' AS INT) = CAST(c.data->>'c_custkey' AS INT)
                        AND CAST(c.data->>'c_nationkey' AS INT) = r_n1_joined.n_nationkey
                        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n2.data->>'n_nationkey' AS INT)
                        AND CAST(o.data->>'o_orderdate' AS DATE) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS all_nations
GROUP BY
        o_year
ORDER BY
        o_year;""",



    # Query 9: Summarizes data with aggregates like sum or avg
    #     """SELECT
    #             nation,
    #             o_year,
    #             SUM(amount) AS sum_profit
    #     FROM
    #             (
    #                     SELECT
    #                             (n.data->>'n_name') AS nation,
    #                             extract(year FROM CAST(o.data->>'o_orderdate' AS DATE)) AS o_year,
    #                             CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2))) - CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) * CAST(l.data->>'l_discount' AS DECIMAL(12,2)) AS amount
    #                     FROM
    #                             tpch p,
    #                             tpch s,
    #                             tpch l,
    #                             tpch ps,
    #                             tpch o,
    #                             tpch n
    #                     WHERE
    #                             CAST(s.data->>'s_suppkey' AS INT) = CAST(l.data->>'l_suppkey' AS INT)
    #                             AND CAST(ps.data->>'ps_suppkey' AS INT) = CAST(l.data->>'l_suppkey' AS INT)
    #                             AND CAST(ps.data->>'ps_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
    #                             AND CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
    #                             AND CAST(o.data->>'o_orderkey' AS INT) = CAST(l.data->>'l_orderkey' AS INT)
    #                             AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
    #                             AND (p.data->>'p_name') like '%green%'
    #             ) AS profit
    #     GROUP BY
    #             nation,
    #             o_year
    #     ORDER BY
    #             nation,
    #             o_year desc;""",

    """SELECT
        nation,
        o_year,
        SUM(amount) AS sum_profit
FROM
        (
                SELECT
                        (n.data->>'n_name') AS nation,
                        extract(year FROM CAST(o.data->>'o_orderdate' AS DATE)) AS o_year,
                        lp_joined.l_extendedprice * (1 - lp_joined.l_discount) - CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) * lp_joined.l_discount AS amount
                FROM
                        (
                                select 
                                        CAST(p.data->>'p_partkey' AS INT) AS p_partkey,
                                        CAST(l.data->>'l_suppkey' AS INT) AS l_suppkey,
                                        CAST(l.data->>'l_partkey' AS INT) AS l_partkey,
                                        CAST(l.data->>'l_orderkey' AS INT) AS l_orderkey,
                                        CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) AS l_extendedprice,
                                        CAST(l.data->>'l_discount' AS DECIMAL(12,2)) AS l_discount

                                FROM
                                        tpch p,
                                        tpch l
                                WHERE
                                        (p.data->>'p_name') like '%green%'
                                        AND CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
                        ) AS lp_joined,
                        tpch s,
                        tpch ps,
                        tpch o,
                        tpch n
                WHERE
                        CAST(s.data->>'s_suppkey' AS INT) = lp_joined.l_suppkey
                        AND CAST(ps.data->>'ps_suppkey' AS INT) = lp_joined.l_suppkey
                        AND CAST(ps.data->>'ps_partkey' AS INT) = lp_joined.l_partkey
                        AND CAST(o.data->>'o_orderkey' AS INT) = lp_joined.l_orderkey
                        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
        ) AS profit
GROUP BY
        nation,
        o_year
ORDER BY
        nation,
        o_year desc;""",

    # Query 10: Summarizes data with aggregates like sum or avg
    """SELECT
        CAST(c.data->>'c_custkey' AS INT) AS c_custkey,
        c.data->>'c_name' AS c_name,
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS revenue,
        CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)) AS c_acctbal,
        (n.data->>'n_name') AS c_name,
        (c.data->>'c_address') AS c_address,
        (c.data->>'c_phone') AS c_phone,
        (c.data->>'c_comment') AS c_comment
FROM
        tpch c,
        tpch o,
        tpch l,
        tpch n
WHERE
        CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
        AND CAST(l.data->>'l_orderkey' AS INT) = CAST(o.data->>'o_orderkey' AS INT)
        AND CAST(o.data->>'o_orderdate' AS DATE) >= DATE '1993-10-01'
        AND CAST(o.data->>'o_orderdate' AS DATE) < DATE '1993-10-01' + interval '3' month
        AND CAST(l.data->>'l_returnflag' AS CHAR(1)) = 'R'
        AND CAST(c.data->>'c_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
GROUP BY
        CAST(c.data->>'c_custkey' AS INT),
        (c.data->>'c_name'),
        CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)),
        (c.data->>'c_phone'),
        (n.data->>'n_name'),
        (c.data->>'c_address'),
        (c.data->>'c_comment')
ORDER BY
        revenue desc
LIMIT
        20;""",

    # Query 11: Summarizes data with aggregates like sum or avg
    """SELECT
        CAST(ps.data->>'ps_partkey' AS INT) AS ps_partkey,
        SUM(CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) * CAST(ps.data->>'ps_availqty' AS INT)) AS value
FROM
        tpch ps,
        tpch s,
        tpch n
WHERE
        CAST(ps.data->>'ps_suppkey' AS INT) = CAST(s.data->>'s_suppkey' AS INT)
        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
        AND (n.data->>'n_name') = 'GERMANY'
GROUP BY
        CAST(ps.data->>'ps_partkey' AS INT) having
                SUM(CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) * CAST(ps.data->>'ps_availqty' AS INT)) > (
                        SELECT
                                SUM(CAST(ps.data->>'ps_supplycost' AS DECIMAL(12,2)) * CAST(ps.data->>'ps_availqty' AS INT)) * 0.0001
                        FROM
                                tpch ps,
                                tpch s,
                                tpch n
                        WHERE
                                CAST(ps.data->>'ps_suppkey' AS INT) = CAST(s.data->>'s_suppkey' AS INT)
                                AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
                                AND (n.data->>'n_name') = 'GERMANY'
                )
ORDER BY
        value desc;""",

    # Query 12: Summarizes data with aggregates like sum or avg
    """SELECT
       (l.data->>'l_shipmode') AS l_shipmode,
        SUM(case
                WHEN (o.data->>'o_orderpriority') = '1-URGENT'
                        or (o.data->>'o_orderpriority') = '2-HIGH'
                        THEN 1
                ELSE 0
        END) AS high_line_count,
        SUM(case
                WHEN (o.data->>'o_orderpriority') <> '1-URGENT'
                        AND (o.data->>'o_orderpriority') <> '2-HIGH'
                        THEN 1
                ELSE 0
        END) AS low_line_count
FROM
        tpch o,
        tpch l
WHERE
        CAST(o.data->>'o_orderkey' AS INT) = CAST(l.data->>'l_orderkey' AS INT)
        AND (l.data->>'l_shipmode') in ('MAIL', 'SHIP')
        AND CAST(l.data->>'l_commitdate' AS DATE) < CAST(l.data->>'l_receiptdate' AS DATE)
        AND CAST(l.data->>'l_shipdate' AS DATE) < CAST(l.data->>'l_commitdate' AS DATE)
        AND CAST(l.data->>'l_receiptdate' AS DATE) >= DATE '1994-01-01'
        AND CAST(l.data->>'l_receiptdate' AS DATE) < DATE '1994-01-01' + interval '1' year
GROUP BY
        (l.data->>'l_shipmode')
ORDER BY
        (l.data->>'l_shipmode');""",

    # Query 13: Groups data based on specified columns
    """SELECT
        c_count,
        count(*) AS custdist
FROM
        (
                SELECT
                        CAST(c.data->>'c_custkey' AS INT),
                        count(CAST(o.data->>'o_orderkey' AS INT))
                FROM
                        tpch c left outer join tpch o on
                                CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
                                AND (o.data->>'o_comment') not like '%special%requests%'
                GROUP BY
                        CAST(c.data->>'c_custkey' AS INT)
        ) AS c_orders (c_custkey, c_count)
GROUP BY
        c_count
ORDER BY
        custdist desc,
        c_count desc;""",

    # Query 14: Summarizes data with aggregates like sum or avg
    """SELECT
        100.00 * SUM(case
                WHEN (p.data->>'p_type') like 'PROMO%'
                        THEN CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))
                ELSE 0
        END) / SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS promo_revenue
FROM
        tpch l,
        tpch p
WHERE
        CAST(l.data->>'l_partkey' AS INT) = CAST(p.data->>'p_partkey' AS INT)
        AND CAST(l.data->>'l_shipdate' AS DATE) >= DATE '1995-09-01'
        AND CAST(l.data->>'l_shipdate' AS DATE) < DATE '1995-09-01' + interval '1' month;""",

    # Query 15: Summarizes data with aggregates like sum or avg
    """with revenue (supplier_no, total_revenue) AS (
        SELECT
                CAST(l.data->>'l_suppkey' AS INT),
                SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2))))
        FROM
               tpch l
        WHERE
                CAST(l.data->>'l_shipdate' AS DATE) >= DATE '1996-01-01'
                AND CAST(l.data->>'l_shipdate' AS DATE) < DATE '1996-01-01' + interval '3' month
        GROUP BY
                CAST(l.data->>'l_suppkey' AS INT))
SELECT
        CAST(s.data->>'s_suppkey' AS INT) AS s_suppkey,
        (s.data->>'s_name') AS s_name,
        (s.data->>'s_address') AS s_address,
        (s.data->>'s_phone') AS s_phone,
        total_revenue
FROM
        tpch s,
        revenue
WHERE
        CAST(s.data->>'s_suppkey' AS INT) = supplier_no
        AND total_revenue = (
                SELECT
                        max(total_revenue)
                FROM
                        revenue
        )
ORDER BY
        CAST(s.data->>'s_suppkey' AS INT);""",

    # Query 16: Groups data based on specified columns
    """SELECT
        (p.data->>'p_brand') AS p_brand,
        (p.data->>'p_type') AS p_type,
        CAST(p.data->>'p_size' AS INT) AS p_size,
        count(distinct CAST(ps.data->>'ps_suppkey' AS INT)) AS supplier_cnt
FROM
        tpch ps,
        tpch p
WHERE
        CAST(p.data->>'p_partkey' AS INT) = CAST(ps.data->>'ps_partkey' AS INT)
        AND (p.data->>'p_brand') <> 'Brand#45'
        AND (p.data->>'p_type') not like 'MEDIUM POLISHED%'
        AND CAST(p.data->>'p_size' AS INT) in (49, 14, 23, 45, 19, 3, 36, 9)
        AND CAST(ps.data->>'ps_suppkey' AS INT) not in (
                SELECT
                        CAST(s.data->>'s_suppkey' AS INT)
                FROM
                        tpch s
                WHERE
                        (s.data->>'s_comment') like '%Customer%Complaints%'
        )
GROUP BY
        (p.data->>'p_brand'),
        (p.data->>'p_type'),
        CAST(p.data->>'p_size' AS INT)
ORDER BY
        supplier_cnt desc,
        (p.data->>'p_brand'),
        (p.data->>'p_type'),
        CAST(p.data->>'p_size' AS INT)""",

    # Query 17: Summarizes data with aggregates like sum or avg
    """SELECT
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2))) / 7.0 AS avg_yearly
FROM
        tpch l,
        tpch p
WHERE
        CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
        AND (p.data->>'p_brand') = 'Brand#23'
        AND (p.data->>'p_container') = 'MED BOX'
        AND CAST(l.data->>'l_quantity' AS INT) < (
                SELECT
                        0.2 * avg(CAST(l.data->>'l_quantity' AS INT))
                FROM
                        tpch l
                WHERE
                        CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
        );""",

    # Query 18: Summarizes data with aggregates like sum or avg
    """SELECT
        (c.data->>'c_name') AS c_name,
        CAST(c.data->>'c_custkey' AS INT) AS c_custkey,
        CAST(o.data->>'o_orderkey' AS INT) AS o_orderkey,
        CAST(o.data->>'o_orderdate' AS DATE) AS o_orderdate,
        CAST(o.data->>'o_totalprice' AS DECIMAL(12,2)) AS o_totalprice,
        SUM(CAST(l.data->>'l_quantity' AS INT)) AS l_quantity
FROM
        tpch c,
        tpch o,
        tpch l
WHERE
        CAST(o.data->>'o_orderkey' AS INT) in (
                SELECT
                        CAST(l.data->>'l_orderkey' AS INT)
                FROM
                        tpch l
                GROUP BY
                        CAST(l.data->>'l_orderkey' AS INT) having
                                SUM(CAST(l.data->>'l_quantity' AS INT)) > 300
        )
        AND CAST(c.data->>'c_custkey' AS INT) = CAST(o.data->>'o_custkey' AS INT)
        AND CAST(o.data->>'o_orderkey' AS INT) = CAST(l.data->>'l_orderkey' AS INT)
GROUP BY
        (c.data->>'c_name'),
        CAST(c.data->>'c_custkey' AS INT),
        CAST(o.data->>'o_orderkey' AS INT),
        CAST(o.data->>'o_orderdate' AS DATE),
        CAST(o.data->>'o_totalprice' AS DECIMAL(12,2))
ORDER BY
        CAST(o.data->>'o_totalprice' AS DECIMAL(12,2)) desc,
        CAST(o.data->>'o_orderdate' AS DATE)
LIMIT
        100;""",

    # Query 19: Summarizes data with aggregates like sum or avg
    """SELECT
        SUM(CAST(l.data->>'l_extendedprice' AS DECIMAL(12,2)) * (1 - CAST(l.data->>'l_discount' AS DECIMAL(12,2)))) AS revenue
FROM
        tpch l,
        tpch p
WHERE
        (
                CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
                AND (p.data->>'p_brand') = 'Brand#12'
                AND (p.data->>'p_container') in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND CAST(l.data->>'l_quantity' AS INT) >= 1 AND CAST(l.data->>'l_quantity' AS INT) <= 1 + 10
                AND CAST(p.data->>'p_size' AS INT) BETWEEN 1 AND 5
                AND (l.data->>'l_shipmode') in ('AIR', 'AIR REG')
                AND (l.data->>'l_shipinstruct') = 'DELIVER IN PERSON'
        )
        or
        (
                CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
                AND (p.data->>'p_brand') = 'Brand#23'
                AND (p.data->>'p_container') in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND CAST(l.data->>'l_quantity' AS INT) >= 10 AND CAST(l.data->>'l_quantity' AS INT) <= 10 + 10
                AND CAST(p.data->>'p_size' AS INT) BETWEEN 1 AND 10
                AND (l.data->>'l_shipmode') in ('AIR', 'AIR REG')
                AND (l.data->>'l_shipinstruct') = 'DELIVER IN PERSON'
        )
        or
        (
                CAST(p.data->>'p_partkey' AS INT) = CAST(l.data->>'l_partkey' AS INT)
                AND (p.data->>'p_brand') = 'Brand#34'
                AND (p.data->>'p_container') in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND CAST(l.data->>'l_quantity' AS INT) >= 20 AND CAST(l.data->>'l_quantity' AS INT) <= 20 + 10
                AND CAST(p.data->>'p_size' AS INT) BETWEEN 1 AND 15
                AND (l.data->>'l_shipmode') in ('AIR', 'AIR REG')
                AND (l.data->>'l_shipinstruct') = 'DELIVER IN PERSON'
        );""",

    # Query 20: Summarizes data with aggregates like sum or avg
    """SELECT
        (s.data->>'s_name') AS s_name,
        (s.data->>'s_address') AS s_address
FROM
        tpch s,
        tpch n
WHERE
        CAST(s.data->>'s_suppkey' AS INT) in (
                SELECT
                        CAST(ps.data->>'ps_suppkey' AS INT)
                FROM
                        tpch ps
                WHERE
                        CAST(ps.data->>'ps_partkey' AS INT) in (
                                SELECT
                                        CAST(p.data->>'p_partkey' AS INT)
                                FROM
                                        tpch p
                                WHERE
                                        (p.data->>'p_name') like 'forest%'
                        )
                        AND CAST(ps.data->>'ps_availqty' AS INT) > (
                                SELECT
                                        0.5 * SUM(CAST(l.data->>'l_quantity' AS INT))
                                FROM
                                        tpch l
                                WHERE
                                        CAST(l.data->>'l_partkey' AS INT) = CAST(ps.data->>'ps_partkey' AS INT)
                                        AND CAST(l.data->>'l_suppkey' AS INT) = CAST(ps.data->>'ps_suppkey' AS INT)
                                        AND CAST(l.data->>'l_shipdate' AS DATE) >= DATE '1994-01-01'
                                        AND CAST(l.data->>'l_shipdate' AS DATE) < DATE '1994-01-01' + interval '1' year
                        )
        )
        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
        AND (n.data->>'n_name') = 'CANADA'
ORDER BY
        (s.data->>'s_name');""",

    # Query 21: Groups data based on specified columns
    """SELECT
        (s.data->>'s_name') AS s_name,
        count(*) AS numwait
FROM
        tpch s,
        tpch l1,
        tpch o,
        tpch n
WHERE
        CAST(s.data->>'s_suppkey' AS INT) = CAST(l1.data->>'l_suppkey' AS INT)
        AND CAST(o.data->>'o_orderkey' AS INT) = CAST(l1.data->>'l_orderkey' AS INT)
        AND CAST(o.data->>'o_orderstatus' AS CHAR(1)) = 'F'
        AND CAST(l1.data->>'l_receiptdate' AS DATE) > CAST(l1.data->>'l_commitdate' AS DATE)
        AND exists (
                SELECT
                        *
                FROM
                        tpch l2
                WHERE
                        CAST(l2.data->>'l_orderkey' AS INT) = CAST(l1.data->>'l_orderkey' AS INT)
                        AND CAST(l2.data->>'l_suppkey' AS INT) <> CAST(l1.data->>'l_suppkey' AS INT)
        )
        AND not exists (
                SELECT
                        *
                FROM
                        tpch l3
                WHERE
                        CAST(l3.data->>'l_orderkey' AS INT) = CAST(l1.data->>'l_orderkey' AS INT)
                        AND CAST(l3.data->>'l_suppkey' AS INT) <> CAST(l1.data->>'l_suppkey' AS INT)
                        AND CAST(l3.data->>'l_receiptdate' AS DATE) > CAST(l3.data->>'l_commitdate' AS DATE)
        )
        AND CAST(s.data->>'s_nationkey' AS INT) = CAST(n.data->>'n_nationkey' AS INT)
        AND (n.data->>'n_name') = 'SAUDI ARABIA'
GROUP BY
        (s.data->>'s_name')
ORDER BY
        numwait desc,
        (s.data->>'s_name')
LIMIT
        100;""",
    # Query 22: Summarizes data with aggregates like sum or avg
    """SELECT
        cntrycode,
        count(*) AS numcust,
        SUM(c_acctbal) AS totacctbal
FROM
        (
                SELECT
                        substring(c.data->>'c_phone' FROM 1 for 2) AS cntrycode,
                        CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)) AS c_acctbal
                FROM
                        tpch c
                WHERE
                        substring(c.data->>'c_phone' FROM 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        AND CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)) > (
                                SELECT
                                        avg(CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)))
                                FROM
                                        tpch c
                                WHERE
                                        CAST(c.data->>'c_acctbal' AS DECIMAL(12,2)) > 0.00
                                        AND substring(c.data->>'c_phone' FROM 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        AND not exists (
                                SELECT
                                        *
                                FROM
                                        tpch o
                                WHERE
                                        CAST(o.data->>'o_custkey' AS INT) = CAST(c.data->>'c_custkey' AS INT)
                        )
        ) AS custsale
GROUP BY
        cntrycode
ORDER BY
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
    AND p.p_type LIKE '%BRASS'
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND r.r_name = 'EUROPE'
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
    AND r.r_name = 'ASIA'
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
            AND p.p_type = 'ECONOMY ANODIZED STEEL'
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
            AND p.p_name LIKE '%green%'
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
    AND n.n_name = 'GERMANY'
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
            AND n_inner.n_name = 'GERMANY'
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
                AND o.o_comment NOT LIKE '%special%requests%'
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
    AND p.p_brand <> 'Brand#45'
    AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
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
    AND p.p_container = 'MED BOX'
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
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = 'Brand#23'
        AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l.l_quantity BETWEEN 10 AND 20
        AND p.p_size BETWEEN 1 AND 10
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_partkey = l.l_partkey
        AND p.p_brand = 'Brand#34'
        AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l.l_quantity BETWEEN 20 AND 30
        AND p.p_size BETWEEN 1 AND 15
        AND l.l_shipmode IN ('AIR', 'AIR REG')
        AND l.l_shipinstruct = 'DELIVER IN PERSON'
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
    AND n.n_name = 'CANADA'
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
    AND n.n_name = 'SAUDI ARABIA'
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
