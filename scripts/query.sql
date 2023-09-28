select top 100
  c_name, c_custkey, o_orderkey, o_orderdate, 
  o_totalprice, sum(l_quantity)
from snowflake_sample_data.tpch_sf1.customer
  inner join snowflake_sample_data.tpch_sf1.orders
  on c_custkey = o_custkey
  inner join snowflake_sample_data.tpch_sf1.lineitem
  on o_orderkey = l_orderkey
where o_orderkey in (
    select l_orderkey
    from snowflake_sample_data.tpch_sf1.lineitem
    group by l_orderkey
    having sum(l_quantity) > 0)
  and o_orderdate >= dateadd(year, -30, current_date)
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice 
order by o_totalprice desc, o_orderdate;