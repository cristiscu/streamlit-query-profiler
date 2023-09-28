Streamlit Custom Query Profiler
===============================

Simple tool to connect to Snowflake and generate a graph that shows the query profile for a query ID. It runs by default for the ID of the last executed query.

Sample Query
------------

In a Snowflake worksheet, I ran the following query:

```
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
```
Query Profile in Snowflake
--------------------------

In Snowflake, click on the query ID of the executed query, and you go to the built-in Query Profile. Here is a first partial image for the executed query:

![Query Profile in Snowflake](/images/query_profile.png)

And another partial view:

![Query Profile in Snowflake - Another View](/images/query_profile2.png)

My Custom Query Profile
-----------------------

In our custom profiler, enter the same query ID. Or just leave it empty, if that was the last executed query. Here is a first partial view of the previous query:

![Top Portion of Custom Query Profile](/images/diagram1.png)

The middle portion:

![Middle Portion of Custom Query Profile](/images/diagram2.png)

The bottom portion:

![Bottom Portion of Custom Query Profile](/images/diagram3.png)

Setup Instructions
------------------
* Clone the repository locally, and open it in VSCode or some other IDE.
* Create and activate a virtual environment. Install all the dependencies from the **requirements.txt** file.
Install **SnowSQL**. Locate the **~/.snowsql/config** file.
Add a **[connection.my_conn]** section with the Snowflake *accountname*, *username* and *password*.
* Run **`streamlit run app.py`**, to test the app locally. Terminate the web session with CTRL+C.
* In **scripts/deploy.sql**, update the path to your **app.py** local file in the PUT statement. Then run **`snowsql -c my_conn -f scripts/deploy.sql`**, which will (re)create a **STREAMLIT_QUERY_PROFILER** database, will load the Python file in a stage, and will create a Streamlit app from this file. Test and run the Streamlit app online. Share a link to this app with other roles or users.
