"""
Created By:    Cristian Scutaru
Creation Date: Sep 2023
Company:       XtractPro Software
"""

import configparser, json, os
import urllib.parse
#from html import escape
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

def getGraph(rows):
    nodes = ""
    edges = ""
    queryId = None
    for row in rows:
        queryId = str(row['QUERY_ID']) 
        nodeId = str(row['OPERATOR_ID'])
        parentIds = [] if row['PARENT_OPERATORS'] is None else json.loads(str(row['PARENT_OPERATORS']))
        step = str(row['OPERATOR_TYPE'])
        nodes += (f'  n{nodeId} [\n'
            + f'    style="filled" shape="record" color="SkyBlue"\n'
            + f'    fillcolor="#d3dcef:#ffffff" color="#716f64" penwidth="1"\n'
            + f'    label=<<table style="rounded" border="0" cellborder="0" cellspacing="0" cellpadding="1">\n'
            + f'      <tr><td bgcolor="transparent" align="center"><font color="#000000"><b>{step}</b></font></td></tr>\n')

        oper = json.loads(str(row['OPERATOR_ATTRIBUTES']))
        if 'table_name' in oper:
            nodes += f'      <tr><td align="left"><font color="#000000">table_name: {oper["table_name"]}</font></td></tr>\n'
        #if 'filter_condition' in oper:
        #    nodes += f'      <tr><td align="left"><font color="#000000">filter_condition: {escape(oper["filter_condition"])}</font></td></tr>\n'
        if 'join_type' in oper:
            nodes += f'      <tr><td align="left"><font color="#000000">join_type: {oper["join_type"]}</font></td></tr>\n'
        if 'join_id' in oper:
            edges += f'  n{nodeId} -> n{oper["join_id"]} [  dir="forward" style="dashed" ];\n'

        execTime = json.loads(str(row['EXECUTION_TIME_BREAKDOWN']))
        if 'overall_percentage' in execTime:
            overall_percentage = float(execTime['overall_percentage'])
            if overall_percentage  > 0.0:
                nodes += f'      <tr><td align="left"><font color="#000000">overall_percentage: {"{0:.1%}".format(overall_percentage)}</font></td></tr>\n'
            if 'remote_disk_io' in execTime:
                nodes += f'      <tr><td align="left"><font color="#000000">remote_disk_io: {"{0:.0%}".format(execTime["remote_disk_io"])}</font></td></tr>\n'

        stats = json.loads(str(row['OPERATOR_STATISTICS']))
        if 'io' in stats:
            io = stats["io"]
            if 'bytes_scanned' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">bytes_scanned: {io["bytes_scanned"]:,}</font></td></tr>\n'
            if 'percentage_scanned_from_cache' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">percentage_scanned_from_cache: {"{0:.2%}".format(io["percentage_scanned_from_cache"])}</font></td></tr>\n'
            if 'bytes_written_to_result' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">bytes_written_to_result: {io["bytes_written_to_result"]:,}</font></td></tr>\n'

        if 'pruning' in stats:
            nodes += f'      <tr><td align="left"><font color="#000000">partitions_scanned: {stats["pruning"]["partitions_scanned"]}</font></td></tr>\n'
            nodes += f'      <tr><td align="left"><font color="#000000">partitions_total: {stats["pruning"]["partitions_total"]}</font></td></tr>\n'

        nodes += f'    </table>>\n  ]\n'

        if 'input_rows' in stats:
            nodes += f'  i{nodeId} [ label="{stats["input_rows"]:,}" style="filled" shape="oval" fillcolor="#ffffff" ]\n'
            edges += f'  n{nodeId} -> i{nodeId};\n'

        for parentId in parentIds:
            edges += f'  i{parentId} -> n{nodeId};\n'

    return ('digraph {\n'
        + f'  graph [ rankdir="TB" bgcolor="#ffffff" ]\n'
        + f'  edge [ penwidth="1" color="#696969" dir="back" style="solid" ]\n\n'
        + f'{nodes}\n{edges}}}\n'), queryId


@st.cache_data(show_spinner="Reading metadata...")
def getQuery(queryId):
    s = f"'{queryId}'" if len(queryId) > 0 else "last_query_id()"
    query = f"select * from table(get_query_operator_stats({s}))"
    #st.write(query)
    return session.sql(query).collect()


def getSession():
    try:
        return get_active_session()
    except:
        parser = configparser.ConfigParser()
        parser.read(os.path.join(os.path.expanduser('~'), ".snowsql/config"))
        section = "connections.my_conn"
        pars = {
            "account": parser.get(section, "accountname"),
            "user": parser.get(section, "username"),
            "password": parser.get(section, "password")
        }
        return Session.builder.configs(pars).create()


st.set_page_config(layout="wide")
st.title("Custom Query Profiler")

queryId = st.sidebar.text_input('Query ID')
btn = st.sidebar.button("Profile")

session = getSession()
rows = getQuery(queryId)
graph, lastQueryId = getGraph(rows)
s = '' if len(queryId) > 0 else ' last'
st.markdown(f"Generates a custom query profile graph for the{s} query ID **{{{lastQueryId}}}**")

tabGraph, tabCode = st.tabs(["Query Profile Graph", "Generated DOT Script"])

with tabGraph:
    st.caption("This is your custom query profile graph, with GraphViz.")
    st.graphviz_chart(graph, use_container_width=True)

with tabCode:
    st.caption("This is the DOT script generated for your previous GraphViz chart.")
    link = f'http://magjac.com/graphviz-visual-editor/?dot={urllib.parse.quote(graph)}'
    st.link_button("Test this online!", link)
    st.code(graph, language="dot", line_numbers=True)
