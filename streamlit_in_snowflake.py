# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark as sp
import pandas as pd
import json
import numpy as np
import re

try:
    # Set the page layout to wide
    st.set_page_config(
        layout="wide",
    )
except:
    pass
   
disclaimer = """Disclaimer: Use at your own discretion. This site does not store your Snowflake credentials and your credentials are only used as a passthrough to connect to your Snowflake account."""

def main():
    pass


def resize_wh(warehouse_name, warehouse_size):
    session.sql(f"""ALTER WAREHOUSE "{warehouse_name}" SET WAREHOUSE_SIZE={warehouse_size}""").collect()

if __name__ == "__main__":
    try:

        st.title("Streamlit: Snowflake Query Profiling")


        # Get the current credentials
        session = get_active_session()     
        # Set up general laytout
        settings = st.columns(4)
        query_id = st.text_input("Query Id", help="To go back, clear out the text input")
        clear = st.columns(4)
        aggcharts = st.columns(4)
        container = st.container()
        st.write("")
        footer = st.container()

        with footer:
            st.caption(f"""©2023 by Andrew Kim (andrew.kim@snowflake.com)""")
            st.caption(f"Streamlit Version: {st.__version__}")
            st.caption(f"Snowpark Version: {sp.__version__}")


        def resize_wh(warehouse_name, warehouse_size):
            session.sql(f"""ALTER WAREHOUSE "{warehouse_name}" SET WAREHOUSE_SIZE={warehouse_size}""").collect()

        try:
            # Get query history over last 14 days
            qh = session.sql("""SELECT 
                    QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,
                    q.USER_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,
                    QUERY_TAG,EXECUTION_STATUS,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,
                    BYTES_SCANNED,PERCENTAGE_SCANNED_FROM_CACHE,BYTES_WRITTEN,BYTES_WRITTEN_TO_RESULT,
                    BYTES_READ_FROM_RESULT,ROWS_PRODUCED,ROWS_INSERTED,ROWS_UPDATED,ROWS_DELETED,
                    ROWS_UNLOADED,BYTES_DELETED,PARTITIONS_SCANNED,PARTITIONS_TOTAL,
                    BYTES_SPILLED_TO_LOCAL_STORAGE,BYTES_SPILLED_TO_REMOTE_STORAGE,
                    BYTES_SENT_OVER_THE_NETWORK,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,
                    QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,CREDITS_USED_CLOUD_SERVICES,
                    QUERY_LOAD_PERCENT,IS_CLIENT_GENERATED_STATEMENT,CLIENT_APPLICATION_ID,
                    PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION::string as APPLICATION_NAME
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
                INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s ON q.SESSION_ID = s.SESSION_ID
                WHERE EXECUTION_STATUS = 'SUCCESS' AND END_TIME >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '14 DAY' 
                AND WAREHOUSE_NAME IS NOT NULL AND WAREHOUSE_SIZE IS NOT NULL
                ORDER BY END_TIME DESC""").to_pandas()

            qh["TOTAL_ELAPSED_TIME"] = qh["TOTAL_ELAPSED_TIME"].astype(int)
            qh["COMPILATION_TIME"] = qh["COMPILATION_TIME"].astype(int)
            qh["EXECUTION_TIME"] = qh["EXECUTION_TIME"].astype(int)
            qh["QUEUED_PROVISIONING_TIME"] = qh["QUEUED_PROVISIONING_TIME"].astype(int)
            qh["QUEUED_REPAIR_TIME"] = qh["QUEUED_REPAIR_TIME"].astype(int)
            qh["QUEUED_OVERLOAD_TIME"] = qh["QUEUED_OVERLOAD_TIME"].astype(int)
            qh["QUERY_LOAD_PERCENT"] = qh["QUERY_LOAD_PERCENT"].astype(int)
            qh["START_TIME"] = pd.to_datetime(qh["START_TIME"])
            qh["END_TIME"] = pd.to_datetime(qh["END_TIME"])
            qh["BYTES_WRITTEN"] = qh["BYTES_WRITTEN"].astype(int)
            qh["BYTES_WRITTEN_TO_RESULT"] = qh["BYTES_WRITTEN_TO_RESULT"].astype(int)
            qh["BYTES_READ_FROM_RESULT"] = qh["BYTES_READ_FROM_RESULT"].astype(int)
            qh["BYTES_READ_FROM_RESULT"] = qh["BYTES_READ_FROM_RESULT"].astype(int)
            qh["ROWS_INSERTED"] = qh["ROWS_INSERTED"].astype(int)
            qh["ROWS_UPDATED"] = qh["ROWS_UPDATED"].astype(int)
            qh["ROWS_DELETED"] = qh["ROWS_DELETED"].astype(int)
            qh["ROWS_UNLOADED"] = qh["ROWS_UNLOADED"].astype(int)
            qh["BYTES_DELETED"] = qh["BYTES_DELETED"].astype(int)
            qh["PARTITIONS_SCANNED"] = qh["PARTITIONS_SCANNED"].astype(int)
            qh["PARTITIONS_TOTAL"] = qh["PARTITIONS_TOTAL"].astype(int)
            qh["BYTES_SPILLED_TO_LOCAL_STORAGE"] = qh["BYTES_SPILLED_TO_LOCAL_STORAGE"].astype(int)


            filtered_qh = qh
        except:
            pass

        # Add dropdown to change pie charts between count, duration, or bubble chart w/ count & duration
        with settings[3]:
            # Only show if no Query ID has been provided
            if(query_id == ''):
                sumby = st.selectbox(
                'Show by',
                ('Count', 'Total Duration', 'Count & Duration'))

        #region This builds the pie charts
        index = 0
        groupbys = ["WAREHOUSE_NAME", "USER_NAME", "QUERY_TYPE", "APPLICATION_NAME"]
        for groupby in groupbys:
            with aggcharts[index]:
                # Only show if no Query ID has been provided
                if query_id == '':
                    try:
                        df = qh.groupby(groupby).agg({'TOTAL_ELAPSED_TIME': 'sum', 'QUERY_ID': 'count'}).reset_index().rename(columns={'QUERY_ID':'Count of Queries'})
                        df['Query Time (hr)'] = df['TOTAL_ELAPSED_TIME']/1000/60/60
                    
                        if sumby == 'Count':
                            st.vega_lite_chart(df, {
                                "mark": {"type": "arc", "tooltip": True},
                                "encoding": {
                                "theta": {"field": "Count of Queries", "type": "quantitative", "stack": "normalize"},
                                "color": {"field": groupby, "type": "nominal", "legend":{"orient":"bottom"}},
                                "order": {"field": "Count of Queries", "type": "quantitative", "sort": "descending"}
                            }
                            }, use_container_width=True)
                        elif sumby == 'Total Duration':
                            st.vega_lite_chart(df, {
                                "mark": {"type": "arc", "tooltip": True},
                                "encoding": {
                                "theta": {"field": "Query Time (hr)", "type": "quantitative", "stack": "normalize"},
                                "color": {"field": groupby, "type": "nominal", "legend":{"orient":"bottom"}},
                                "order": {"field": "Count of Queries", "type": "quantitative", "sort": "descending"}
                            }
                            }, use_container_width=True)
                        else:
                            st.vega_lite_chart(df, {
                                "mark": {"type": "point", "tooltip": True},
                                "encoding": {
                                "x": {
                                    "field": "Count of Queries",
                                    "type": "quantitative",
                                    "scale": {"zero": False}
                                },
                                "y": {
                                    "field": "Query Time (hr)",
                                    "type": "quantitative",
                                    "scale": {"zero": False}
                                },
                                "color": {"field": groupby, "type": "nominal", "legend":{"orient":"bottom"}},
                            }
                            }, use_container_width=True)
                
                        # filter = st.selectbox(
                        #     f"{groupby}",
                        #     ['All'] + df[groupby].to_list())
                        # if filter != 'All':
                        #     filtered_qh = filtered_qh[df[groupby] == filter] 
                    except:
                        pass
                    
            index = index + 1
        #endregion


        with container:
            if(query_id == ""):
                # If no Query ID has been provided, show table of most recent 50 queries
                st.write(filtered_qh)
            else:
                pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if not re.match(pattern, query_id):
                    st.error("Please enter a valid query id")
                    st.stop()
                
                try:
                    st.caption("")
                    #region Specific query detail
                    # If Query ID has been provided, filter query history on the provided query_id
                    # query = qh[qh["QUERY_ID"] == query_id].reset_index()
                    query = session.sql(f"""SELECT 
                        QUERY_ID,QUERY_TEXT,DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE,
                        q.USER_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,
                        QUERY_TAG,EXECUTION_STATUS,START_TIME,END_TIME,TOTAL_ELAPSED_TIME,
                        BYTES_SCANNED,PERCENTAGE_SCANNED_FROM_CACHE,BYTES_WRITTEN,BYTES_WRITTEN_TO_RESULT,
                        BYTES_READ_FROM_RESULT,ROWS_PRODUCED,ROWS_INSERTED,ROWS_UPDATED,ROWS_DELETED,
                        ROWS_UNLOADED,BYTES_DELETED,PARTITIONS_SCANNED,PARTITIONS_TOTAL,
                        BYTES_SPILLED_TO_LOCAL_STORAGE,BYTES_SPILLED_TO_REMOTE_STORAGE,
                        BYTES_SENT_OVER_THE_NETWORK,COMPILATION_TIME,EXECUTION_TIME,QUEUED_PROVISIONING_TIME,
                        QUEUED_REPAIR_TIME,QUEUED_OVERLOAD_TIME,CREDITS_USED_CLOUD_SERVICES,
                        QUERY_LOAD_PERCENT,IS_CLIENT_GENERATED_STATEMENT,CLIENT_APPLICATION_ID,
                        PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION::string as APPLICATION_NAME
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
                    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s ON q.SESSION_ID = s.SESSION_ID
                    WHERE QUERY_ID='{query_id}'
                    ORDER BY END_TIME DESC""").to_pandas()
                    
            
                    # Show a simplified time and convert milliseconds to larger time increment
                    simple_time = ""
                    if query['TOTAL_ELAPSED_TIME'][0] > (1000*60*60):
                        simple_time = f" ({round(query['TOTAL_ELAPSED_TIME'][0]/1000/60/60,2)} hrs)"
                    elif query['TOTAL_ELAPSED_TIME'][0] > (1000*60):
                        simple_time = f" ({round(query['TOTAL_ELAPSED_TIME'][0]/1000/60,2)} min)"
                    elif query['TOTAL_ELAPSED_TIME'][0] > 1000:
                        simple_time = f" ({round(query['TOTAL_ELAPSED_TIME'][0]/1000,2)} s)"
            
                    # Text of when query ran and how long
                    st.caption(f"Query started at {query['START_TIME'][0]} and finished at {query['END_TIME'][0]} with a total execution time of {'{:,.0f}'.format(query['TOTAL_ELAPSED_TIME'][0])} ms {simple_time}")
                except Exception as e:
                    query = pd.DataFrame({
                        'QUERY_ID':[query_id],
                        'WAREHOUSE_NAME':['Unknown'],
                        'WAREHOUSE_SIZE':['Unknown'],
                        'WAREHOUSE_TYPE':['Unknown'],
                        'DATABASE_NAME':['Unknown'],
                        'QUERY_TEXT':['Unknown'],
                        'WAREHOUSE_SIZE':['Unknown'],
                        'COMPILATION_TIME':[0],
                        'QUEUED_PROVISIONING_TIME':[0],
                        'QUEUED_REPAIR_TIME':[0],
                        'QUEUED_OVERLOAD_TIME':[0],
                        'EXECUTION_TIME':[0],
                        'SCHEMA_NAME':['Unknown'],
                        'QUERY_TYPE':['Unknown'],
                        'USER_NAME':['Unknown'],
                        'QUERY_TAG':['Unknown'],
                        'APPLICATION_NAME':['Unknown'],
                        'CLIENT_APPLICATION_ID':['Unknown']
                        })
                    st.warning(e)

                # Build 4 columns
                metrics = st.columns(4)
                with metrics[0]:
                    try:
                        st.text_input('Warehouse Name', value=query['WAREHOUSE_NAME'][0], disabled=True)
                        st.text_input('DB.SCHEMA', value=f"{query['DATABASE_NAME'][0]}.{query['SCHEMA_NAME'][0]}", disabled=True)
                        st.text_input('Client Application', value=query['APPLICATION_NAME'][0], disabled=True)
                        
                    except Exception as e:
                        st.warning(e)
                with metrics[1]:
                    try:
                        st.text_input('Warehouse Size', value=query['WAREHOUSE_SIZE'][0], disabled=True)
                        st.text_input('Query Type', value=query['QUERY_TYPE'][0], disabled=True)
                        st.text_input('Client Application ID', value=query['CLIENT_APPLICATION_ID'][0], disabled=True)
                    except Exception as e:
                        st.warning(e)
                with metrics[2]:
                    try:
                        st.text_input('Warehouse Type', value=query['WAREHOUSE_TYPE'][0], disabled=True)

                        st.text_input('User Name', value=query['USER_NAME'][0], disabled=True)
                    except Exception as e:
                        st.warning(e)
                with metrics[3]:
                    try:
                        st.vega_lite_chart(pd.DataFrame([
                                {"Time (ms)":query['COMPILATION_TIME'][0], "Stage":"Compilation Time"},
                                {"Time (ms)":query['QUEUED_PROVISIONING_TIME'][0], "Stage":"Queued Time"},
                                {"Time (ms)":query['QUEUED_REPAIR_TIME'][0], "Stage":"Queued Repair Time"},
                                {"Time (ms)":query['QUEUED_OVERLOAD_TIME'][0], "Stage":"Queued Overload Time"},
                                {"Time (ms)":query['EXECUTION_TIME'][0], "Stage":"Execution Time"}
                            ], columns=['Time (ms)','Stage']), 
                            {
                                "mark": {"type": "bar", "tooltip": True},
                                "encoding": {
                                "x": {
                                "aggregate": "sum",
                                "field": "Time (ms)"
                                },
                                "color": {"field": "Stage", "type": "nominal", "legend":None},
                            }
                            }, 
                            use_container_width=True, height=105)

                    except Exception as e:
                        st.warning(e)

                # Build 2 columns
                details = st.columns(2)
                with details[0]:
                    try:
                        
                        st.write("Query Text")
                        st.markdown(f"""`{query['QUERY_TEXT'][0]}`""")
                        st.text_input('Query Tag', value=query['QUERY_TAG'][0], disabled=True)
                    except Exception as e:
                        st.warning(e)

                with details[1]:
                    
                    try:
                        stats = session.sql(f"""with query_stats as(
                            select 
                                QUERY_ID,
                                STEP_ID,
                                OPERATOR_ID,
                                PARENT_OPERATOR_ID,
                                OPERATOR_TYPE,
                                OPERATOR_STATISTICS,
                                EXECUTION_TIME_BREAKDOWN,
                                OPERATOR_ATTRIBUTES,
                                EXECUTION_TIME_BREAKDOWN:overall_percentage::float as OPERATOR_EXECUTION_TIME,
                                OPERATOR_STATISTICS:output_rows output_rows,
                                OPERATOR_STATISTICS:input_rows input_rows,
                                CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,

                                // look for queries too large to fit into memory as described at https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#queries-too-large-to-fit-in-memory
                                OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
                                OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                                
                                operator_statistics:io:percentage_scanned_from_cache::float percentage_scanned_from_cache,

                                operator_attributes:table_name::string tablename,
                                OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
                                OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
                                OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,

                                
                                //*****COMMON QUERY PROBLEMS IDENTIFIED BY QUERY PROFILE**** https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#common-query-problems-identified-by-query-profile

                                    // 1) EXPLODING JOIN (https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#exploding-joins)
                                    CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN,

                                    // 2) "UNION WITHOUT ALL" (https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#union-without-all)
                                    CASE WHEN OPERATOR_TYPE = 'UnionAll' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = 'Aggregate' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL,

                                    // 3) Queries Too Large to Fit in Memory (https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#queries-too-large-to-fit-in-memory)
                                    CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY,
                                    
                                    // 4) Inefficient Pruning (https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#inefficient-pruning)
                                        // for example if partition_scan_ratio > 80% and any given table has a total partition count > 20,000 
                                        // (these are arbitrary numbers that may vary by customer or scenario - find what works for you)
                                    CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG
                                
                                from table(get_query_operator_stats('{query_id}')) -- 7h 30m 9s X-Small ALL 4 CONDITIONS MET

                            ORDER BY STEP_ID,OPERATOR_ID
                            )
                            SELECT 
                                QUERY_ID,
                                STEP_ID,
                                OPERATOR_ID,
                                PARENT_OPERATOR_ID,
                                OPERATOR_TYPE,
                                OPERATOR_STATISTICS,
                                EXECUTION_TIME_BREAKDOWN,
                                OPERATOR_ATTRIBUTES,
                                OPERATOR_EXECUTION_TIME,
                                OUTPUT_ROWS,
                                INPUT_ROWS,
                                ROW_MULTIPLE,
                                BYTES_SPILLED_LOCAL,
                                BYTES_SPILLED_REMOTE,
                                PERCENTAGE_SCANNED_FROM_CACHE,
                                TABLENAME,
                                PARTITIONS_SCANNED,
                                PARTITIONS_TOTAL,
                                PARTITION_SCAN_RATIO,
                                EXPLODING_JOIN,
                                UNION_WITHOUT_ALL,
                                QUERIES_TOO_LARGE_MEMORY,
                                INEFFICIENT_PRUNING_FLAG,
                                CLUSTERING_KEY
                            FROM query_stats
                            LEFT JOIN SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES t
                                on query_stats.TABLENAME = t.TABLE_CATALOG || '.' || t.TABLE_SCHEMA || '.' || t.TABLE_NAME
                            ORDER BY STEP_ID,OPERATOR_ID""").to_pandas()

                        stats['BYTES_SPILLED_LOCAL'] = stats['BYTES_SPILLED_LOCAL'].fillna(0).astype(int)
                        stats['BYTES_SPILLED_REMOTE'] = stats['BYTES_SPILLED_REMOTE'].fillna(0).astype(int)
                        stats["OUTPUT_ROWS"] = stats["OUTPUT_ROWS"].fillna(0).astype(int)
                        stats["INPUT_ROWS"] = stats["INPUT_ROWS"].fillna(0).astype(int)
                        stats["PARTITIONS_TOTAL"] = stats["PARTITIONS_TOTAL"].fillna(0).astype(int)
                        stats["PARTITIONS_SCANNED"] = stats["PARTITIONS_SCANNED"].fillna(0).astype(int)

                        try:
                            if stats["EXPLODING_JOIN"].max() == 1:
                                with st.expander("⚠️ Exploding Join Detected"):
                                    st.caption(f"""One of the Join operators produced significantly more rows than it consumed (output rows > input rows), often due to the absence of a join condition which often results in a Cartesian product and consumes additional resources.""")
                                    st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity#exploding-joins""")
                            
                                    problem = stats[stats["EXPLODING_JOIN"]==1]
                                    for index, row in problem.iterrows():
                                        parsed = json.loads(row["OPERATOR_ATTRIBUTES"])
                                        if "join_type" in parsed:
                                            st.markdown(f"""**Join Type:** {parsed["join_type"]}""")
                                        if "equality_join_condition" in parsed:
                                            st.markdown(f"""**Condition:**""")
                                            st.markdown(f"""`{parsed["equality_join_condition"]}`""") 
                                        try:
                                            if row['OUTPUT_ROWS'] > row['INPUT_ROWS']:
                                                st.markdown(f"""This join turned `{'{:,.0f}'.format(row['INPUT_ROWS'])}` input rows into `{'{:,.0f}'.format(row['OUTPUT_ROWS'])}` output rows (`{'{:,.1f}'.format(row['OUTPUT_ROWS']/row['INPUT_ROWS'])}`x multiple)""")
                                        except Exception as e:
                                            st.warning(e)

                            else:
                                with st.expander("✅ No Exploding Joins Detected"):
                                    st.caption(f"""One of the Join operators produced significantly more rows than it consumed (output rows > input rows), often due to the absence of a join condition which often results in a Cartesian product and consumes additional resources.""")
                                    st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity#exploding-joins""")
                        except Exception as e:
                            st.warning(e)

                    
                        try:
                            if stats["UNION_WITHOUT_ALL"].max() == 1:
                                _heading = "⚠️ UNION Without ALL Detected"
                            else:
                                _heading = "✅ No UNION Without ALL Detected"                        
        
                            with st.expander(_heading):
                                st.caption(f"""This query performs a UNION without ALL, which not only concatenates inputs, but also performs duplicate elimination which may not be necessary and may consume additional resources.""")
                                st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#union-without-all""")                                 
                        
                        except Exception as e:
                            st.warning(e)


                        try:
                            if stats["QUERIES_TOO_LARGE_MEMORY"].max() == 1:
                                with st.expander("⚠️ Queries Too Large to Fit in Memory"):
                                    st.caption(f"""The [compute resources](https://docs.snowflake.com/en/user-guide/warehouses-considerations) used in this query were insufficient to hold intermediate results, and resulted in {"local" if stats["BYTES_SPILLED_REMOTE"].sum() == 0 else "local and remote"} spilling, which negatively affected query performance.  Consider increasing the [virtual warehouse size](https://docs.snowflake.com/en/user-guide/warehouses-overview#impact-on-query-processing), or processing the data in smaller batches.""")
                                    st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#queries-too-large-to-fit-in-memory""")

                                    for index, row in stats.sort_values(by=['OPERATOR_ID']).iterrows():
                                        if ((row["BYTES_SPILLED_LOCAL"] != 0) & (row["BYTES_SPILLED_REMOTE"] != 0)):
                                            st.caption(f"""* Step {row["OPERATOR_ID"]}: `{"{:,.0f}".format(int(row["BYTES_SPILLED_LOCAL"]))}` bytes spilled onto local storage and `{"{:,.0f}".format(int(row["BYTES_SPILLED_LOCAL"]))}` bytes spilled onto remote storage""")
                                        elif ((row["BYTES_SPILLED_LOCAL"] != 0) & (row["BYTES_SPILLED_REMOTE"] == 0)):
                                            st.caption(f"""* Step {row["OPERATOR_ID"]}: `{"{:,.0f}".format(int(row["BYTES_SPILLED_LOCAL"]))}` bytes spilled onto local storage """)
                                        elif ((row["BYTES_SPILLED_LOCAL"] == 0) & (row["BYTES_SPILLED_REMOTE"] != 0)):
                                            st.caption(f"""* Step {row["OPERATOR_ID"]}: `{"{:,.0f}".format(int(row["BYTES_SPILLED_REMOTE"]))}` bytes spilled onto remote storage """)

                                    try:
                                        wh_sizes = ['X-Small','Small','Medium','Large','X-Large','2X-Large','3X-Large','4X-Large','5X-Large','6X-Large']
                                        index = 0
                                        for wh_size in wh_sizes:
                                            if query['WAREHOUSE_SIZE'][0] == wh_size:
                                                size_up = index + 1
                                            index = index + 1

                                        current_wh_size = session.sql(f"SHOW WAREHOUSES LIKE '{query['WAREHOUSE_NAME'][0]}'").collect()[0]['size']

                                        index = 0
                                        for wh_size in wh_sizes:
                                            if current_wh_size == wh_size:
                                                current_wh_num = index
                                            index = index + 1
                                                
                                        new_size = ['XSMALL','SMALL','MEDIUM','LARGE','XLARGE','XXLARGE','XXXLARGE','X4LARGE','X5LARGE','X6LARGE']

                                        if current_wh_num < size_up:       
                                            st.markdown(f"""This query was executed on WH `{query['WAREHOUSE_NAME'][0]}`, sized “`{query['WAREHOUSE_SIZE'][0]}` at the time of execution”.  Would you like to increase the WH size to `{new_size[size_up]}`, to help decrease future spilling?""")
                                            st.markdown(f"""`ALTER WAREHOUSE "{query['WAREHOUSE_NAME'][0]}" SET WAREHOUSE_SIZE={new_size[size_up]};`""")
                    
                                            st.write("Or use the following button to automatically increase your warehouse size")
                                            st.button(f"Change {query['WAREHOUSE_NAME'][0]} to {new_size[size_up]}", on_click=resize_wh, args=(query['WAREHOUSE_NAME'][0], new_size[size_up]), type='primary')
                                        else:
                                            st.write("Your current warehouse size is already greater than the size when this query was executed. Try running the query again to see if the warehouse is appropriately sized.")
                                    except Exception as e:
                                        pass
                                    
                            else:
                                with st.expander("✅ Queries Fit in Memory"):
                                    st.caption(f"""The [compute resources](https://docs.snowflake.com/en/user-guide/warehouses-considerations) used in this query were sufficient to hold intermediate results, and did not result in local or remote spilling, which would have negatively affected query performance.  If the queries could not fit in memory, consider increasing the [virtual warehouse size](https://docs.snowflake.com/en/user-guide/warehouses-overview#impact-on-query-processing), or processing the data in smaller batches.""")
                                    st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#queries-too-large-to-fit-in-memory""")
                        except Exception as e:
                            st.warning(e)

                        
                        try:
                            if stats["INEFFICIENT_PRUNING_FLAG"].max() == 1:
                                _heading = "⚠️ Inefficient Pruning"
                            else:
                                _heading = "✅ Efficient Pruning"
                            with st.expander(_heading):
                                st.caption(f"""This query executed large table scans against very large tables, scanning a very high percentage of the total number of partitions (Ideally constructed queries only read necessary parts of a table).  Consider adding additional filters, re-sorting the table (ordered by columns commonly used in join and filter operations), adding a [table cluster key](https://docs.snowflake.com/en/user-guide/tables-clustering-keys), or confirming that existing cluster keys, were effective for this query.  Future data loading and ingestion operations on these tables should be sorted the same way.""")
                                st.caption(f"""https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#inefficient-pruning""")
                        
                                try:
                                    problem = stats[stats["TABLENAME"].notnull()]
                                    problem["sort"] = 5
                                    problem["sort"] = np.where(problem["PARTITIONS_TOTAL"].le(1000), 1, problem["sort"])
                                    problem["sort"] = np.where(problem["PARTITIONS_SCANNED"].le(problem["PARTITIONS_TOTAL"]*.5), 1, problem["sort"])
                                    problem["sort"] = np.where(problem["INEFFICIENT_PRUNING_FLAG"].eq(1), 10, problem["sort"])
                                    
                                    for index, row in problem.sort_values(by=['sort', 'PARTITIONS_TOTAL'], ascending=False).iterrows():
                                        # cols = st.columns(2)
                                        parsed = json.loads(row["OPERATOR_ATTRIBUTES"])
                                        # with cols[0]:
                                        # with cols[1]:
                                        indicator = '🔴'
                                        if row['sort'] < 5:
                                            indicator = '🟢'
                                        elif row['sort'] < 10:
                                            indicator = '🟡'
                                        
                                        st.markdown(f"""{indicator} **Partitions Scanned / Total:** {'{:,.0f}'.format(row["PARTITIONS_SCANNED"])} / {'{:,.0f}'.format(row["PARTITIONS_TOTAL"])} ({round(row["PARTITION_SCAN_RATIO"]*100,1)}%)""")
                                        st.markdown(f"""**Table:** {parsed["table_name"]}""")
                                        st.markdown(f"""**Columns:** `{parsed["columns"]}`""")


                                        if row['CLUSTERING_KEY'] != None:
                                            st.markdown(f"""**Clustering Key:** `{row["CLUSTERING_KEY"]}`""")
                                        st.write("")
                                        st.write("")
                                except Exception as e:
                                    st.warning(e)
                        except Exception as e:
                            st.warning(e)
                    except Exception as e:
                        st.warning(e)
                
                with st.expander("More query details"):
                    try:
                        transpose = query.transpose()
                        try:
                            transpose = transpose.drop(['index'])
                        except:
                            pass
                        transpose = transpose.drop([
                            'QUERY_ID',
                            'QUERY_TEXT',
                            'WAREHOUSE_NAME',
                            'WAREHOUSE_SIZE',
                            'WAREHOUSE_TYPE',
                            'QUERY_TYPE',
                            'USER_NAME',
                            'CLIENT_APPLICATION_ID',
                            'QUERY_TAG'
                        ])
                        st.dataframe(transpose, use_container_width=True)

                    except Exception as e:
                        st.warning(e)

                    try:
                        st.write(stats)
                    except Exception as e:
                        st.warning(e)
            #endregion
    except Exception as e:
        # st.warning(e)
        pass
        # st.write(e)