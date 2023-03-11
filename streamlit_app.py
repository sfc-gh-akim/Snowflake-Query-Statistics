# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark as sp
import pandas as pd

try:
    # Set the page layout to wide
    st.set_page_config(
        layout="wide",
    )
except:
    pass

st.title("Streamlit Query Profiler")

# Set up general laytout
settings = st.columns(4)
query_id = st.text_input("Query Id")
clear = st.columns(4)
aggcharts = st.columns(4)
container = st.container()
st.write("")
footer = st.container()

with footer:
    st.caption(f"""©2023 by Andrew Kim (andrew.kim@snowflake.com)""")
    st.caption(f"Streamlit Version: {st.__version__}")
    st.caption(f"Snowpark Version: {sp.__version__}")

# Get the current credentials
session = get_active_session()

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
        QUERY_LOAD_PERCENT,IS_CLIENT_GENERATED_STATEMENT,CLIENT_APPLICATION_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS s ON q.SESSION_ID = s.SESSION_ID
    WHERE EXECUTION_STATUS = 'SUCCESS' AND END_TIME >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '14 DAY' 
    AND WAREHOUSE_NAME IS NOT NULL AND WAREHOUSE_SIZE IS NOT NULL
    ORDER BY END_TIME DESC""").to_pandas()
filtered_qh = qh

# Add dropdown to change pie charts between count, duration, or bubble chart w/ count & duration
with settings[3]:
    # Only show if no Query ID has been provided
    if(query_id == ''):
        sumby = st.selectbox(
        'Show by',
        ('Count', 'Total Duration', 'Count & Duration'))

#region This builds the pie charts
index = 0
groupbys = ["WAREHOUSE_NAME", "USER_NAME", "QUERY_TYPE", "CLIENT_APPLICATION_ID"]
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
                        "color": {"field": groupby, "type": "nominal", "legend":{"orient":"bottom"}}
                      }
                    }, use_container_width=True)
                elif sumby == 'Total Duration':
                    st.vega_lite_chart(df, {
                        "mark": {"type": "arc", "tooltip": True},
                        "encoding": {
                        "theta": {"field": "Query Time (hr)", "type": "quantitative", "stack": "normalize"},
                        "color": {"field": groupby, "type": "nominal", "legend":{"orient":"bottom"}}
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
        
                filter = st.selectbox(
                    f"{groupby}",
                    ['All'] + df[groupby].to_list())
                if filter != 'All':
                    filtered_qh = filtered_qh[groupby == filter] 
            except:
                pass
            
    index = index + 1
#endregion


with container:
    if(query_id == ""):
        # If no Query ID has been provided, show table of most recent 50 queries
        st.write(filtered_qh.head(50))
    else:
        #region Specific query detail
        # If Query ID has been provided, filter query history on the provided query_id
        query = qh[qh["QUERY_ID"] == query_id].reset_index()

        # Show a simplified time and convert milliseconds to larger time increment
        simple_time = ""
        if query['TOTAL_ELAPSED_TIME'][0] > (1000*60*60):
            simple_time = f" ({query['TOTAL_ELAPSED_TIME'][0]/1000/60/60} hrs)"
        elif query['TOTAL_ELAPSED_TIME'][0] > (1000*60):
            simple_time = f" ({query['TOTAL_ELAPSED_TIME'][0]/1000/60} min)"
        elif query['TOTAL_ELAPSED_TIME'][0] > 1000:
            simple_time = f" ({query['TOTAL_ELAPSED_TIME'][0]/1000} s)"

        # Text of when query ran and how long
        st.caption(f"Query started at {query['START_TIME'][0]} and finished at {query['END_TIME'][0]} with a total execution time of {query['TOTAL_ELAPSED_TIME'][0]}ms {simple_time}")

        # Build 2 columns
        details = st.columns(2)
        with details[0]:
            st.text_input('DB.SCHEMA', value=f"{query['DATABASE_NAME'][0]}.{query['SCHEMA_NAME'][0]}", disabled=True)
            st.write("Query Text")
            st.caption(query["QUERY_TEXT"][0])
            st.text_input('Query Tag', value=query['QUERY_TAG'][0], disabled=True)

        with details[1]:
            st.vega_lite_chart(pd.DataFrame([{"Time (ms)":query['COMPILATION_TIME'][0], "Stage":"Compilation Time"},
                                              {"Time (ms)":query['QUEUED_PROVISIONING_TIME'][0], "Stage":"Queued Time"},
                                              {"Time (ms)":query['EXECUTION_TIME'][0], "Stage":"Execution Time"}], 
                                             columns=['Time (ms)','Stage']), {
                "mark": {"type": "bar", "tooltip": True},
                "encoding": {
                "x": {
                  "aggregate": "sum",
                  "field": "Time (ms)"
                },
                "color": {"field": "Stage", "type": "nominal", "legend":{"orient":"bottom"}},
              }
            }, use_container_width=True)
            st.text_input('Warehouse Name', value=query['WAREHOUSE_NAME'][0], disabled=True)
            st.text_input('Warehouse Size', value=query['WAREHOUSE_SIZE'][0], disabled=True)
            st.text_input('Warehouse Type', value=query['WAREHOUSE_TYPE'][0], disabled=True)
            st.text_input('Query Type', value=query['QUERY_TYPE'][0], disabled=True)
            st.text_input('User Name', value=query['USER_NAME'][0], disabled=True)
            st.text_input('Client Application', value=query['CLIENT_APPLICATION_ID'][0], disabled=True)


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
        st.write(query)
        st.write(stats)
        #endregion
