# Import python packages
import streamlit as st
import st_connection
import st_connection.snowflake
import snowflake.snowpark as sp
import pandas as pd
import json
import numpy as np

try:
    # Set the page layout to wide
    st.set_page_config(
        page_title="Snowflake Query Statistics",
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
        st.title("Streamlit Query Statistics")

        # Things above here will be run before (and after) you log in.
        if 'ST_SNOW_SESS' not in st.session_state:
            with st.expander("Login Help", False):
                st.markdown(
                        """
        ***account***: this should be the portion in between "https://" and ".snowflakecomputing.com" - for example https://<account>.snowflakecomputing.com
                            
        ***database***: This should remain ***SNOWFLAKE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location
        ***schema***: This should remain ***ACCOUNT_USAGE*** unless you have copied your `query_history` and `warehouse_metering_history` to another location
        ***role***: This should remain ***ACCOUNTADMIN*** unless you have delegated access to `query_history` and `warehouse_metering_history`
                """)
            login = st.container()
            st.caption(disclaimer)
            st.write("")
            st.caption(f"""©2023 by Andrew Kim (andrew.kim@snowflake.com)""")
            st.caption(f"Streamlit Version: {st.__version__}")
            st.caption(f"Snowpark Version: {sp.__version__}")

            with login:
                session = st.connection.snowflake.login({
                    'account': 'XXX',
                    'user': '',
                    'password': None,
                    'warehouse': 'ADHOC_WH',
                    'database': 'SNOWFLAKE',
                    'schema': 'ACCOUNT_USAGE',
                    'role': 'ACCOUNTADMIN',
                }, {
                    'ttl': 120
                }, 'Snowflake Login')

        session = st.connection.snowflake.login({
            'account': 'XXX',
            'user': '',
            'password': None,
            'warehouse': 'ADHOC_WH',
            'database': 'SNOWFLAKE',
            'schema': 'ACCOUNT_USAGE',
            'role': 'ACCOUNTADMIN',
        }, {
            'ttl': 120
        }, 'Snowflake Login')
        
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
                try:
                    #region Specific query detail
                    # If Query ID has been provided, filter query history on the provided query_id
                    query = qh[qh["QUERY_ID"] == query_id].reset_index()
                    
            
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
                except:
                    pass
                    
                # Build 2 columns
                details = st.columns(2)
                with details[0]:
                    try:
                        st.text_input('DB.SCHEMA', value=f"{query['DATABASE_NAME'][0]}.{query['SCHEMA_NAME'][0]}", disabled=True)
                        st.write("Query Text")
                        st.caption(query["QUERY_TEXT"][0])
                        st.text_input('Query Tag', value=query['QUERY_TAG'][0], disabled=True)
                    except:
                        pass

                with details[1]:
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
                        st.text_input('Warehouse Name', value=query['WAREHOUSE_NAME'][0], disabled=True)
                        st.text_input('Warehouse Size', value=query['WAREHOUSE_SIZE'][0], disabled=True)
                        st.text_input('Warehouse Type', value=query['WAREHOUSE_TYPE'][0], disabled=True)
                        st.text_input('Query Type', value=query['QUERY_TYPE'][0], disabled=True)
                        st.text_input('User Name', value=query['USER_NAME'][0], disabled=True)
                        st.text_input('Client Application', value=query['CLIENT_APPLICATION_ID'][0], disabled=True)
                    except:
                        pass

                with st.expander("More query details"):
                    transpose = query.transpose()
                    transpose = transpose.drop([
                        'index',
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
                    
                # st.write(stats)

                try:
                    if stats["EXPLODING_JOIN"].max() == 1:
                        with st.expander("⚠️ Exploding Join Detected"):
                            st.caption(f"""One of the common mistakes SQL users make is joining tables without providing a join condition (resulting in a “Cartesian product”), or providing a condition where records from one table match multiple records from another table. For such queries, the Join operator produces significantly (often by orders of magnitude) more tuples than it consumes.
        
    This can be observed by looking at the number of records produced by a Join operator, and typically is also reflected in Join operator consuming a lot of time.
        
    https://docs.snowflake.com/en/user-guide/ui-snowsight-activity#exploding-joins""")
                    
                            problem = stats[stats["EXPLODING_JOIN"]==1]
                            for index, row in problem.iterrows():
                                parsed = json.loads(row["OPERATOR_ATTRIBUTES"])
                                st.markdown(f"""**Join Type:** {parsed["join_type"]}
        
    **Condition:** 

    `{parsed["equality_join_condition"]}`""")
                except:
                    pass

            
                try:
                    if stats["UNION_WITHOUT_ALL"].max() == 1:
                        with st.expander("⚠️ UNION Without ALL Detected"):
                            st.caption(f"""In SQL, it is possible to combine two sets of data with either UNION or UNION ALL constructs. The difference between them is that UNION ALL simply concatenates inputs, while UNION does the same, but also performs duplicate elimination.
        
    A common mistake is to use UNION when the UNION ALL semantics are sufficient. These queries show in Query Profile as a UnionAll operator with an extra Aggregate operator on top (which performs duplicate elimination).
            
    https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#union-without-all""")               
                
                except:
                    pass


                try:
                    if stats["QUERIES_TOO_LARGE_MEMORY"].max() == 1:
                        with st.expander("⚠️ Queries Too Large to Fit in Memory "):
                            st.caption(f"""For some operations (e.g. duplicate elimination for a huge data set), the amount of memory available for the servers used to execute the operation might not be sufficient to hold intermediate results. As a result, the query processing engine will start spilling the data to local disk. If the local disk space is not sufficient, the spilled data is then saved to remote disks.
        
    This spilling can have a profound effect on query performance (especially if remote disk is used for spilling). To alleviate this, we recommend:
    * Using a larger warehouse (effectively increasing the available memory/local disk space for the operation), and/or
    * Processing data in smaller batches.
        
    https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#queries-too-large-to-fit-in-memory""")
        
                            size_up = 'XSMALL'
                            if query['WAREHOUSE_SIZE'][0] == 'X-Small':
                                size_up = 'SMALL'
                            elif query['WAREHOUSE_SIZE'][0] == 'Small':
                                size_up = 'MEDIUM'
                            elif query['WAREHOUSE_SIZE'][0] == 'Medium':
                                size_up = 'LARGE'
                            elif query['WAREHOUSE_SIZE'][0] == 'Large':
                                size_up = 'XLARGE'
                            elif query['WAREHOUSE_SIZE'][0] == 'X-Large':
                                size_up = 'XXLARGE'
                            elif query['WAREHOUSE_SIZE'][0] == '2X-Large':
                                size_up = 'XXXLARGE'
                            elif query['WAREHOUSE_SIZE'][0] == '3X-Large':
                                size_up = 'X4LARGE'
                            elif query['WAREHOUSE_SIZE'][0] == '4X-Large':
                                size_up = 'X5LARGE'
                            elif query['WAREHOUSE_SIZE'][0] == '5X-Large':
                                size_up = 'X6LARGE'
                            elif query['WAREHOUSE_SIZE'][0] == '5X-Large':
                                size_up = False
            
                            if size_up != False:                
                                st.markdown(f"""To change your warehouse size, run the following query
                            
    `ALTER WAREHOUSE "{query['WAREHOUSE_NAME'][0]}" SET WAREHOUSE_SIZE={size_up};`                
                            """)
        
                                st.write("Or use the following button to automatically increase your warehouse size")
                                st.button(f"Change {query['WAREHOUSE_NAME'][0]} to {size_up}", on_click=resize_wh, args=(query['WAREHOUSE_NAME'][0], size_up), type='primary')

                except:
                    pass

                
                try:
                    if stats["INEFFICIENT_PRUNING_FLAG"].max() == 1:
                        with st.expander("⚠️ Inefficient Pruning"):
                            st.caption(f"""Snowflake collects rich statistics on data allowing it not to read unnecessary parts of a table based on the query filters. However, for this to have an effect, the data storage order needs to be correlated with the query filter attributes.

    The efficiency of pruning can be observed by comparing Partitions scanned and Partitions total statistics in the TableScan operators. If the former is a small fraction of the latter, pruning is efficient. If not, the pruning did not have an effect.

    Of course, pruning can only help for queries that actually filter out a significant amount of data. If the pruning statistics do not show data reduction, but there is a Filter operator above TableScan which filters out a number of records, this might signal that a different data organization might be beneficial for this query.

    https://docs.snowflake.com/en/user-guide/ui-snowsight-activity.html#inefficient-pruning""")
                    
                            problem = stats[stats["TABLENAME"].notnull()]
                            problem["PARTITIONS_TOTAL"] = problem["PARTITIONS_TOTAL"].astype(int)
                            problem["PARTITIONS_SCANNED"] = problem["PARTITIONS_SCANNED"].astype(int)
                            problem["sort"] = 5
                            problem["sort"] = np.where(problem["PARTITIONS_TOTAL"].le(1000), 1, problem["sort"])
                            problem["sort"] = np.where(problem["PARTITIONS_SCANNED"].le(problem["PARTITIONS_TOTAL"]*.5), 1, problem["sort"])
                            problem["sort"] = np.where(problem["INEFFICIENT_PRUNING_FLAG"].eq(1), 10, problem["sort"])
                            
                            for index, row in problem.sort_values(by=['sort', 'PARTITIONS_TOTAL'], ascending=False).iterrows():
                                cols = st.columns(2)
                                parsed = json.loads(row["OPERATOR_ATTRIBUTES"])
                                with cols[0]:
                                    st.markdown(f"""**Table:** {parsed["table_name"]}""")
                                    st.markdown(f"""**Columns:** `{parsed["columns"]}`""")
                                with cols[1]:
                                    indicator = '🔴'
                                    if row['sort'] < 5:
                                        indicator = '🟢'
                                    elif row['sort'] < 10:
                                        indicator = '🟡'
                                    
                                    st.markdown(f"""{indicator} **Partitions Scanned / Total:**
                                
    {'{:,.0f}'.format(row["PARTITIONS_SCANNED"])} / {'{:,.0f}'.format(row["PARTITIONS_TOTAL"])} ({round(row["PARTITION_SCAN_RATIO"]*100,1)}%)""")

                                st.write("")
                                st.write("")
                except:
                    pass
            #endregion
    except Exception as e:
        st.warning(e)
        pass
        # st.write(e)