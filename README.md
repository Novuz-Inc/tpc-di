**Structure of the code:**
1. source: Original Snowflake code
2. converted: Converted DAG came out of Novuz automation
3. deployed: Code deployed to Databricks

**Approach:**
1. Converted to Delta Live Table (DLT) framework, due to complex dependencies on Snowflake side. DLT handles all dependencies through lineage, so it's taken care of.
2. Novuz platform has the capability to convert regular delta tables as well instead of DLT. It will be used as needed.
3. In this solution, tables have been converted to streaming live table.
4. Tasks that are ingesting data to the streaming tables have been converted to auto-loader jobs.
5. All procedures have been converted to notebook SQL.
6. A default pipeline has been created and all notebooks (Procedures) were attached to it. The pipeline will handle the execution based on dependencies.

**Manual intervention:**
1. The tables that have multiple feed have been suffixed by _n, since DLT framework doesn't support multiple feed for a table yet.
2. Manual notebooks were created to join (Through union all) all suffixed tables to create the final table. This manual step will be eliminated when DLT handles multiple feed.
3. The while statement was removed from the procedures that fed the data in loop from different directories. DLT supposrts multiple directories using RegEx.
4. Generated code for views created for the tables having XML structure were adjusted manually as needed. Databricks XML functions are somewhat limited and need additional information.
   
