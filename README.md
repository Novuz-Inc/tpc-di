**Structure of the code:**
1. source: Original Snowflake code
2. parsed: Parsed source code data came out of Novuz parser
3. converted: Converted DAG came out of Novuz automation
4. deployed: Code deployed to Databricks

**Approach:**
1. Converted to Delta Live Table (DLT) framework, due to complex dependencies on Snowflake side. DLT handles all dependencies through lineage, so no explicit dependency needed to be added.
2. Novuz platform has the capability to convert regular delta tables as well instead of DLT. It will be used as needed.
3. In this solution, tables have been converted to streaming live tables.
4. Tasks that are ingesting data to the streaming tables have been converted to auto-loader jobs.
5. All procedures have been converted to notebook SQL.
6. A default pipeline has been created and all notebooks (Procedures) are attached to it. The pipeline will handle the execution based on dependencies. 

**Manual intervention:**
1. The tables that have multiple feeds have been suffixed by _n, since DLT framework doesn't support multiple feeds for a table yet.
2. Manual notebooks were created to join (Through union all) all suffixed tables to create the final table. This manual step will be eliminated when DLT handles multiple feeds through multi-flow operation.
3. The while statement inside the procedures that fed the data in loop from different directories was removed. DLT supposrts multiple directories using RegEx/wild characters.
4. Generated code for views created for the tables having XML structure were adjusted manually as needed. Databricks XML functions are somewhat limited and need additional information that didn't exist in Snowflake code.
   
