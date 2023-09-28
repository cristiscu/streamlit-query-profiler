-- Created By:    Cristian Scutaru
-- Creation Date: Sep 2023
-- Company:       XtractPro Software

CREATE OR REPLACE DATABASE streamlit_query_profiler;

CREATE STAGE stage
    directory = (enable=true)
    file_format = (type=CSV field_delimiter=None record_delimiter=None);

PUT file://C:\Projects\streamlit-apps\streamlit-query-profiler\app.py @stage
    overwrite=true auto_compress=false;

CREATE STREAMLIT streamlit_query_profiler
    ROOT_LOCATION = '@streamlit_query_profiler.public.stage'
    MAIN_FILE = '/app.py'
    QUERY_WAREHOUSE = "COMPUTE_WH";
SHOW STREAMLITS;
