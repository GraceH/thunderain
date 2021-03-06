--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- The _cached table is stored in the heap space of Shark service
DROP TABLE IF EXISTS log_view_cached;
CREATE TABLE log_view_cached(
-- The header part of the log record, it is generated by the system automatically
-- All the columns in the header part is prefixed with h_
    h_host_ip STRING,
    h_data_type STRING,
    h_data_source STRING,
    h_user STRING,
    h_tags STRING,
    h_time BIGINT,
-- The body part of the log record, it is parsed from those collected log messages
-- All the columns in the body part is prefixed with b_
    b_message STRING,
    b_log_level STRING,
    b_trace STRING,
    b_module_name STRING, --i.e., classname
    b_others STRING,
---- Following columns are ready for the future usages.
    b_pid STRING, -- (PROCESS_ID)
    b_tid BIGINT, -- (THREAD_ID)
    b_thread_name STRING,
    b_source_file STRING,
    b_line_number STRING
    )
ROW FORMAT SERDE "shark.memstore2.ColumnarSerDe"
TBLPROPERTIES ("shark.cache" = "true");


-- The _tachyon table is stored in the tachyon distributed file system in memory store
-- Such kind of table can be accessed offline
DROP TABLE  IF EXISTS log_view_tachyon;
CREATE TABLE log_view_tachyon(
-- The header part of the log record, it is generated by the system automatically
-- All the columns in the header part is prefixed with h_
     h_host_ip STRING,
     h_data_type STRING,
     h_data_source STRING,
     h_user STRING,
     h_tags STRING,
     h_time BIGINT,
-- The body part of the log record, it is parsed from those collected log messages
-- All the columns in the body part is prefixed with b_
    b_message STRING,
    b_log_level STRING,
    b_trace STRING,
    b_module_name STRING, --i.e., classname
    b_others STRING,
---- Following columns are ready for the future usages.
    b_pid STRING, -- (PROCESS_ID)
    b_tid BIGINT, -- (THREAD_ID)
    b_thread_name STRING,
    b_source_file STRING,
    b_line_number STRING
 )
ROW FORMAT SERDE "shark.memstore2.ColumnarSerDe"
TBLPROPERTIES ("shark.cache" = "tachyon");
