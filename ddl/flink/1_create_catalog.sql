CREATE CATALOG hive_catalog WITH (
'type' = 'hive',
'hive-conf-dir' = '/opt/bigdata/hive-3.1.2/conf',
'default-database' = 'demo_ods'
);
