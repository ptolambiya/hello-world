-- SYSTEM_CONFIG samples for different databases
INSERT INTO system_config (system_name, system_type, connection_config) VALUES (
    'ORACLE_ERP',
    'ORACLE',
    '{"host": "erp-db", "port": 1521, "service_name": "ERPPROD", "user": "etl_user", "password": "etl_pass"}'
);

INSERT INTO system_config (system_name, system_type, connection_config) VALUES (
    'MSSQL_HR',
    'MSSQL',
    '{"host": "hr-db", "port": 1433, "database": "HRDB", "user": "sa", "password": "sqlPass123"}'
);

INSERT INTO system_config (system_name, system_type, connection_config) VALUES (
    'POSTGRES_INVENTORY',
    'POSTGRES',
    '{"host": "inv-pg", "port": 5432, "database": "inventory", "user": "pguser", "password": "pgpass"}'
);

-- SERVICE_GROUP_CONFIG for cross-database ETL
INSERT INTO service_group_config (group_name, source_system_id, target_system_id, schedule_cron) VALUES (
    'HR_TO_ORACLE',
    (SELECT id FROM system_config WHERE system_name = 'MSSQL_HR'),
    (SELECT id FROM system_config WHERE system_name = 'ORACLE_ERP'),
    '0 1 * * *'
);

-- SERVICE_DETAIL_CONFIG with MSSQL source
INSERT INTO service_detail_config (group_id, source_type, source_data_type, extraction_query, destination_table, process_order) 
VALUES (
    (SELECT id FROM service_group_config WHERE group_name = 'HR_TO_ORACLE'),
    'DATABASE',
    'TABLE',
    'SELECT EmployeeID, FirstName, LastName FROM Employees',
    'HR_EMPLOYEES',
    1
);

-- COLUMN_MAPPING for MSSQL to Oracle
INSERT INTO service_column_mapping (service_detail_id, source_path, destination_column, data_type, process_order)
VALUES (
    (SELECT id FROM service_detail_config WHERE destination_table = 'HR_EMPLOYEES'),
    'EmployeeID',
    'EMP_ID',
    'NUMBER',
    1
);