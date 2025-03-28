from etl.services.etl_service import ETLService


# Initialize with Oracle connection
etl = ETLService('oracle+cx_oracle://hr:hr@localhost:1521/?service_name=LOM')

# Execute ETL group
#etl.run_etl_group('HR_TO_HR')
etl.run_etl_group('MYSQL_TO_HR')