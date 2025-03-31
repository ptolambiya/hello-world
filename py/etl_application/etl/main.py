from etl.app.etl_application import ETLApplication

if __name__ == "__main__":
    # Initialize with your Oracle connection string
    config_db_uri = 'oracle+cx_oracle://hr:hr@localhost:1521/?service_name=LOM'
    app = ETLApplication(config_db_uri)
    app.start()
