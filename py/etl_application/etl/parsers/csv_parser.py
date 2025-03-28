import csv
from io import StringIO
from etl.parsers.base_parser import BaseParser

class CsvParser(BaseParser):
    def parse(self, raw_data):
        parsed = []
        for csv_str in raw_data:
            reader = csv.DictReader(StringIO(csv_str))
            parsed.extend([row for row in reader])
        return parsed
