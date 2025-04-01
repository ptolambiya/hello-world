import json
import jsonpath_ng
from etl.parsers.base_parser import BaseParser

class JsonParser(BaseParser, data_type="JSON"):
    def parse(self, raw_data):
        parsed = []
        for item in raw_data:
            if isinstance(item, str):
                data = json.loads(item)
            else:
                data = item

            record = {}
            for mapping in self.mappings:
                expr = jsonpath_ng.parse(mapping.source_path)
                matches = [match.value for match in expr.find(data)]
                record[mapping.destination_column] = matches[0] if matches else None
            parsed.append(record)
        return parsed
