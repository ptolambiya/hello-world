from etl.parsers.base_parser import BaseParser
from datetime import datetime

class TableParser(BaseParser, data_type="TABLE"):
    def parse(self, raw_data):
        if not self.mappings:
            #print(raw_data)
            return raw_data
            
        mapped_data = []
        for row in raw_data:
            mapped_row = {}
            for mapping in self.mappings:
                src_value = row.get(mapping.source_path)
                mapped_row[mapping.destination_column] = self._apply_transformations(src_value, mapping)
            mapped_data.append(mapped_row)
        #print(mapped_data)
        return mapped_data

    def _apply_transformations(self, value, mapping):
        if mapping.data_type:
            value = self._cast_type(value, mapping.data_type)
        if mapping.transformation:
            value = self._apply_transformation(value, mapping.transformation)
        return value

    def _apply_transformation(self, value, transformation):
        # Implement transformation functions
        if transformation == 'UPPERCASE':
            return str(value).upper()
        elif transformation == 'TRIM':
            return str(value).strip()
        # Add more transformations as needed
        return value
    
    def _cast_type(self, value, data_type):
        # Implement type casting logic
        try:
            if data_type == 'CHAR':
                return value
            elif data_type == 'NUMBER':
                return value
            elif data_type == 'DATE':
                return datetime.strptime(value, '%Y-%m-%d')
            elif data_type == 'FLOAT':
                return float(value)
            # Add more type conversions as needed
        except:
            return value