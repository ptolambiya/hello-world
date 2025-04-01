from lxml import etree
from io import BytesIO
from etl.parsers.base_parser import BaseParser

class XmlParser(BaseParser, data_type="XML"):
    def parse(self, raw_data):
        parsed = []
        for xml_str in raw_data:
            root = etree.parse(BytesIO(xml_str.encode())).getroot()
            record = {}
            for mapping in self.mappings:
                elements = root.xpath(mapping['source_path'])
                record[mapping['destination_column']] = elements[0].text if elements else None
            parsed.append(record)
        return parsed
