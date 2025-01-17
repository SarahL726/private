import json

class Generator:
    def __init__(self):
        pass

    def generate(self, attributes, destinationFolderID):
        units = []
        for attribute in attributes:
            unit = {
                    "id": attribute[0],    # attribute id
                    "name": attribute[1],  # attribute name
                    "type": "attribute"
                }
            units.append(unit)

        json_dict = {
            "information": {
                "name": "Temp report",  # attribute name
                "destinationFolderId": destinationFolderID
            },
            "sourceType": "normal",
            "dataSource": {
                "dataTemplate": {
                    "units": units
                }
            }
        }

        json_string = json.dumps(json_dict, indent=2)
        return json_string