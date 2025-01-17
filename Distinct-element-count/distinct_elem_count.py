import requests
import os
import json
import pandas as pd
import csv
import generateJson
import logging
import time

is_verified = True

# Api token authentication
DssXmlAuthApiToken = 4096

# define a global dict to store the uuid to the form order with following data
FormIDToOrder ={}
indexed_forms = {
    'char',
    'varChar',
    'nChar',
    'nVarChar',
    'longVarChar'
}


class MSTRApp:
    def __init__(self) -> None:
        self.base_url = os.getenv("MSTR_BASE_URL")
        self.username = os.getenv("MSTR_USERNAME")
        self.password = os.getenv("MSTR_PASSWORD")
        self.destinationFolderID = os.getenv("MSTR_DESTINATIONFOLDERID")
        self.elem_count_OLAP = []
        self.elem_count_MTDI = []
        self.elem_exceeded_limit = []
        self.setup_logging()
        self.login()
        a = 1

    # logout of the MSTR session and clear the cookies when the object is out of scope
    def __del__(self):
        url = self.base_url + "/api/auth/logout"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken
        }
        response = requests.request("POST", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        self.cookies.clear()
        logging.info("Logged out of MSTR")

    def login(self, login_mode = 1, api_token = None):
        url = self.base_url + "/api/auth/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "loginMode": 16 if self.base_url.startswith("https://aqueduct") else login_mode,
            "maxSearch": 3,
            "workingSet": 10,
            "changePassword": False,
            "newPassword": "string",
            "metadataLocale": "en_us",
            "warehouseDataLocale": "en_us",
            "displayLocale": "en_us",
            "messagesLocale": "en_us",
            "numberLocale": "en_us",
            "timeZone": "UTC",
            "applicationId": "C2B2023642F6753A2EF159A75E0CFF29",
            "applicationType": 35
        }

        if not api_token is None:
            payload = {
                "username": api_token,
                "password": api_token,
                "loginMode": DssXmlAuthApiToken
            }

        headers = {
            'Content-Type': 'application/json'
        }

        logging.info("Logging in to " + url)
        response = requests.request("POST", url, json=payload, headers=headers, verify=is_verified)
        self.cookies = response.cookies

        if response.status_code == 204:
            headers = response.headers
            self.authtoken = headers['X-MSTR-AuthToken']
            logging.info("Login success")
        else:
            logging.info("Login failed")
            logging.info(response.text)
            exit(1)

    def apiToken(self , GUID: str):
        url = self.base_url + "/api/auth/apiTokens"
        payload = {
            "userId": GUID
        }

        headers = {
            'X-MSTR-AuthToken': self.authtoken
        }

        logging.info("Get apiToken in to " + url)
        response = requests.request("POST", url, json=payload, headers=headers, cookies=self.cookies, verify=is_verified)
        self.cookies = response.cookies

        if response.status_code == 204 or response.status_code == 201:
            jsonResponse = response.json()
            self.api_token = jsonResponse["apiToken"]
            logging.info("Get apiToken success")
        else:
            logging.info("Login failed")
            logging.info(response.text)
            exit(1)
        
        self.login(login_mode=DssXmlAuthApiToken, api_token=self.api_token)
 
    def setup_logging(self):
        if os.path.exists("app.log"):
            os.remove("app.log")
            
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
            logging.FileHandler("app.log", encoding='utf-8'),
            logging.StreamHandler()
        ])

    def listProjects(self):
        """
        Get the projects from the environment

        :return: a list of project ids in the environment
        """
        url = self.base_url + "/api/projects"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 200:
            return []
        jsonResponse = response.json()

        # return the list of projects in the form of project id and project name
        return [(project['id'], project['name']) for project in jsonResponse]

        # logging.info(json.dumps(jsonResponse, indent=4, sort_keys=True))
   
    def listCertifiedDashboard(self, projID):
        """
        Get certified dashboards from a project

        :param projID: project id
        :return: a list of dashboard ids
        """

        url = self.base_url + "/api/dossiers?certifiedStatus=CERTIFIED_ONLY"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 200:
            return []
        jsonResponse = response.json()

        # return the list of projects in the form of project id and project name
        return [dashboard['id'] for dashboard in jsonResponse['result']]
    
    def listCube_CertifiedDashboard(self, projID, dashboardID):
        """
        Get the cubes used in a certified dashboard

        :param projID: the project id that the dashboard is in
        :param dashboardID: dashboard id 
        :return: a list of cube ids that's in the dashboard
        """

        url = self.base_url + "/api/v2/dossiers/" + dashboardID + "/definition"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'dossierId': dashboardID,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 200:
            return []
        jsonResponse = response.json()

        # return the list of projects in the form of project id and project name
        return [cube['id'] for cube in jsonResponse['datasets']]

    def getCubeStatus(self, projID, cubeID):
        """
        Get the status code of a cube

        :param projID: project id
        :param cubeID: cube id
        :return: status code of the cube
        """

        url = self.base_url + "/api/cubes?id=" + cubeID
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }

        params = {
        }

        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, params=params, verify=is_verified)
        if response.status_code != 200:
            return ""
        response_text = response.text

        results = json.loads(response_text)
        return results["cubesInfos"][0]["status"]
    
    def searchCubes(self, projID, cubetype):
        """
        Get the MTDI/OLAP cubes in a project

        :param projID: project id
        :param cubetype: "MTDI" or "OLAP"
        :return: a list of cube ids
        """

        url = self.base_url + "/api/searches/results?type=3"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }

        params = {
        }

        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, params=params, verify=is_verified)
        response_text = response.text

        results = json.loads(response_text)
        
        # return the list of elements in the form of element id and element name  
        cubes = []
        # Ignore the cube that are not loaded
        for item in results["result"]:
            if cubetype == "OLAP" and item["subtype"] == 776 and self.getCubeStatus(projID, item["id"]) != 0:
                cubes.append((item["id"], item["name"]))
            elif cubetype == "MTDI" and item["subtype"] == 779 and self.getCubeStatus(projID, item["id"]) != 0:
                cubes.append((item["id"], item["name"]))
        
        return cubes

    def listCube(self, projID, cubetype, certified):
        """
        Set a filter for certified dashboard on cubes in the project

        :param projID: project id
        :param cubetype: "MTDI" or "OLAP"
        :param certified: true or false
        :return: a list of cube ids
        """
        if certified:
            certifiedDashboards = self.listCertifiedDashboard(projID)
            cubesInCertified = []
            for dashboard_id in certifiedDashboards:
                cubesInCertified += self.listCube_CertifiedDashboard(projID, dashboard_id)

        if cubetype == "MTDI":
            cubes = self.searchCubes(projID, "MTDI")
        elif cubetype == "OLAP":
            cubes = self.searchCubes(projID, "OLAP")
        
        if certified:
            cubeIds = [cube[0] for cube in cubes]
            certified_cubes = list(set(cubeIds).intersection(set(cubesInCertified)))
            return [(cubeID, cubeName) for cubeID, cubeName in cubes if cubeID in certified_cubes]
        return cubes

    def setFolderID(self, projID):
        """
        Get the folders in a project

        :param projID: project id
        :param cubetype: "MTDI" or "OLAP"
        :return: a list of cube ids
        """

        url = self.base_url + "/api/searches/results?type=8"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }

        params = {
        }

        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, params=params, verify=is_verified)
        response_text = response.text

        results = json.loads(response_text)
        
        for folder in results["result"]:
            if folder["acg"] == 255:
                self.destinationFolderID = folder["id"]
                return
        
        logging.INFO("No folder with full access found in this project.") 
        exit(1)

    def listAttributes(self, projID, cubeID):
        """
        Get attributes from a cube

        :param projID: project id
        :param cubeID: cube id
        :return: a list of attribute, in the form of [(attribute id, attribute name, attribute form name list, attribute form index list, base form ids)]
        """
        url = self.base_url + "/api/v2/cubes/" + cubeID
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 200:
            return []
        response_text = response.text

        objectJson = json.loads(response_text)
        attributes = objectJson['definition']['availableObjects']['attributes']
        
        attributesList = []
        for attribute in attributes:
            form_names = []
            form_indices = []
            baseFormIds = []
            for index, form in enumerate(attribute['forms']):
                if form['dataType'] in indexed_forms:
                    form_names.append(form['name'])
                    form_indices.append(index)
                baseFormIds.append(form['id'])
            attributesList.append((attribute['id'], attribute['name'], form_names, form_indices, baseFormIds))
        return attributesList
    
    def listElements_MTDI(self, projID, cubeID, attribute, element_limit = 10000, page_size = 100000):
        """
        Get the distinct element counts in an attribute in MTDI cube

        :param projID: project id
        :param cubeID: cube id
        :param attribute: (attribute id, attribute name, attribute form name list, attribute form index list, base form ids) for one attribute
        :return: distinct element counts
        """
        # paging 
        offset = 0
        all_elements = []
        element_counts = 0
        elements = self._listElements_MTDI(projID, cubeID, attribute[0], attribute[4], element_limit, page_size, offset)
        logging.info(f"fetch offset: {offset}, page_size: {page_size}.")   
        if isinstance(elements, int):
            return elements 
        all_elements.extend(elements)

        if page_size != -1:
            offset += page_size                    
            while len(elements) == page_size:
                elements = self._listElements_MTDI(projID, cubeID, attribute[0], attribute[4], element_limit, page_size, offset)
                logging.info(f"fetch offset: {offset}, page_size: {page_size}.")   
                if isinstance(elements, int):
                    return elements          
                all_elements.extend(elements)
                if len(all_elements) > element_limit:
                    logging.info(f"Attribute {attribute[0]} ignored because element count exceeds the limit {element_limit}.")
                    return 10000
                offset += page_size

        if all_elements:
            element_counts = self._countElemByForm(all_elements, attribute[3])
        return element_counts
    
    def _listElements_MTDI(self, projID, cubeID, attributeID, baseFormIds, element_limit, page_size, offset):
        """
        Return a list of lists, where each list gives the form values for an element.
        cubeID: the ID of the cube
        attributeID: the ID of the attribute
        """
        url = self.base_url + "/api/cubes/" + cubeID + "/attributes/" + attributeID + "/elements"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }

        params = {
            'limit' : -1,
            'offset' : offset,
            'baseFormIds': baseFormIds
        }

        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, params=params, verify=is_verified)
        if response.status_code == 500:
            return -1
        elif response.status_code != 200:
            return []
        response_text = response.text

        elementList = json.loads(response_text)
        # We do not index attribute that has more than 10K elements
        if len(elementList) > element_limit:
            logging.info(f"Attribute {attributeID} ignored because element count exceeds the limit {element_limit}.")
            return 10000
        
        # return the list of elements in the form of element id and element name  
        for element in elementList:
            if not 'formValues' in element:
                # logging.info("elements in element list have no formValues: "+str(elementList))
                return []
        
        return [element['formValues'] for element in elementList]
    
    def _countElemByForm(self, all_elements, form_indices):
        """
        Count distinct element number by form

        :param all_elements: the elements extracted from the cube
        :param form_indices: a list of index of forms that needs indexing
        :return: a list of distinct element count
        """
        elements = pd.DataFrame(all_elements)
        selected_forms = elements[form_indices]
        distinct_counts = selected_forms.apply(lambda col: col.nunique())
        return distinct_counts.tolist()

    def _createReport(self, projID, body):
        """
        Create a new report based on the OLAP cube attribute

        :param projID: project id
        :param body: json body for REST request
        :return: new report's report id and instance id
        """
        url = self.base_url + "/api/model/reports"
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=body, cookies=self.cookies, verify=is_verified)
        response_text = response.text

        objectJson = json.loads(response_text)
        if response.status_code == 500:
            logging.info(f"{response.text}")
            exit(1)
        elif response.status_code == 400:
            return -1, -1
        elif response.status_code != 201:
            return 0, 0
        
        reportID = objectJson["information"]["objectId"]
        instanceID = response.headers['x-mstr-ms-instance']
        return reportID, instanceID

    def _saveReport(self, reportID, instanceID):
        """
        Save the temporary report

        :param reportID: report id
        :param instanceID: instance id
        """
        url = self.base_url + "/api/model/reports/" + reportID + "/instances/save"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-MS-Instance': instanceID,
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 201:
            logging.info(f"!!! Report creation failed !!!")
        else:
            logging.info(f"Report creation succeeded.")

    def _delReport(self, projID, reportID):
        """
        Delete the temporary report

        :param projID: project id
        :param reportID: report id
        """
        url = self.base_url + "/api/objects/" + reportID + "?type=3"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }
        response = requests.request("DELETE", url, headers=headers, data=payload, cookies=self.cookies, verify=is_verified)
        if response.status_code != 204:
            logging.info(f"!!! Report deletion failed !!!")
            exit()
        else:
            logging.info(f"Report deletion succeed.")

    def listElements_OLAP(self, projID, reportID, attribute, element_limit = 10000, page_size = 100000):
        """
        Get the distinct element counts in an attribute in MTDI cube

        :param projID: project id
        :param cubeID: cube id
        :param attribute: (attribute id, attribute name, attribute form name list, attribute form index list, base form ids) for one attribute
        :return: distinct element counts
        """
        # paging 
        offset = 0
        all_elements = []
        element_counts = 0
        elements = self._listElements_OLAP(projID, reportID, attribute[0], attribute[4], element_limit, page_size, offset)
        logging.info(f"fetch offset: {offset}, page_size: {page_size}.")   
        if isinstance(elements, int):
            return elements
        all_elements.extend(elements)

        if page_size != -1:
            offset += page_size                    
            while len(elements) == page_size:
                elements = self._listElements_OLAP(projID, reportID, attribute[0], attribute[4], element_limit, page_size, offset)
                logging.info(f"fetch offset: {offset}, page_size: {page_size}.")   
                if isinstance(elements, int):
                    return elements          
                all_elements.extend(elements)
                if len(all_elements) > element_limit:
                    logging.info(f"Attribute {attribute[0]} ignored because element count exceeds the limit {element_limit}.")
                    return 10000
                offset += page_size

        if all_elements:
            element_counts = self._countElemByForm(all_elements, attribute[3])
        return element_counts
    
    def _listElements_OLAP(self, projID, reportID, attributeID, baseFormIds, element_limit, page_size, offset):
        """
        Return a list of lists, where each list gives the form values for an element.
        cubeID: the ID of the cube
        attributeID: the ID of the attribute
        """
        url = self.base_url + "/api/reports/" + reportID + "/attributes/" + attributeID + "/elements"
        payload = {}
        headers = {
            'X-MSTR-AuthToken': self.authtoken,
            'X-MSTR-ProjectID': projID,
            'Content-Type': 'application/json'
        }

        params = {
            'limit' : -1,
            'offset' : offset,
            'baseFormIds': baseFormIds
        }

        response = requests.request("GET", url, headers=headers, data=payload, cookies=self.cookies, params=params, verify=is_verified)
        if response.status_code == 500:
            return -1
        elif response.status_code != 200:
            return []
        response_text = response.text

        elementList = json.loads(response_text)
        # We do not index attribute that has more than 10K elements
        if len(elementList) > element_limit:
            logging.info(f"Attribute {attributeID} ignored because element count exceeds the limit {element_limit}.")
            return 10000
        
        # return the list of elements in the form of element id and element name  
        for element in elementList:
            if not 'formValues' in element:
                return []
        
        return [element['formValues'] for element in elementList]
    
    def add_record(self, cube_name, attri_name, attri_form_name, elem_count, type):
        """
        Add the count record

        :param cube_name: cube name
        :param attri_name: attribute name
        :param attri_form_name: attribute form name
        :param elem_count: distinct element count
        :param type: OLAP/MTDI/EXCEED
        """
        record = {
            'cube_name': cube_name,
            'attribute_name': attri_name,
            'attribute_form_name': attri_form_name,
            'count_number': elem_count
        }
        if type == "OLAP":
            self.elem_count_OLAP.append(record)
        elif type == "MTDI":
            self.elem_count_MTDI.append(record)
        elif type == "EXCEED":
            self.elem_exceeded_limit.append(record)
        logging.info(f"Record added: {record}")

    def countElemInCube_MTDI(self, projID, cube_ids):
        """
        Count the element in MTDI cube

        :param projID: project id
        :param cube_ids: cube ids
        """
        for cube in cube_ids:
            logging.info(f"Start counting distinct elements in Cube: {cube[1]}...")
            attributes = self.listAttributes(projID, cube[0])
            for attribute in attributes:
                distinctElemCount = self.listElements_MTDI(projID, cube[0], attribute)
                if distinctElemCount == -1:
                    logging.info(f"Cube is not published")
                    break
                elif distinctElemCount == 10000:
                    for form_index in range(len(attribute[3])):
                        self.add_record(cube[1], attribute[1], attribute[2][form_index], ">10000", "EXCEED")
                    break
                elif distinctElemCount:
                    for form_index in range(len(attribute[3])):
                        if distinctElemCount[form_index] != 0:
                            self.add_record(cube[1], attribute[1], attribute[2][form_index], distinctElemCount[form_index], "MTDI")
                else:
                    logging.info(f"There is no element of string form in attribute \"{attribute[1]}\" to be indexed")
            logging.info(f"Distinct element count of Cube: {cube[1]} done.")
    
    def countElem_MTDI(self, proj, certified_flag):
        """
        Main function of count elements in the project's MTDI cubes

        :param projID: project id
        :param certified_flag: true/false
        """
        start_time = time.time()
        mtdi_cubes = self.listCube(proj[0], "MTDI", certified_flag)
        logging.info(f"Count of MTDI cubes for indexing in {proj[1]}: {len(mtdi_cubes)}")
        self.countElemInCube_MTDI(proj[0], mtdi_cubes) 
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"The sizing time for {proj[1]}'s MTDI cubes was: {elapsed_time} seconds")
    
    def countElemInCube_OLAP(self, projID, cube_ids):
        """
        Count the element in OLAP cube

        :param projID: project id
        :param cube_ids: cube ids
        """
        for cube in cube_ids:
            logging.info(f"Start counting distinct elements in Cube: {cube[1]}...")
            attributes = self.listAttributes(projID, cube[0])
            body = generateJson.Generator().generate(attributes, self.destinationFolderID)
            reportID, instanceID = self._createReport(projID, body)
            if reportID == -1 and instanceID == -1:
                self.countManagedCube_OLAP(projID, cube, attributes)
            else:    
                self._saveReport(reportID, instanceID)
                for attribute in attributes:
                    distinctElemCount = self.listElements_OLAP(projID, reportID, attribute)
                    if distinctElemCount == -1:
                        logging.info(f"Cube is not published")
                        break
                    elif distinctElemCount == 10000:
                        for form_index in range(len(attribute[3])):
                            self.add_record(cube[1], attribute[1], attribute[2][form_index], ">10000", "EXCEED")
                            break
                    elif distinctElemCount:
                        for form_index in range(len(attribute[3])):
                            if distinctElemCount[form_index] != 0:
                                self.add_record(cube[1], attribute[1], attribute[2][form_index], distinctElemCount[form_index], "OLAP")
                    else:
                        logging.info(f"There is no element of string form in attribute \"{attribute[1]}\" to be indexed")
                self._delReport(projID, reportID)
                logging.info(f"Distinct element count of Cube: {cube[1]} done.")

    def countManagedCube_OLAP(self, projID, cube, attributes):
        """
        Additional function for OLAP cubes that contains managed objects

        :param projID: project ID
        :param cube: (cube ID, cube name)
        :param attributes: attribute list retrieved from the cube
        """
        logging.info(f"Start counting distinct elements in Cube: {cube[1]}...")
        for attribute in attributes:
            distinctElemCount = self.listElements_MTDI(projID, cube[0], attribute)
            if distinctElemCount == -1:
                logging.info(f"Cube is not published")
                break
            elif distinctElemCount == 10000:
                for form_index in range(len(attribute[3])):
                    self.add_record(cube[1], attribute[1], attribute[2][form_index], ">10000", "EXCEED")
                break
            elif distinctElemCount:
                for form_index in range(len(attribute[3])):
                    if distinctElemCount[form_index] != 0:
                        self.add_record(cube[1], attribute[1], attribute[2][form_index], distinctElemCount[form_index], "OLAP")
            else:
                logging.info(f"There is no element of string form in attribute \"{attribute[1]}\" to be indexed")
        logging.info(f"Distinct element count of Cube: {cube[1]} done.")

    def countElem_OLAP(self, proj, certified_flag):
        """
        Main function of count elements in the project's OLAP cubes

        :param proj: [project id, project name]
        :param certified_flag: true/false
        """
        start_time = time.time()
        olap_cubes = self.listCube(proj[0], "OLAP", certified_flag)
        logging.info(f"Count of OLAP cubes for indexing in {proj[1]}: {len(olap_cubes)}")
        self.countElemInCube_OLAP(proj[0], olap_cubes)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"The sizing time for {proj[1]}'s OLAP cubes was: {elapsed_time} seconds")
        
    def eliminateDup(self):
        """
        Eliminate duplications in OLAP attribute list
        """
        columns=['cube_name', 'attribute_name', 'attribute_form_name', 'count_number']
        df = pd.DataFrame(self.elem_count_OLAP, columns=columns)
        unique_df = df.drop_duplicates(subset=['attribute_name', 'attribute_form_name'])
        return unique_df
    
    def getRecordsInCSV(self, type):
        """
        Create csv files of OLAP and MTDI cubes element counts

        :param type: OLAP/MTDI/EXCEED
        """
        field_names = ['cube_name', 'attribute_name', 'attribute_form_name', 'count_number']

        if type == "OLAP":
            with open("distinct_element_count_OLAP.csv", 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                for record in self.elem_count_OLAP:
                    writer.writerow(record)
            unique_df = self.eliminateDup()
            logging.info(f"Distinct element counts in OLAP cubes so far: {unique_df['count_number'].sum()}")
        elif type == "MTDI":
            with open("distinct_element_count_MTDI.csv", 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                for record in self.elem_count_MTDI:
                    writer.writerow(record)
            df = pd.DataFrame(self.elem_count_MTDI, columns=field_names)
            logging.info(f"Distinct element counts in MTDI cubes so far: {df['count_number'].sum()}")
        elif type == "EXCEED":
            with open("distinct_element_count_EXCEED.csv", 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names[:3])
                writer.writeheader()
                for record in self.elem_exceeded_limit:
                    writer.writerow(record[:3])
            df = pd.DataFrame(self.elem_exceeded_limit, columns=field_names)
            logging.info(f"{len(df) - 1} attributes exceeded 10K limit")

