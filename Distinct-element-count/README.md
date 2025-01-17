# Element sizing Tool

## Pre-requisite
### Packages
```
requests
pandas
argparse
```
### Reports
There must not be a report named "Temp Report" existed in the current environment in the requested destination folder
## Usage
### Running the Script

Navigate to the project root folder and execute the following command in your terminal:
* Run ```python test_distinct_elem_count.py```

#### Flags
* ```-projID``` : Specifies the project ID to analyze.
  * Default setting: the script would run through the whole environment
* ```-certified``` : Specify to analyze only the certified ones
  * Default setting: analyze all dashboards
* ```-OLAP```: Specify to analyze only OLAP cubes
  * Default setting: analyze both OLAP & MTDI cubes
* ```-MTDI```: Specify to analyze only MTDI cubes
  * Default setting: analyze both OLAP & MTDI cubes
* **Both OLAP and MTDI flags cannot be specified simultaneously.** 

### Input Parameters
You will be prompted to enter the following information:
* ```Enter MSTR base URL (http://.../MicroStrategy):``` : enter the based URL
* ```Enter MSTR username:``` : username
* ```Enter your MSTR password:``` : password
* ```Enter a destination folder id to store temporary report:``` : enter a destination folder ID to store temporarily created report
  * Recommended: ```[Your Project]/Public Object```
  * To get folder ID, right click on the ```[folder] -> Properties -> General -> ID```

Upon successful login, you will see ```Login success``` in the terminal.

### Output 
Once the script has completed execution, you will see ```Logged out of MSTR``` in the terminal.

Two CSV files will be generated in the root folder:
* ```distinct_element_count_OLAP.csv```: Contains the count of distinct elements for OLAP cubes
* ```distinct_element_count_MTDI.csv```: Contains the count of distinct elements for MTDI cubes

If you specified only one type of cube, then only the correspond csv file will be created.

## Examples
1. Analyze certified dashboards with OLAP cubes
```
python test_distinct_elem_count.py -certified -OLAP
```
2. Analyze all dashboards with MTDI cubes
```
python test_distinct_elem_count.py -MTDI
```
3. Analyze dashboards from project 000000 (project ID)
```
python test_distinct_elem_count.py -projID 000000
```
4. Analyze all dashboards including MTDI and OLAP cubes
```
python test_distinct_elem_count.py
```
### Wrong usage
```
python test_distinct_elem_count.py -OLAP -MTDI
```
Will return ```Both OLAP and MTDI flags cannot be specified simultaneously.``` and exit process.
