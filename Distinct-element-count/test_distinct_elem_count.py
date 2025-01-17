import distinct_elem_count
import os
import argparse
import getpass

os.environ["MSTR_BASE_URL"] = "https://aqueduct-tech3.customer.cloud.microstrategy.com/MicroStrategyLibrary"
os.environ["MSTR_USERNAME"] = "xinyli"
os.environ["MSTR_PASSWORD"] = "1Sarah_L.010726"

def main():
    parser = argparse.ArgumentParser(description='Count distinct elements in MicroStrategy projects.')
    parser.add_argument('-projID', metavar='project_id', type=str, help='Specify project ID to run against')
    parser.add_argument('-certified', action='store_true', help='Specify if count elements against certified dashboards or all dashboards')
    parser.add_argument('-OLAP', action='store_true', help='Specify the cube type (OLAP)')
    parser.add_argument('-MTDI', action='store_true', help='Specify the cube type (MTDI)')
    args = parser.parse_args()

    if not os.getenv("MSTR_BASE_URL"):
        os.environ["MSTR_BASE_URL"] = input("Enter MSTR base URL (http://.../MicroStrategy): ")
    if not os.getenv("MSTR_USERNAME"):
        os.environ["MSTR_USERNAME"] = input("Enter MSTR username: ")
    if not os.getenv("MSTR_PASSWORD"):
        os.environ["MSTR_PASSWORD"] = getpass.getpass("Enter your MSTR password: ")

    mstr = distinct_elem_count.MSTRApp()
    
    # Default setting: against all projects
    projects = mstr.listProjects()
    if args.projID:
        project_ids = [project[0] for project in projects]
        if args.projID not in project_ids:
            print(f"Invalid Project ID {args.projID} provided.")
            exit()
        else:
            projects = [(args.projID, "specified project")]

    
    # Default setting: against certified dashboards
    certified = False
    if args.certified:
        certified = True

    # Default setting: against both OLAP and MTDI cubes
    OLAP_flag = True
    MTDI_flag = True
    if args.OLAP:
        MTDI_flag = False
    elif args.MTDI:
        OLAP_flag = False
    
    if args.OLAP and args.MTDI:
        print("Both OLAP and MTDI flags cannot be specified simultaneously.")
        exit()

    for proj in projects:
        mstr.setFolderID(proj[0])
        if OLAP_flag:
            mstr.countElem_OLAP(proj, certified)
        if MTDI_flag:
            mstr.countElem_MTDI(proj, certified)

    mstr.getRecordsInCSV("OLAP")
    mstr.getRecordsInCSV("MTDI")
    mstr.getRecordsInCSV("EXCEED")

if __name__ == "__main__":
    main()