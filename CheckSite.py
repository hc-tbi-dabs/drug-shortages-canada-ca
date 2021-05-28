
import requests
import pandas as pd
import json
#from pandas.io.json import json_normalize


def pullObjectFromWebsite (start_month, start_day, end_month, end_day):
    parameters = {
        "term": "",
        "date_property": "updated_date",
        "date_range[date_range_start][month]": start_month,
        "date_range[date_range_start][day]": start_day,
        "date_range[date_range_start][year]": "2021",
        "date_range[date_range_end][month]": end_month,
        "date_range[date_range_end][day]": end_day,
        "date_range[date_range_end][year]": "2021",
        "filter_type": "_all_",
        "filter_status": "_all_",
    }

    # login
    login_request = requests.post('https://www.drugshortagescanada.ca/api/v1/login',
                                 data={"email": "bryan.paget@canada.ca", "password": "Y6yzY7QHL%ZzPhjtMT6bTND%z"})

    if login_request.status_code == 200:
        authToken = login_request.headers['auth-token']
        searchRequest = requests.get('https://www.drugshortagescanada.ca/api/v1/search?term=0011223344',
                                     headers={"auth-token": authToken}, params=parameters)


    if searchRequest.status_code == 200:
        r = searchRequest.json()
        total_pages = int(r['total_pages'])

        # results will be appended to this list
        #all_time_entries = []

        # loop through all pages and return JSON object
        #for page in range(0, total_pages):
          #  url = "https://www.drugshortagescanada.ca/api/v1/search?limit=20&offset=" + str(page*20)
           # response = requests.get(url=url, headers={"auth-token": authToken}, params=parameters).json()
            #all_time_entries.append(response)
            #page =page + 1


        # prettify JSON
        #data = json.dumps(all_time_entries, sort_keys=True, indent=4)
        #print(data)

        return requests.get("https://www.drugshortagescanada.ca/api/v1/search?limit="+ str(total_pages*20), headers={"auth-token": authToken}, params=parameters).json()
        #return all_time_entries
    else :
        print(searchRequest.status_code + "request was not good")
        return None


def comparison (subset1, subset2,c, conn):
    # innerJoin = pd.DataFrame.merge(subset1, subset2, how='outer', on=["drug_strength", "drug.brand_name", "company_name"])
    # print(innerJoin)
    resolvedTableToAppend = pd.DataFrame()
    tableToAppend = pd.DataFrame(columns=["id", "drug.brand_name", "company_name", "updated_date", "status", "drug_strength", "shortage_reason.en_reason", "shortage_reason.fr_reason", "en_discontinuation_comments", "fr_discontinuation_comments"])
    for index, row in subset2.iterrows():
        dfIfReocrdUpdated = subset1.loc[(subset1["drug_strength"]==row[["drug_strength"]][0]) & (subset1["drug.brand_name"]==row[["drug.brand_name"]][0]) & (subset1["company_name"]==row[["company_name"]][0])]
        if dfIfReocrdUpdated.shape[0] >0:
            # means that this record is found and has been updated
            # put the id of the drug put the new id put the updated date and the new status in the resolved_date table
            dfIfReocrdUpdated = dfIfReocrdUpdated [["id","company_name", "drug_strength", "updated_date", "status", "shortage_reason.en_reason", "shortage_reason.fr_reason", "drug.brand_name"]]
            resolvedTableToAppend = resolvedTableToAppend.append(dfIfReocrdUpdated)
        else:
            # means that you should just append this row
            # adapted from https://pynative.com/python-sqlite-insert-into-table/
            tableToAppend = tableToAppend.append(row)
    tableToAppend.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="append")
    resolvedTableToAppend.to_sql('Resolved_Date', conn, if_exists="append")
    conn.commit()
    return True

def createTables (subsetDB):
    # adapted from https://datatofish.com/pandas-dataframe-to-sql/
    conn = sqlite3.connect('Shortages.db')
    c = conn.cursor()
    conn.commit()
    subsetDB.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="replace", index=True)
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS Resolved_Date
                    (id_resolved Primary key AUTOINCREMENT, updated_date, status, constraint id_shortage foreign key (id) References Drug_shortages_and_discontinuations(id))""")
    except Exception as e:
        pass
    conn.commit()


obj =pullObjectFromWebsite ("5", "10", "5", "10")
print(obj["data"][0].keys())
pd.options.display.max_columns = None
pd.options.display.max_rows = None

data = pd.DataFrame.from_dict(pd.json_normalize(obj['data']), orient="columns")
#print (data)

DB= data[data['status'] != 'resolved']
#print (DB)

subsetDB = DB[["id", "drug.brand_name", "company_name", "updated_date", "status", "drug_strength", "shortage_reason.en_reason", "shortage_reason.fr_reason", "en_discontinuation_comments", "fr_discontinuation_comments"]]
#print (subsetDB)

#adapted from https://pythonexamples.org/pandas-render-dataframe-as-html-table/#:~:text=To%20render%20a%20Pandas%20DataFrame,thead%3E%20table%20head%20html%20element.
html = subsetDB.to_html()
#print(html)

text_file = open("index.html", "w")
text_file.write(html)
text_file.close()

#df = df[['id', 'drug', 'created_date', 'type', 'din', 'company_name', 'atc_number', 'atc_description', 'estimated_end_date', 'actual_start_date', 'decision_reversal', 'shortage_reason', 'updated_date', 'status', 'en_drug_brand_name', 'en_drug_common_name', 'fr_drug_brand_name', 'fr_drug_common_name', 'en_ingredients', 'fr_ingredients', 'drug_strength', 'drug_dosage_form', 'drug_dosage_form_fr', 'drug_route', 'drug_route_fr', 'drug_package_quantity', 'resolved', 'avoided', 'advance', 'late_submission', 'shortage_duplicated', 'discontinuance_duplicated', 'draft', 'discontinuation_duplication', 'shortage_duplication', 'unknown_estimated_end_date', 'marker_a1', 'marker_a2', 'marker_a3', 'marker_b1', 'marker_b2', 'marker_b3', 'late_marker_disabled']]
#print (df)

import sqlite3
createTableVar = createTables (subsetDB)

obj2 =pullObjectFromWebsite ("5", "11", "5", "11")
data2 = pd.DataFrame.from_dict(pd.json_normalize(obj2['data']), orient="columns")
#print (data)

DB2= data2[data2['status'] != 'resolved']
#print (DB2)

subsetDB2 = data2[["id", "drug.brand_name", "company_name", "updated_date", "status", "drug_strength", "shortage_reason.en_reason", "shortage_reason.fr_reason"]]
#print (subsetDB2)

conn = sqlite3.connect('Shortages.db')
c = conn.cursor()

newTableToIngest = comparison (subsetDB, subsetDB2, c, conn)

c.execute('''  
SELECT * FROM Drug_shortages_and_discontinuations
          ''')

for row in c.fetchall():
    print (row)

c.execute('''  
SELECT * FROM Resolved_Date
              ''')

for row in c.fetchall():
    print(row)


