
import requests
import json
import stripe
from pandas.io.json import json_normalize


def pullObjectFromWebsite ():
    parameters = {
        "term": "",
        "date_property": "updated_date",
        "date_range[date_range_start][month]": "5",
        "date_range[date_range_start][day]": "10",
        "date_range[date_range_start][year]": "2021",
        "date_range[date_range_end][month]": "5",
        "date_range[date_range_end][day]": "10",
        "date_range[date_range_end][year]": "2021",
        "filter_type": "_all_",
        "filter_status": "_all_",
    }

    # login
    login_request = requests.post('https://www.drugshortagescanada.ca/api/v1/login',
                                 data={"email": "bryan.paget@canada.ca", "password": "Y6yzY7QHL%ZzPhjtMT6bTND%z"})

    if login_request.status_code == 200:
        authToken = login_request.headers['auth-token']
        searchRequest = requests.get('https://www.drugshortagescanada.ca/api/v1/search?term=0011223344&page=2',
                                     headers={"auth-token": authToken}, params=parameters)


    if searchRequest.status_code == 200:
        r = searchRequest.json()
        total_pages = int(r['total_pages'])

        # results will be appended to this list
        all_time_entries = []

        # loop through all pages and return JSON object
        for page in range(1, total_pages+1):
            url = "https://www.drugshortagescanada.ca/api/v1/search?page=" + str(page)
            response = requests.get(url=url, headers={"auth-token": authToken}, params=parameters).json()
            all_time_entries.append(response)
            page =page + 1


        # prettify JSON
        #data = json.dumps(all_time_entries, sort_keys=True, indent=4)
        #print(data)
        return requests.get("https://www.drugshortagescanada.ca/api/v1/search", headers={"auth-token": authToken}, params=parameters).json()
        return all_time_entries



#customers = stripe.Data.list(limit=40)
#for customer in customers.auto_paging_iter(): # Do something with customer
 #   objectReturned = pullObjectFromWebsite()
 #   print(*objectReturned, sep="\n")
  #  data = json.dumps(objectReturned, sort_keys=True, indent=4)
  #  print(data)

import pandas as pd
obj =pullObjectFromWebsite ()
print(obj["data"][0].keys())
pd.options.display.max_columns = None
pd.options.display.max_rows = None

data = pd.DataFrame.from_dict(pd.json_normalize(obj['data']), orient="columns")
#print (data)

DB= data[data['status'] != 'resolved']
print (DB)

subsetDB = DB[["drug.brand_name", "company_name", "status", "drug_strength", "shortage_reason.en_reason", "shortage_reason.fr_reason", "en_discontinuation_comments", "fr_discontinuation_comments"]]
#print (subsetDB)
#adapted from https://pythonexamples.org/pandas-render-dataframe-as-html-table/#:~:text=To%20render%20a%20Pandas%20DataFrame,thead%3E%20table%20head%20html%20element.
html = subsetDB.to_html()
#print(html)

text_file = open("index.html", "w")
text_file.write(html)
text_file.close()

#df = df[['id', 'drug', 'created_date', 'type', 'din', 'company_name', 'atc_number', 'atc_description', 'estimated_end_date', 'actual_start_date', 'decision_reversal', 'shortage_reason', 'updated_date', 'status', 'en_drug_brand_name', 'en_drug_common_name', 'fr_drug_brand_name', 'fr_drug_common_name', 'en_ingredients', 'fr_ingredients', 'drug_strength', 'drug_dosage_form', 'drug_dosage_form_fr', 'drug_route', 'drug_route_fr', 'drug_package_quantity', 'resolved', 'avoided', 'advance', 'late_submission', 'shortage_duplicated', 'discontinuance_duplicated', 'draft', 'discontinuation_duplication', 'shortage_duplication', 'unknown_estimated_end_date', 'marker_a1', 'marker_a2', 'marker_a3', 'marker_b1', 'marker_b2', 'marker_b3', 'late_marker_disabled']]
#print (df)

#adapted from https://datatofish.com/pandas-dataframe-to-sql/
import sqlite3
conn =sqlite3.connect('Shortages.db')
c= conn.cursor()
conn.commit()
subsetDB.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="replace", index=False)


c.execute('''  
SELECT * FROM Drug_shortages_and_discontinuations
          ''')

for row in c.fetchall():
    print (row)



