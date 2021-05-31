#!/usr/bin/env python

import json
import requests
import sqlite3
import pandas as pd


def pull_object_from_website(start_month="01", start_day="01", end_month="12", end_day="31"):
    """Pulls records from drugshortagescanada.ca.

    Pulls records falling within a specified date-range.

    Args:
        start_month (string)
        start_day (string)
        end_month (string)
        end_day (string)

    Returns:
        requests.get().json(): JSON object with records.
    """

    # TODO: How to handle years?
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

    login_request = requests.post('https://www.drugshortagescanada.ca/api/v1/login',
                                 data={"email": "bryan.paget@canada.ca",
                                       "password": "Y6yzY7QHL%ZzPhjtMT6bTND%z"})

    if login_request.status_code == 200:
        authToken = login_request.headers['auth-token']
        searchRequest = requests.get('https://www.drugshortagescanada.ca/api/v1/search?term=0011223344',
                                     headers={"auth-token": authToken}, params=parameters)

    if searchRequest.status_code == 200:
        r = searchRequest.json()
        total_pages = int(r['total_pages'])

        return requests.get(
            "https://www.drugshortagescanada.ca/api/v1/search?limit=" + str(total_pages * 20),
            headers={"auth-token": authToken}, params=parameters).json()
    else :
        print(searchRequest.status_code + "request was not good")
        return


def comparison(subset1, subset2, c, conn):
    """Compares two subsets.

    The reason we compare two subsets is because...

    Args:
        subset1 (pd.DataFrame):
        subset2 (pd.DataFrame):
        c (sqlite database connection):
        conn (sqlite databse connection):

    Returns:

    """

    resolvedTableToAppend = pd.DataFrame()
    
    tableToAppend = pd.DataFrame(
        columns=["id",
                 "drug.brand_name",
                 "company_name",
                 "updated_date",
                 "status",
                 "drug_strength",
                 "shortage_reason.en_reason",
                 "shortage_reason.fr_reason",
                 "en_discontinuation_comments",
                 "fr_discontinuation_comments"])

    for index, row in subset2.iterrows():
        
        dfIfReocrdUpdated = subset1.loc[
            (subset1["drug_strength"] == row[["drug_strength"]][0]) &
            (subset1["drug.brand_name"] == row[["drug.brand_name"]][0]) &
            (subset1["company_name"] == row[["company_name"]][0])]

        if dfIfReocrdUpdated.shape[0] > 0:
            # means that this record is found and has been updated
            # put the id of the drug put the new id put the updated date and
            # the new status in the resolved_date table
            dfIfReocrdUpdated = dfIfReocrdUpdated[
                ["id",
                  "company_name",
                  "drug_strength",
                  "updated_date",
                  "status",
                  "shortage_reason.en_reason",
                  "shortage_reason.fr_reason",
                  "drug.brand_name"]]
            resolvedTableToAppend = resolvedTableToAppend.append(dfIfReocrdUpdated)
        else:
            # means that you should just append this row
            # adapted from https://pynative.com/python-sqlite-insert-into-table/
            tableToAppend = tableToAppend.append(row)
    tableToAppend.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="append")
    resolvedTableToAppend.to_sql('Resolved_Date', conn, if_exists="append")
    conn.commit()
    return True


def create_tables(subsetDB):
    """Create tables from subset of database?

    Longer description.

    Args:
        subsetDB 

    Returns:
    
    """
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


if __name__ == "__main__":

    # TODO: introduce more descriptive variable names, obj/obj2 --> early_records/later_records?
    obj = pull_object_from_website(start_month="5", start_day="10", end_month="5", end_day="10")
    print(obj["data"][0].keys())
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    
    data = pd.DataFrame.from_dict(pd.json_normalize(obj['data']), orient="columns")
    
    DB= data[data['status'] != 'resolved']
    
    subsetDB = DB[["id", "drug.brand_name", "company_name", "updated_date", "status", "drug_strength", "shortage_reason.en_reason", "shortage_reason.fr_reason", "en_discontinuation_comments", "fr_discontinuation_comments"]]
    
    html = subsetDB.to_html()
    
    text_file = open("index.html", "w")
    text_file.write(html)
    text_file.close()
    
    createTableVar = create_tables(subsetDB)
   
    obj2 = pull_object_from_website(start_month="5", start_day="11", end_month="5", end_day="11")
    data2 = pd.DataFrame.from_dict(pd.json_normalize(obj2['data']), orient="columns")
    
    DB2= data2[data2['status'] != 'resolved']
    
    subsetDB2 = data2[[
        "id",
        "drug.brand_name",
        "company_name",
        "updated_date",
        "status",
        "drug_strength",
        "shortage_reason.en_reason",
        "shortage_reason.fr_reason"]]
    
    conn = sqlite3.connect('Shortages.db')
    c = conn.cursor()
    
    newTableToIngest = comparison(subsetDB, subsetDB2, c, conn)
    
    c.execute("SELECT * FROM Drug_shortages_and_discontinuations")
    
    for row in c.fetchall():
        print (row)
    
    c.execute("SELECT * FROM Resolved_Date")
    
    for row in c.fetchall():
        print(row)
