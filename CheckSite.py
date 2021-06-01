#!/usr/bin/env python

import json
# from pandas.io.json import json_normalize

import requests
import sqlite3
import pandas as pd
import datetime

def pull_object_from_website(start_month="01", start_day="01", start_year="2021", end_month="12", end_day="31",
                             end_year="2021"):
    """Pulls records from drugshortagescanada.ca.

    Pulls records falling within a specified date-range.

    Args:
        start_month (string)
        start_day (string)
        start_year (string)
        end_month (string)
        end_day (string)
        end_year (string)
>>>>>>> 240d30c0edfc38cb8fbf204450de781546110fc9

    Returns:
        requests.get().json(): JSON object with records.
    """

    parameters = {
        "term": "",
        "date_property": "updated_date",
        "date_range[date_range_start][month]": start_month,
        "date_range[date_range_start][day]": start_day,
        "date_range[date_range_start][year]": start_year,
        "date_range[date_range_end][month]": end_month,
        "date_range[date_range_end][day]": end_day,
        "date_range[date_range_end][year]": end_year,
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

        return requests.get("https://www.drugshortagescanada.ca/api/v1/search?limit=" + str(total_pages * 20),
                            headers={"auth-token": authToken}, params=parameters).json()
        # return all_time_entries
    else:
        print(searchRequest.status_code + "request was not good")
        return None


def comparison(subset1, subset2, c, conn):
    """Compares two subsets.
    subset1 represents the table in the sqliteDB
    subset2 represents the incoming table from the website
    The reason we compare two subsets is because not all entries need to be directly inserted into the table in the DB

    Args:
        subset1 (pd.DataFrame):
        subset2 (pd.DataFrame):
        c (sqlite database connection):
        conn (sqlite database connection):

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
            # means that you should just append this row, it is a new entry
            # adapted from https://pynative.com/python-sqlite-insert-into-table/
            tableToAppend = tableToAppend.append(row)
    tableToAppend.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="append")
    resolvedTableToAppend.to_sql('Resolved_Date', conn, if_exists="append")
    conn.commit()
    return True


def create_tables(subsetDB):
    """Create tables from subset/JSON object from the database?

    If there is no table then create a tables (for the first time this is ever run to set up the database)

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

def cleanup (conn, c, limit_stored_days=200,  limit_stored_after_resolved_days=200):
    ''' Cleans up the data base if it has it has entries that are stored longer than X days
    Cleans up records that have been resolved for longer than Y days

        Args:
            limit_stored_days (integer)
            limit_stored_after_resolved_days (integer)

        Returns:
            true if all database objects can be removed '''

    before_limit = datetime.datetime.now() - datetime.timedelta(days=limit_stored_days)
    time_str = before_limit.strftime('%Y-%m-%dT%H:%M:%fZ')
    # datetime.now().strftime('%Y-%m-%dT%H:%M:%fZ')
    resolved_limit = datetime.datetime.now() - datetime.timedelta(days=limit_stored_after_resolved_days)
    time_str_resolved = resolved_limit.strftime('%Y-%m-%dT%H:%M:%fZ')

    sql_delete_query = """DELETE from Drug_shortages_and_discontinuations where updated_date<= ?"""
    c.execute(sql_delete_query, (time_str,))

    sql_delete_query = """DELETE from Resolved_Date where updated_date<= ?"""
    c.execute(sql_delete_query, (time_str_resolved,))

    conn.commit()
    return True

if __name__ == "__main__":

    # TODO: introduce more descriptive variable names, obj/obj2 --> early_records/later_records?
    first_pull = pull_object_from_website(start_month="5", start_day="10", end_month="5", end_day="10")
    print(first_pull["data"][0].keys())
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    data = pd.DataFrame.from_dict(pd.json_normalize(first_pull['data']), orient="columns")

    DB = data[data['status'] != 'resolved']

    subsetDB = DB[["id", "drug.brand_name", "company_name", "updated_date", "status", "drug_strength",
                   "shortage_reason.en_reason", "shortage_reason.fr_reason", "en_discontinuation_comments",
                   "fr_discontinuation_comments"]]

    html = subsetDB.to_html()

    text_file = open("index.html", "w")
    text_file.write(html)
    text_file.close()

    # only the  first time running this function should be called to initialize the DB
    createTableVar = create_tables(subsetDB)

    second_pull = pull_object_from_website(start_month="5", start_day="11", end_month="5", end_day="11")
    data2 = pd.DataFrame.from_dict(pd.json_normalize(second_pull['data']), orient="columns")

    DB2 = data2[data2['status'] != 'resolved']

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
        print(row)

    c.execute("SELECT * FROM Resolved_Date")

    for row in c.fetchall():
        print(row)

    # run to remove all record older than X days from the table
    cleanup (conn=conn, c=c)




