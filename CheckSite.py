#!/usr/bin/env python

import json
# from pandas.io.json import json_normalize
import threading
import requests
import sqlite3
import pandas as pd
import datetime
from abc import ABC, abstractmethod

class Website(ABC, threading.Thread):
    @abstractmethod
    def getAPI(self):
        # obtains the API request from the website and returns a JSON object to parse
        pass
    def writeDB(self):
        # create the DB and put the JSON object into it
        pass
    def cleanup(self):
        #remove all outdated lines from the table in the DB
        pass
    def run(self):
        # thread run functionality
        pass


class DrugShortages(Website):
    def run(self):
        first_pull =self.getAPI(start_month="5", start_day="10", end_month="5", end_day="10")
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

        createTableVar = self.create_tables(subsetDB)

        second_pull = self.getAPI(start_month="5", start_day="11", end_month="5", end_day="11")
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

        # write the subsets into the database
        self.writeDB(subsetDB, subsetDB2)

        # run to remove all record older than X days from the table
        self.cleanup()

    def getAPI(self, start_month="01", start_day="01", start_year="2021", end_month="12", end_day="31",
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


    def writeDB(self, subset1, subset2):
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
            true after all additions to the db are completed
        """
        conn = sqlite3.connect('Shortages.db')
        c = conn.cursor()
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
            true if all the saves to the DB happen
        """
        # adapted from https://datatofish.com/pandas-dataframe-to-sql/
        conn = sqlite3.connect('Shortages.db')
        c = conn.cursor()
        conn.commit()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Drug_shortages_and_discontinuations' ''')

        # if the count is 1, then table exists
        if c.fetchone()[0] == 1:
            # if table exists append the subset onto existing table
            # if the entries are already in the table nothing should be added, if id already exists then do not enter it

        else:
            # since the table DNE then we should create it based on the JSON object
            subsetDB.to_sql('Drug_shortages_and_discontinuations', conn, if_exists="replace", index=True)

        try:
            c.execute("""CREATE TABLE IF NOT EXISTS Resolved_Date
                        (id_resolved Primary key AUTOINCREMENT, updated_date, status, constraint id_shortage foreign key (id) References Drug_shortages_and_discontinuations(id))""")
        except Exception as e:
            pass
            return False
        conn.commit()
        c.close()
        return True

    def cleanup (self, limit_stored_days=200,  limit_stored_after_resolved_days=200):
        ''' Cleans up the data base if it has it has entries that are stored longer than X days
        Cleans up records that have been resolved for longer than Y days

            Args:
                limit_stored_days (integer)
                limit_stored_after_resolved_days (integer)

            Returns:
                true if all database objects can be removed '''
        conn = sqlite3.connect('Shortages.db')
        c = conn.cursor()
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
        c.close()
        return True

if __name__ == "__main__":

    # TODO: introduce more descriptive variable names, obj/obj2 --> early_records/later_records?
    DrugAPI = DrugShortages ()
    DrugAPI.start()





