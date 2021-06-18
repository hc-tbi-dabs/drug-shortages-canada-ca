# drug-shortages-canada-ca

Tools for working with https://drugshortagescanada.ca and filtering records.
Assumptions and observations about drug shortages cite

* Some keys do not always exist 
* The id changes when the data is updated. Therefore, the id cannot be used to filter or identify the drug during the comparison

| Status      | Drug Name   | Company Name | Drug Strength | Updated Date  |
| ----------- | ----------- | ----------- | ----------- |----------- |
| Avoided shortage      | LANTUS | SANOFI-AVENTIS CANADA INC |100UNIT | 5/10/2021 | 137925 |
| Resolved      | LANTUS | SANOFI-AVENTIS CANADA INC |100UNIT | 5/11/2021 | 138648 |

* A function will remove after X amount of days from the table
* A function will compare the incoming object to the pre-existing object and insert new as needed

## UML for classes

![Screenshot 2021-06-11 134556](https://user-images.githubusercontent.com/65190279/121731639-d5842d00-cab6-11eb-9dc8-f74cd30a7ccc.png)

## ERD Diagram
![image](https://user-images.githubusercontent.com/65190279/121731757-f9e00980-cab6-11eb-8bac-479cbc66d9bf.png)

| Function	|Returns	|Inherits function from parent class (Y/N)|	Variables passed in	|Description|
| ----------- | ----------- | ----------- | ----------- |----------- |
|run	|Error if writeDB does not execute|	Y|	self|	Run is the execution of the thread that is called. It calls the other the classes enforced by the Website abstract class.|
|getAPI	|Object retuned from site|	Y	|start_month, start_day, end_month, end_day, start_year, end_year|	To obtain the JSON object from the appropriate website.|
|writeDB|		|Y|	Data frame to insert into the database|	Take the JSON object turn into a dataframe. Pass the dataframe into the function and check if the entry is already in the database, if it is then it should be appended to the resolved table. If it has not been found then put it in the drug_shortages_and_discontinuations table.| 
|createTables|		|N|	Data frame to insert into the database|	If the table drug_shortages_and_discontinuations exists then append new entries and if does not create the table.|
|cleanup	|	|Y|		|Delete all rows where the updated date is less that the limit in both the drug_shortages_and_discontinuations table and the Resolved_Date table. Delete all rows from drug_shortages_and_discontinuations table where the status is resolved.|
