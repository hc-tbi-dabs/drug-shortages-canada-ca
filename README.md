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
