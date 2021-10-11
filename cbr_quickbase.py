import pandas as pd
import requests
from dateutil import parser

class CBR_QB:
    def __init__(self, realm, usertoken, logging=False):
        self.logging=logging
        self.headers = {
            'QB-Realm-Hostname': realm,
            'Authorization': f'QB-USER-TOKEN {usertoken}'
        }

    def _process_dates(self, cell):
        if cell and type(cell) == str:
            if cell[:4] == '1900':
                return ''
            else:
                return parser.isoparse(cell).astimezone()
        else:
            if not type(cell) == str:
                print(type(cell))
            return cell 

    def _qb_to_df(self,r):
        df = pd.json_normalize(r.json()['data'])
        fields = pd.DataFrame(r.json()['fields'])
        fields_mapping = dict(zip(fields['id'], fields['label']))
        df.rename(columns=lambda x: fields_mapping[int(x.replace('.value', ''))], inplace=True)
        df.set_index('Related Site', inplace=True)  
        df = df.applymap(self._process_dates)
        df.reset_index(inplace=True)
        return df

    def query_records(self, tableid, fieldslist, query):
        body = {
            "from": tableid,
            "select": fieldslist,
            "where": query
        }
        r = requests.post(
            'https://api.quickbase.com/v1/records/query', 
            headers = self.headers, 
            json = body
        )
        return self._qb_to_df(r)

    def _process_dataframe_for_update(self, df, tableid):
        data = []
        for _, row in df.iterrows():
            mapping = {}
            for column in list(df.columns):
                mapping[str(column)] = {
                    "value": row[column]
                }
            data.append(mapping)
        return {
            "to": tableid,
            "data": data
        }

    #Dataframe must have specific form
    #Columns are field IDs, rows contain the data, no index
    def update_records_with_dataframe(self, df, tableid):
        body = self._process_dataframe_for_update(df, tableid)
        r = requests.post(
            'https://api.quickbase.com/v1/records', 
            headers = self.headers, 
            json = body
        )
        print(r.json()) if self.logging else None
        return 
    

if __name__ == '__main__':
    import numpy as np
    qb_client = CBR_QB('thecbrgroup', 'b5qarz_i55j_0_cqcsib22aqqtcfrxvsizrdtam')
    df = pd.DataFrame([['TEST', 'ABC123']], columns=[6, 27])
    print(df.head())
    qb_client.update_records_with_dataframe(df, 'bq8fc85sy')