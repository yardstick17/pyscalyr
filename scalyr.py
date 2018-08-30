import requests
import json

import pandas as pd


class Scalyr(object):
    SERVER = 'https://eu.scalyr.com'
    LOG_QUERY_URI = '/api/query'

    def __init__(self, server=SERVER):
        self.server = server

    def get_logs_in_dataframe(self, **kwargs):
        """ The method query scalyr log server, retrieves data in DataFrame.

        :param kwargs: This is a dict of param.
                Example: {
                          "token":             "xxx",
                          "queryType":         "log",
                          "filter":            "...",
                          "startTime":         "...",
                          "endTime":           "...",
                          "maxCount":          nnn,
                          "pageMode":          "...",
                          "columns":           "...",
                          "continuationToken": "...",
                          "priority":          "..."
                        }
        :return: DataFrame
        """
        response = self.query_log(**kwargs)
        df = self.get_dataframe_from_response(response)
        list_of_df = [df]
        if 'continuationToken' in response:
            while 'continuationToken' in response:
                continuation_token = response['continuationToken']
                kwargs['continuationToken'] = continuation_token
                response = self.query_log(**kwargs)
                new_df = self.get_dataframe_from_response(response)
                list_of_df.append(new_df)

        single_df = pd.concat(list_of_df)
        out_file = kwargs.get('out_file', None)
        if out_file:
            single_df.to_csv(out_file, index=False)
        return single_df

    def query_log(self, **kwargs):
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.post(self.SERVER + self.LOG_QUERY_URI, headers=headers, data=json.dumps(dict(kwargs)))
        response.raise_for_status()
        response = response.json()
        return response

    def get_dataframe_from_response(self, response):
        df = pd.DataFrame([values['attributes'] for values in response['matches']])
        return df
