import logging
import re
from constants import ALLOWED_CONNECTION_PARAMETERS

def context_data_interceptor(context, entries, codec):
    result = {}
    for k, v in entries.items():
        key: str = k.decode(codec)
        value = v.decode(codec)
        # don't keep parameters not allowed by postgres
        if key.lower() not in ALLOWED_CONNECTION_PARAMETERS:
            continue

        if key.lower() == 'application_name':
            # truncate to postgres max_identifier_length
            value = value[:63]

        result[k] = value.encode(codec)
    
    return result


def query_interceptor(context, query) -> str:
    # remove options not supported in postgres
    patterns = [
        # r'backup yes', 
        # r'backup no', 
        # r'auto refresh no', 
        # r'auto refresh yes',
        # r'\b(diststyle)\s+\w+\b', # diststyle
        r'sortkey\s*\([^)]*\)', # sortkey
    ]

    for pattern in patterns:
        query = re.sub(pattern, '', query)

    return query


def response_interceptor(context, key: str, value: str):
    # force to use the redshift server_version of postgres 8.0.2
    # some drivers like the dbt redshift adapter validate that.
    if key == 'server_version':
       value = '8.0.2'

    return key, value