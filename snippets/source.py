from system import constant as constant
from system import database as database
import pymysql

import json as simplejson

def get_unique_ids(data, id_name):
    id_name = str(id_name)
    ids = set()

    if not data:
        return list()

    for value in data['hits']['hits']:
        if id_name in value["_source"]:
            ids.add(value["_source"][id_name])

    return list(ids)


def _get_id(server_name, index_name, type_name, order_field, order):
    query = simplejson.dumps({
        "sort": [
            {
                order_field: {
                    "order": str(order)
                }
            }
        ],
        "size": 1,
        "_source": order_field  # to get only one field
    })

    try:
        last_row = search_in_elastic(server_name, index_name, type_name, query)
    except Exception as e:
        print("Error :" + str(e))
        return 0

    if len(last_row["hits"]["hits"]):
        return last_row["hits"]["hits"][0]["_source"][order_field]

    return 0


def get_last_id(server_name, index_name, type_name, order_field):
    return _get_id(server_name, index_name, type_name, order_field, 'desc')


def get_first_id(server_name, index_name, type_name, order_field):
    return _get_id(server_name, index_name, type_name, order_field, 'asc')


def search_in_elastic(server_name, index_name, data_type, body_data):

    result = server_name.search(
            index=index_name,
            doc_type=data_type,
            body=body_data,
            # filter_path=filter_path
            )

    if result["hits"]["total"] == 0:
        print("No data in ", index_name)
        return []
    else:
        return result


def save_data(server_name, body_data, index_name, data_type):
    return check_for_errors(server_name.bulk(index = index_name, doc_type = data_type, body = body_data, refresh = True))


def check_for_errors(response):
    if "error" in response:
        print('Query returned error value')
        return True


def get_from_mysql(query):
    try:
        conn = pymysql.connect(
            host=database.HOST,
            port=3306,
            user=database.USER,
            passwd=database.PASSWORD,
            db=database.DB,
            charset='utf8')
        cur = conn.cursor()
        cur.execute(query)

    except Exception as e:
        print("Error1 :" + str(e))
        print("Error1 Query =", query)
        cur.close()
        conn.close()
        return

    if not cur.rowcount:
        # print("No results found in MySql!")
        cur.close()
        conn.close()
        return []
    else:
        # Returns all rows from a cursor as a list of dicts
        desc = cur.description
        results = [dict(zip([col[0] for col in desc], row)) for row in cur.fetchall()]

        cur.close()
        conn.close()
        return results

