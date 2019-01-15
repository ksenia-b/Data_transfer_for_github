import sys

sys.path.append('../')

import os.path

from system import constant as constant
from system import database as database
from snippets import source as source
from snippets import parser as parser
from elastic_queries import respondent
from elastic_queries import sys_respondent
import json as simplejson
import logging
import time

### = "###.txt"
MINIMUM_ID = 0
MAXIMUM_REQUEST_SIZE = 1000

def main(last_id):
    print("last_id  = ", last_id)

    if int(last_id) < MINIMUM_ID:
        return

    respondents_in_surveys = get_respondents_from_surveys(last_id, MAXIMUM_REQUEST_SIZE)
    last_id = get_survey_min_id(respondents_in_surveys)

    respondent_ids = source.get_unique_ids(respondents_in_surveys, constant.INDEX_UNIQUE_FIELD_RESPONDENT_ID)
    system_respondents = search_updated_respondents(respondent_ids)
    
    if system_respondents:
        updated_ids = parse_ids(respondent_ids, system_respondents['hits']['hits'])
        result_ids =  list(set(respondent_ids) - set(updated_ids))
    else:
        result_ids = respondent_ids

    print("Count of results to update ", len(result_ids))

    if result_ids:
        write_to_elastic(result_ids)
    else:
        write_logs_and_run_recursively(last_id)
        return

    result_respondents = get_respondents(result_ids)

    aggregation = get_aggregated_repondents(result_ids)
    respondents_profit = aggregation['profit']
    respondents_transactions = aggregation['respondents']

    respondents = join_profit_per_respondent(parse_profit(respondents_profit), result_respondents)
    respondents = join_transactions_per_respondent(get_transactions_per_respondent(respondents_transactions), result_respondents)
    respondents = join_profit_per_partner(get_profit_per_partner(respondent_ids), result_respondents)

    update_respondents(respondents)
    write_logs_and_run_recursively(last_id)


def parse_ids(respondent_ids, system_respondents):
    ids = set()

    for item in system_respondents:
        ids.add(item['_source']['###'])

    return ids


def search_updated_respondents(respondent_ids):
    elastic_query = sys_respondent.query_updated_respondents_with_profit(respondent_ids)

    return source.search_in_elastic(database.es, constant.SYS_INDEX, constant.SYS_PROFIT_PER_RESPONDENT_TYPE, elastic_query) or {}


def write_to_elastic(result_respondents):
    bulk_data = ''
    checked_data = {}

    for id in result_respondents:
        bulk_data_firstpart = simplejson.dumps(
            {
                "update": {
                    "_id":  int(id),
                    "_type": constant.###,
                    "_index": constant.###
                }
            })

        date_now = int(time.time())
        checked_data.update({"updated_time_ts": int(date_now)})
        checked_data.update({"updated_time": convert_timestamp_to_date(date_now)})
        checked_data.update({"###": int(id)})

        bulk_data_secondpart = str(
            {
                "doc": checked_data,
                "doc_as_upsert": "true",
                "retry_on_conflict": 3
            })

        bulk_data += str(bulk_data_firstpart) + '\n' + str(bulk_data_secondpart) + '\n'

    source.save_data(database.es, bulk_data.replace("\'", "\""), constant.###, constant.SYS_PROFIT_PER_RESPONDENT_TYPE)


def get_profit_per_partner(respondent_ids):
    profit_per_respondent = {}

    for partner_id, partner_name in constant.PARTNERS.items():
        partners_profit = get_partners_profit(respondent_ids, partner_id)

        for respondents in partners_profit:
            respondent_id = respondents["key"]

            if respondent_id not in profit_per_respondent:
                profit_per_respondent.update({respondent_id: {}})

            if "###" not in profit_per_respondent[respondent_id]:
                profit_per_respondent[respondent_id].update({"###": {}})

            if partner_name not in profit_per_respondent[respondent_id]["###"]:
                profit_per_respondent[respondent_id]["###"].update({partner_name: {}, "###" : partner_id })

            profit_per_respondent[respondent_id]["###"][partner_name].update(dict(constant.STATUSES_DEFAULT, **constant.PROFIT)) 

            if "partner_statuses" in respondents:
                profit = 0
                expenses = 0
                expenses_st_3 = 0
                expenses_st_4 = 0

                for value in respondents["partner_statuses"]["buckets"]:
                    status = "STATUS_" + str(value["key"])
                    status_count = value["doc_count"]

                    profit_per_respondent[respondent_id]["###"][partner_name].update({
                        status: status_count
                    })

                    if value["key"] == 1:
                        profit = value["profit"]["value"]
                        expenses = value["expenses"]["value"]

                    if value["key"] == 3:
                        expenses_st_3 = value["expenses"]["value"]

                    if value["key"] == 4:
                        expenses_st_4 = value["expenses"]["value"]

                profit_per_respondent[respondent_id]["###"][partner_name].update({
                    "profit": round(profit, 3),
                    "revenue": round(profit + abs(expenses), 3),
                    "expenses": round(abs(expenses_st_3) + abs(expenses_st_4) + abs(expenses), 3),
                    "count": round(status_count, 3)
                })

    return profit_per_respondent


def join_profit_per_partner(profit_per_partner, respondents):
    if respondents:
        for respondent in respondents:
            respondent_id = respondent["_source"]["###"]

            if "transactions" not in respondent["_source"]:
                respondent["_source"].update({"transactions": {}})

            if "###" not in respondent["_source"]["transactions"]:
                respondent["_source"]["transactions"].update({"###": {}})

            if respondent_id in profit_per_partner:
                respondent["_source"]["transactions"]["###"].update(
                    profit_per_partner[respondent_id]["###"])

    return respondents


def write_logs_and_run_recursively(last_id):
    write_logs(last_id)
    write_last_id_to_file(last_id)
    main(last_id) 


def get_survey_min_id(respondents_in_surveys):
    total = len(respondents_in_surveys['hits']['hits']) - 1
    print("total = ", total)
    return respondents_in_surveys['hits']['hits'][total]["_id"]


def join_transactions_per_respondent(transactions, respondents):
    for respondent in respondents:
        respondent_id = respondent["_source"]["###"]
        resp_transactions = respondent["_source"]["transactions"]

        if respondent_id in transactions:
            if "statuses" not in resp_transactions:
                resp_transactions.update({"statuses" : {}})

            resp_transactions["statuses"].update({**constant.STATUSES_DEFAULT, **transactions[respondent_id]})

    return respondents


def join_profit_per_respondent(profit_per_respondent, respondents):
    for respondent in respondents:
        respondent_id = respondent["_source"]["###"]

        if respondent_id in profit_per_respondent:
            respondent["_source"].update(profit_per_respondent[respondent_id])

    return respondents


def get_respondents(respondents_ids):
    query = simplejson.dumps({
        "size": str(len(respondents_ids)),
        "query": {
            "terms": {
                "###": respondents_ids
            }
        }
    })
    result = source.search_in_elastic(database.es, constant.VOP_RESPONDENTS_INDEX, constant.VOP_RESPONDENTS_TYPE, query)

    if result:
        return result["hits"]["hits"]
    return []


def update_respondents(respondents_with_profit):
    bulk_data = ''

    for respondent in respondents_with_profit:
        id = respondent["_id"]

        bulk_data_firstpart = simplejson.dumps(
            {
                "update": {
                    "_id": id,
                    "_type": constant.VOP_RESPONDENTS_TYPE,
                    "_index": constant.VOP_RESPONDENTS_INDEX
                }
            })
        bulk_data_secondpart = str(
            {
                "doc": respondent["_source"],
                "doc_as_upsert": "true",
                "retry_on_conflict": 3
            })

        respondent = add_additional_time_fields(respondent)
        
        bulk_data += str(bulk_data_firstpart) + '\n' + str(bulk_data_secondpart) + '\n'

    bulk_data = bulk_data.replace("\'", "\"").replace("\\", "")
    save_to_elastic(bulk_data)


def add_additional_time_fields(respondent):
    date_now = int(time.time())

    respondent.update({"updated_time_ts": int(date_now)})
    respondent.update({"updated_time": convert_timestamp_to_date(date_now)})

    return respondent


def save_to_elastic(bulk_data):
    index_name = constant.VOP_RESPONDENTS_INDEX
    index_type = constant.VOP_RESPONDENTS_TYPE

    source.save_data(database.es, bulk_data, index_name, index_type)


def convert_timestamp_to_date(timestamp):
    return time.strftime("%Y-%m-%dT%TZ", time.localtime(int(timestamp)))


def get_transactions_per_respondent(respondents_transactions):
    transactions_per_respondent = {}

    for transaction in respondents_transactions:
        respondent_id = transaction["key"]
        status = ""
        status_count = 0

        if "statuses" in transaction:
            for value in transaction["statuses"]["buckets"]:
                status = "STATUS_" + str(value["key"])
                status_count = value["doc_count"]

                if respondent_id not in transactions_per_respondent:
                    transactions_per_respondent.update({respondent_id : {}})

                transactions_per_respondent[respondent_id].update({
                            status : status_count
                })

    return transactions_per_respondent


def parse_profit(respondents_profit):
    profit_per_respondent = {}

    for value in respondents_profit:
        respondent_id = value["key"]

        if respondent_id not in profit_per_respondent:
            profit_per_respondent.update({
                respondent_id: {"transactions": {}}
            })

        profit_per_respondent[respondent_id]["transactions"].update(
            {
                "revenue": round_values(value["revenue"]["value"]),
                "profit": round_values(value["profit"]["value"]),
                "expenses": round_values(value["expenses"]["value"]),
                "count": int(value["count_of_transactions"]["value"])
            })

    return profit_per_respondent

def get_aggregated_repondents(respoindents_ids):
    elastic_query = respondent.get_aggregated_respondents(respoindents_ids);
    result = source.search_in_elastic(database.es, constant.###, constant.###, elastic_query)

    return {
        "profit": result["aggregations"]["profit_revenue_expenses"]["buckets"],
        "respondents": result["aggregations"]["respondents"]["buckets"]
    }


def get_respondents_from_surveys(last_id, maximum_request_size):
    elastic_query = respondent.get_from_surveys(last_id, maximum_request_size)

    return source.search_in_elastic(database.es, constant.###, constant.###, elastic_query)


def get_partners_profit(respondents_ids, partner_id):
    elastic_query = respondent.get_profit_per_partner(respondents_ids, partner_id)
    result = source.search_in_elastic(database.es, constant.###, constant.###, elastic_query)

    return result["aggregations"]["profit_per_partner"]["buckets"]


def write_last_id_to_file(update_from_id):
    open(###, 'w+').truncate(0)  # need '0' when using r+
    open(###, 'w+').write(str(update_from_id))


def get_max_id_from_mysql():
    query = "SELECT ### FROM ### ORDER BY ### DESC LIMIT 1"

    return source.get_from_mysql(query)[0]["###"]


def get_last_id():
    last_id = source.get_last_id(database.es,
                                 constant.###,
                                 constant.###,
                                 constant.###)

    return int(last_id) or 0


def check_for_errors(response):
    if "error" in response:
        print('Query returned error value')
        return True


def round_values(value, count_after_coma=3):
    return round(float(value), count_after_coma)


def get_last_saved_id():
    if (os.path.isfile(###)):
        if open(###, 'r').read():
            return int(open(###, 'r').read())
    return int(get_last_id())


if __name__ == '__main__':
    main(get_last_saved_id())