import json as simplejson


def query_updated_respondents_with_profit(ids):
    return simplejson.dumps({
        "size": len(respondent_ids),
        "query": {
            "terms": {
                "###": ids
            }
        }
    })
