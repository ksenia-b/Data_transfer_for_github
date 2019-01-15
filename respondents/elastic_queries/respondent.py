import json as simplejson


def get_from_surveys(last_id, maximum_request_size):
    query = simplejson.dumps({
        "sort": [
            {
                "###": {
                    "order": "desc"
                }
            }
        ],
        "_source": ["###", "###"],
        "size": maximum_request_size,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "###": {
                                "lt": last_id
                            }
                        }
                    },
                    {
                        "query_string": {
                            "query": "###"
                        }
                    }
                ]
            }
        }
    })
    return query


def get_aggregated_respondents(respondents_ids):
    return simplejson.dumps({
        "_source": [],
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "###": respondents_ids
                        }
                    },
                    {
                        "terms": {
                            "###": [2, 6]
                        }
                    }

                ]
            }
        },
        "aggs": {
            "profit_revenue_expenses": {
                "terms": {
                    "field": "###",
                    "size": len(respondents_ids)
                },
                "aggs": {
                    "expenses": {
                        "sum": {
                            "script": {
                                "inline": "###",
                                "lang": "painless"
                            }
                        }
                    },
                    "profit": {
                        "sum": {
                            "script": {
                                "###",
                                "lang": "painless"
    }
    }
    },
    "###": {
        "value_count": {
            "field": "###"
        }
    },
    "revenue": {
        "sum": {
            "field": "###"
        }
    }
    }
    },
    "respondents": {
        "terms": {
            "field": "###",

            "size": len(respondents_ids),
            "order": {
                "_term": "desc"
            }
        },
        "aggs": {
            "###": {
                "terms": {
                    "field": "###",
                    "size": 50
                }
            }
        }
    }
    }
    })


    def get_last_data(last_id, maximum_request_size):
        query = simplejson.dumps({
            "sort": [
                {
                    "###": {
                        "order": "desc"
                    }
                }
            ],
            "_source": ["###"],
            "size": maximum_request_size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "###": {
                                    "lt": last_id
                                }
                            }
                        }
                    ]
                }
            }
        })
        return query
