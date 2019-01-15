from elasticsearch import Elasticsearch

HOST = "###"

USER = "###"
PASSWORD = "###"
DB = "##"

MAIN_GRAPH_SERVER_IP = '###'
STATISTIC_SERVER_IP = '###'

es = Elasticsearch([{'host': MAIN_GRAPH_SERVER_IP, 'port': 9200}])
es_statistic = Elasticsearch([{'host': STATISTIC_SERVER_IP, 'port': 9200}])
