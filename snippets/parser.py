import urllib
import re


def parse_vop_projects_update_script(data):
    if len(data):
        for item in data:
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = str(item['###'])
            item['###'] = str(urllib.parse.unquote(str(item['###']))).encode().decode('utf8')
            if (item['###'] == '0000-00-00 00:00:00') or 'null':
                item['###'] = '1970-01-01T00:00:00Z'
            if (item['###'] == '0000-00-00 00:00:00') or 'null':
                item['###'] = '1970-01-01T00:00:00Z'

    return data


def parse_vop_projects(data):
    projects = {}

    if len(data):
        for item in data:
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = str(item['###'])
            item['###'] = str(item['###']).replace("\'", "").replace("\"", "")

            generic_name = item['###']
            matchObj = re.search(r'\(.*\)', generic_name) or re.search(r'\d+$', generic_name)

            if matchObj:
                matchObj = matchObj.group()
                generic_name = generic_name.replace(matchObj, '')

            item['###'] = str(generic_name)

            if not item['###'] in projects:
                projects[item['###']] = []

            projects[item['###']].append(item)

    return projects


def parse_vop_respondents_selections(cpi_in_selections):
    found_ids = []

    if len(cpi_in_selections):
        for item in cpi_in_selections:
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])

            found_ids.append(item)
        return found_ids
    else:
        return []


def parse_vop_history(surveys):
    if len(surveys):
        for item in surveys:
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = float(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])

    return surveys


def parse_vop_respondents_regdata(data):
    respondents_ids = {}

    if len(data):
        for item in data:
            ### = item['###']

            if  ### not in respondents_ids:
                respondents_ids[  ###] = []
                    respondents_ids[  ###] = item["###"]

    return respondents_ids


def parse_vop_baned(data):
    respondents_ids = {}

    if len(data):
        for item in data:
            respondent_id = item['###']
            del item['###']

            if respondent_id not in respondents_ids:
                respondents_ids[respondent_id] = []
                respondents_ids[respondent_id].append(item)

    return respondents_ids


def parse_vop_respondents(data):
    respondents = {}

    if len(data):
        for item in data:
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])

            if '###' in item and item['###'] == "0000-00-00T00:00:00Z":
                item['###'] = "1970-01-01T00:00:00Z"

            if '###' in item:
                if '###' in respondents:
                    respondents[item['###']] = []

                respondent_id = item['###']

                del item['###']

                respondents[respondent_id] = {}
                respondents[respondent_id].update(item)
    return respondents


def parse_vop_respondents_for_respondents(data):
    respondents = {}

    if len(data):
        for item in data:
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])
            item['###'] = int(item['###'])

            if item['###'] == "0000-00-00T00:00:00Z":
                item['###'] = "1970-01-01T00:00:00Z"

    return data


def parse_vop_questions_vop_answers(data):
    result = {}

    if len(data):
        for item in data:
            ### = item['###']
            item['name'] = str(urllib.parse.unquote(item['name'])).encode().decode('utf8')

            if item['name'] == '###':
                del item
                continue  # DELETE field "###" in "anketa"

            if  ### in result:
                result[  ###][item['name']] =  item['provider_answer_id']
                else:
                result[  ###] = {}
                    result[  ###][item['name']] = item['provider_answer_id']

    return result


def parse_vop_city(city):
    cities = {}

    if city:
        for item in city:
            if item["id"] not in cities:
                cities[item["id"]] = urllib.parse.unquote(item["name_ru"].replace("\'", "*")).encode().decode('utf8')

    return cities


def parse_vop_customers(data):
    customers = {}

    if data:
        for item in data:
            if item["id"] not in customers:
                customers[item["id"]] = item["name"]

    return customers


def parse_vop_history_for_check_ids(data):
    mysql_ids = set()

    if data:
        for result in data:
            mysql_ids.add(str(result['###']) + "_" + str(result['###']))

    return mysql_ids
