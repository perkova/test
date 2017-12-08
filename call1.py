# -*- coding: utf-8 -*-

import httplib2
import math
import datetime
import sqlalchemy
import pandas as pd
import sqlite3
import os
import subprocess

from configs import *

from datetime import date
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta


START_DATE = date(2017, 3, 1)
END_DATE = date(2017, 3, 30)


def build_service():
    http = ServiceAccountCredentials.from_json_keyfile_name(
        PRIVATE_KEY, SCOPE).authorize(httplib2.Http())
    return build('analytics', 'v3', http=http)


def proceed_ga_query(profile_id, start_date, end_date):
    return build_service().data().ga().get(
        ids='ga:' + profile_id,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        metrics='ga:totalEvents',
        dimensions='ga:pagePath',
        filters='ga:eventLabel==call,ga:eventAction==phone',
        samplingLevel='HIGHER_PRECISION',
        start_index=1,
        max_results=10000
    ).execute()

def database(host, database):
    return sqlalchemy.create_engine(
        'mysql://{user}:{password}@{host}/{database}?charset=utf8'
        '&use_unicode=1'.format(user=DATABASE_USER,
                                password=DATABASE_PASS,
                                host=host,
                                database=database))

def get_data_from_database():
    with database(DATABASE_HOST, DATABASE_NAME).connect() as db_conn:
        page_ids = db_conn.execute(u"""
                                      SELECT page, page_uk, companies.name
                                      FROM buildings_to_companies
                                      JOIN buildings ON buildings.building_id=buildings_to_companies.building_id
                                      JOIN site_pages ON site_pages.params=CONCAT('building_id=', buildings_to_companies.building_id)
                                      JOIN companies ON buildings_to_companies.company_id=companies.company_id
                                      WHERE is_primary = 1
                                      AND route = 'building/view'
                                      AND is_active = 'yes'
                                    """).fetchall()
        dict_of_page_to_comp = dict()
        for el in page_ids:
            dict_of_page_to_comp[el[0]] = el[2]
            dict_of_page_to_comp['/uk' + el[1]] = el[2]
    return dict_of_page_to_comp


def get_data_for_period(profile_id, start_date, end_date):
    result = proceed_ga_query(profile_id, start_date, end_date)
    result = result.get('rows')
    dict_of_page_to_comp = get_data_from_database()
    dict_of_comp_to_count = dict()
    for el in result:
        el[0] = el[0].split('?')[0]
        if el[0] in dict_of_page_to_comp.keys():
            if dict_of_page_to_comp[el[0]] in dict_of_comp_to_count.keys():
                dict_of_comp_to_count[dict_of_page_to_comp[el[0]]] += int(el[1].encode('utf-8'))
            else:
                dict_of_comp_to_count[dict_of_page_to_comp[el[0]]] = int(el[1].encode('utf-8'))

    for el in dict_of_comp_to_count:
        print el + ',', dict_of_comp_to_count[el]


get_data_for_period(PROFILE_ID, START_DATE, END_DATE)