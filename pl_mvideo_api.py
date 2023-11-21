import logging as _log

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from io import StringIO, BytesIO
import boto3
import requests
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

def get_data():

    cookies = {
        '_ga_BNX5WPP3YK': 'GS1.1.1700336107.7.0.1700336107.60.0.0',
        '_ga_CFMZTSS5FM': 'GS1.1.1700336107.7.0.1700336107.0.0.0',
        'MVID_CITY_ID': 'CityCZ_975',
        'MVID_KLADR_ID': '7700000000000',
        'MVID_REGION_ID': '1',
        'MVID_REGION_SHOP': 'S002',
        'MVID_TIMEZONE_OFFSET': '3',
        'MVID_ENVCLOUD': 'prod2',
        '__hash_': '2fbf022f890c5be747609d06b1360a46',
        'fgsscgib-w-mvideo': 's5izae8a324fe3a1120e5b8a91060ac5fdd48873',
        'gsscgib-w-mvideo': 'hUc/SVSGgmztX3Of0oeximZoUbC8rJzsseLHCjxrtjK596Dw0/6wQnZECkUgxNqaJ+Y8g0DKz9x3/U+sldGU+a46wHDoOG6wNs0mAXwMXRfksnyL9wxWdbB50WAvGRMibtljNstsc7xlcxa/bsrsl0DEc9O6D8Hs4wGk9CgFplY6hMSxQ54oLneQFG82YQe2OHZQfSnxgpNBmu3YYkp/P9/D6i3BsS2cwi96WC9rzXKunuLjiuwS9SAfhl5ueA==',
        'fgsscgib-w-mvideo': 's5izae8a324fe3a1120e5b8a91060ac5fdd48873',
        'gsscgib-w-mvideo': 'hUc/SVSGgmztX3Of0oeximZoUbC8rJzsseLHCjxrtjK596Dw0/6wQnZECkUgxNqaJ+Y8g0DKz9x3/U+sldGU+a46wHDoOG6wNs0mAXwMXRfksnyL9wxWdbB50WAvGRMibtljNstsc7xlcxa/bsrsl0DEc9O6D8Hs4wGk9CgFplY6hMSxQ54oLneQFG82YQe2OHZQfSnxgpNBmu3YYkp/P9/D6i3BsS2cwi96WC9rzXKunuLjiuwS9SAfhl5ueA==',
        'cfidsgib-w-mvideo': 'pp5YhKczZl+Am+zuOzYAY4TARryMcS7lEwri/lsp5RsYyIN0pMHMEsNUOnGa4bLUqN6rGfXb0sBA77d299X91sh70MP95DYmeFzHlzqJnfnerPVKa+gJw/C691qmBYZBSokDpU9p6ugLMw9X0M+VLvNHsWBBi69DdVb5XJ4=',
        'tmr_detect': '0%7C1700260968223',
        'afUserId': '659a4d1e-af64-4801-8fff-85a6819abcb1-p',
        'tmr_lvid': '6d42014a97f2726be06d238fe1bc121f',
        'tmr_lvidTS': '1700242055898',
        '_sp_id.d61c': '4794ea78-98eb-47ee-8743-871b2a0e2c51.1700242052.6.1700260963.1700259010.8d1d8e58-fc1e-461e-b8fc-ba0fa74c4aca.c0f7db55-9246-4023-ac2f-82782f0fe8a0.fdf2d5be-8eed-49a7-b27d-cc6f15288d8d.1700260962200.11',
        '_ga': 'GA1.1.1178978048.1700242053',
        'directCrm-session': '%7B%22deviceGuid%22%3A%221986d968-c4ea-4164-a67d-e050dc78cab3%22%7D',
        'mindboxDeviceUUID': '1986d968-c4ea-4164-a67d-e050dc78cab3',
        'CACHE_INDICATOR': 'true',
        'COMPARISON_INDICATOR': 'false',
        'MVID_NEW_OLD': 'eyJjYXJ0IjpmYWxzZSwiZmF2b3JpdGUiOnRydWUsImNvbXBhcmlzb24iOnRydWV9',
        'MVID_VIEWED_PRODUCTS': '',
        'bIPs': '1613809182',
        'flacktory': 'no',
        'BIGipServeratg-ps-prod_tcp80': '1694817290.20480.0000',
        'BIGipServeratg-ps-prod_tcp80_clone': '1694817290.20480.0000',
        'SMSError': '',
        'authError': '',
        'deviceType': 'desktop',
        'MVID_GTM_BROWSER_THEME': '1',
        'HINTS_FIO_COOKIE_NAME': '1',
        'JSESSIONID': 'D2JnlXTSMhjhmD2CSL03L6VzC1sMgF7Q8T9gKZpqsVnh6SVml5nN!14028060',
        'MVID_CALC_BONUS_RUBLES_PROFIT': 'true',
        'MVID_CART_MULTI_DELETE': 'true',
        'MVID_GET_LOCATION_BY_DADATA': 'DaData',
        'MVID_GUEST_ID': '23221042682',
        'MVID_OLD_NEW': 'eyJjb21wYXJpc29uIjogdHJ1ZSwgImZhdm9yaXRlIjogdHJ1ZSwgImNhcnQiOiB0cnVlfQ==',
        'MVID_YANDEX_WIDGET': 'true',
        'NEED_REQUIRE_APPLY_DISCOUNT': 'true',
        'PRESELECT_COURIER_DELIVERY_FOR_KBT': 'false',
        'PROMOLISTING_WITHOUT_STOCK_AB_TEST': '2',
        'searchType2': '2',
        'wurfl_device_id': 'generic_web_browser',
        'AF_SYNC': '1700242248615',
        'adrcid': 'A2jKFL8Zktj74inloSi6mqQ',
        '_gpVisits': '{"isFirstVisitDomain":true,"idContainer":"100025D5"}',
        'advcake_session_id': '011376e3-f880-786e-d70b-6e60b24956f6',
        'advcake_track_id': 'fbd92231-a646-9114-e887-3b849ce9a125',
        'gdeslon.ru.__arc_domain': 'gdeslon.ru',
        'gdeslon.ru.user_id': '23ab5141-b9f1-4388-8038-92cb1604dc1d',
        'uxs_uid': '99357be0-856e-11ee-8157-bfca0099a8c8',
        'flocktory-uuid': 'e4fef67c-a2ad-4a02-b185-a46d9b936ee3-4',
        '_ym_d': '1700242053',
        '_ym_uid': '1700242053847633239',
        'MVID_AB_PERSONAL_RECOMMENDS': 'true',
        'MVID_AB_UPSALE': 'true',
        'MVID_ALFA_PODELI_NEW': 'true',
        'MVID_CASCADE_CMN': 'true',
        'MVID_CHAT_VERSION': '4.16.4',
        'MVID_CREDIT_DIGITAL': 'true',
        'MVID_CREDIT_SERVICES': 'true',
        'MVID_CRITICAL_GTM_INIT_DELAY': '3000',
        'MVID_CROSS_POLLINATION': 'true',
        'MVID_DISPLAY_ACCRUED_BR': 'true',
        'MVID_EMPLOYEE_DISCOUNT': 'true',
        'MVID_FILTER_CODES': 'true',
        'MVID_FILTER_TOOLTIP': '1',
        'MVID_FLOCKTORY_ON': 'true',
        'MVID_GTM_ENABLED': '011',
        'MVID_INTERVAL_DELIVERY': 'true',
        'MVID_IS_NEW_BR_WIDGET': 'true',
        'MVID_LAYOUT_TYPE': '1',
        'MVID_MINDBOX_DYNAMICALLY': 'true',
        'MVID_NEW_LK_CHECK_CAPTCHA': 'true',
        'MVID_NEW_LK_OTP_TIMER': 'true',
        'MVID_NEW_MBONUS_BLOCK': 'true',
        'MVID_PODELI_PDP': 'true',
        'MVID_SERVICES': '111',
        'MVID_SERVICE_AVLB': 'true',
        'MVID_SINGLE_CHECKOUT': 'true',
        'MVID_SP': 'true',
        'MVID_TYP_CHAT': 'true',
        'MVID_WEB_SBP': 'true',
        'SENTRY_ERRORS_RATE': '0.1',
        'SENTRY_TRANSACTIONS_RATE': '0.5',
        'MVID_GEOLOCATION_NEEDED': 'true',
        '__lhash_': 'eefc75736a01a8841a8afc69ced97529',
    }

    headers = {
        'Accept': 'application/json',
        # 'Cookie': '_ga_BNX5WPP3YK=GS1.1.1700336107.7.0.1700336107.60.0.0; _ga_CFMZTSS5FM=GS1.1.1700336107.7.0.1700336107.0.0.0; MVID_CITY_ID=CityCZ_975; MVID_KLADR_ID=7700000000000; MVID_REGION_ID=1; MVID_REGION_SHOP=S002; MVID_TIMEZONE_OFFSET=3; MVID_ENVCLOUD=prod2; __hash_=2fbf022f890c5be747609d06b1360a46; fgsscgib-w-mvideo=s5izae8a324fe3a1120e5b8a91060ac5fdd48873; gsscgib-w-mvideo=hUc/SVSGgmztX3Of0oeximZoUbC8rJzsseLHCjxrtjK596Dw0/6wQnZECkUgxNqaJ+Y8g0DKz9x3/U+sldGU+a46wHDoOG6wNs0mAXwMXRfksnyL9wxWdbB50WAvGRMibtljNstsc7xlcxa/bsrsl0DEc9O6D8Hs4wGk9CgFplY6hMSxQ54oLneQFG82YQe2OHZQfSnxgpNBmu3YYkp/P9/D6i3BsS2cwi96WC9rzXKunuLjiuwS9SAfhl5ueA==; fgsscgib-w-mvideo=s5izae8a324fe3a1120e5b8a91060ac5fdd48873; gsscgib-w-mvideo=hUc/SVSGgmztX3Of0oeximZoUbC8rJzsseLHCjxrtjK596Dw0/6wQnZECkUgxNqaJ+Y8g0DKz9x3/U+sldGU+a46wHDoOG6wNs0mAXwMXRfksnyL9wxWdbB50WAvGRMibtljNstsc7xlcxa/bsrsl0DEc9O6D8Hs4wGk9CgFplY6hMSxQ54oLneQFG82YQe2OHZQfSnxgpNBmu3YYkp/P9/D6i3BsS2cwi96WC9rzXKunuLjiuwS9SAfhl5ueA==; cfidsgib-w-mvideo=pp5YhKczZl+Am+zuOzYAY4TARryMcS7lEwri/lsp5RsYyIN0pMHMEsNUOnGa4bLUqN6rGfXb0sBA77d299X91sh70MP95DYmeFzHlzqJnfnerPVKa+gJw/C691qmBYZBSokDpU9p6ugLMw9X0M+VLvNHsWBBi69DdVb5XJ4=; tmr_detect=0%7C1700260968223; afUserId=659a4d1e-af64-4801-8fff-85a6819abcb1-p; tmr_lvid=6d42014a97f2726be06d238fe1bc121f; tmr_lvidTS=1700242055898; _sp_id.d61c=4794ea78-98eb-47ee-8743-871b2a0e2c51.1700242052.6.1700260963.1700259010.8d1d8e58-fc1e-461e-b8fc-ba0fa74c4aca.c0f7db55-9246-4023-ac2f-82782f0fe8a0.fdf2d5be-8eed-49a7-b27d-cc6f15288d8d.1700260962200.11; _ga=GA1.1.1178978048.1700242053; directCrm-session=%7B%22deviceGuid%22%3A%221986d968-c4ea-4164-a67d-e050dc78cab3%22%7D; mindboxDeviceUUID=1986d968-c4ea-4164-a67d-e050dc78cab3; CACHE_INDICATOR=true; COMPARISON_INDICATOR=false; MVID_NEW_OLD=eyJjYXJ0IjpmYWxzZSwiZmF2b3JpdGUiOnRydWUsImNvbXBhcmlzb24iOnRydWV9; MVID_VIEWED_PRODUCTS=; bIPs=1613809182; flacktory=no; BIGipServeratg-ps-prod_tcp80=1694817290.20480.0000; BIGipServeratg-ps-prod_tcp80_clone=1694817290.20480.0000; SMSError=; authError=; deviceType=desktop; MVID_GTM_BROWSER_THEME=1; HINTS_FIO_COOKIE_NAME=1; JSESSIONID=D2JnlXTSMhjhmD2CSL03L6VzC1sMgF7Q8T9gKZpqsVnh6SVml5nN!14028060; MVID_CALC_BONUS_RUBLES_PROFIT=true; MVID_CART_MULTI_DELETE=true; MVID_GET_LOCATION_BY_DADATA=DaData; MVID_GUEST_ID=23221042682; MVID_OLD_NEW=eyJjb21wYXJpc29uIjogdHJ1ZSwgImZhdm9yaXRlIjogdHJ1ZSwgImNhcnQiOiB0cnVlfQ==; MVID_YANDEX_WIDGET=true; NEED_REQUIRE_APPLY_DISCOUNT=true; PRESELECT_COURIER_DELIVERY_FOR_KBT=false; PROMOLISTING_WITHOUT_STOCK_AB_TEST=2; searchType2=2; wurfl_device_id=generic_web_browser; AF_SYNC=1700242248615; adrcid=A2jKFL8Zktj74inloSi6mqQ; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025D5"}; advcake_session_id=011376e3-f880-786e-d70b-6e60b24956f6; advcake_track_id=fbd92231-a646-9114-e887-3b849ce9a125; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=23ab5141-b9f1-4388-8038-92cb1604dc1d; uxs_uid=99357be0-856e-11ee-8157-bfca0099a8c8; flocktory-uuid=e4fef67c-a2ad-4a02-b185-a46d9b936ee3-4; _ym_d=1700242053; _ym_uid=1700242053847633239; MVID_AB_PERSONAL_RECOMMENDS=true; MVID_AB_UPSALE=true; MVID_ALFA_PODELI_NEW=true; MVID_CASCADE_CMN=true; MVID_CHAT_VERSION=4.16.4; MVID_CREDIT_DIGITAL=true; MVID_CREDIT_SERVICES=true; MVID_CRITICAL_GTM_INIT_DELAY=3000; MVID_CROSS_POLLINATION=true; MVID_DISPLAY_ACCRUED_BR=true; MVID_EMPLOYEE_DISCOUNT=true; MVID_FILTER_CODES=true; MVID_FILTER_TOOLTIP=1; MVID_FLOCKTORY_ON=true; MVID_GTM_ENABLED=011; MVID_INTERVAL_DELIVERY=true; MVID_IS_NEW_BR_WIDGET=true; MVID_LAYOUT_TYPE=1; MVID_MINDBOX_DYNAMICALLY=true; MVID_NEW_LK_CHECK_CAPTCHA=true; MVID_NEW_LK_OTP_TIMER=true; MVID_NEW_MBONUS_BLOCK=true; MVID_PODELI_PDP=true; MVID_SERVICES=111; MVID_SERVICE_AVLB=true; MVID_SINGLE_CHECKOUT=true; MVID_SP=true; MVID_TYP_CHAT=true; MVID_WEB_SBP=true; SENTRY_ERRORS_RATE=0.1; SENTRY_TRANSACTIONS_RATE=0.5; MVID_GEOLOCATION_NEEDED=true; __lhash_=eefc75736a01a8841a8afc69ced97529',
        'Connection': 'keep-alive',
        'Accept-Language': 'ru',
        'Host': 'www.mvideo.ru',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15',
        'Referer': 'https://www.mvideo.ru/noutbuki-planshety-komputery-8/noutbuki-118/f/tolko-v-nalichii=da?from=under_search&showCount=72&page=7',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'x-set-application-id': '96595836-62d2-41b4-b2df-3d1ab4e39d28',
        'baggage': 'sentry-environment=production,sentry-public_key=1e9efdeb57cf4127af3f903ec9db1466,sentry-trace_id=2d9bc6f65c1e4500b9b0af53d346d81c,sentry-sample_rate=0.5,sentry-transaction=%2F**%2F,sentry-sampled=true',
        'sentry-trace': '2d9bc6f65c1e4500b9b0af53d346d81c-9ba5e42d08e8e729-1',
    }

    params = {
        'categoryId': '118',
        'offset': '432',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    
    params1 = {
        'categoryId': '118',
        'offset': '0',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    params2 = {
        'categoryId': '118',
        'offset': '72',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    params3 = {
        'categoryId': '118',
        'offset': '144',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    params4 = {
        'categoryId': '118',
        'offset': '216',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    params5 = {
        'categoryId': '118',
        'offset': '288',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    params6 = {
        'categoryId': '118',
        'offset': '360',
        'limit': '72',
        'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
        'doTranslit': 'true',
    }
    response = requests.get('https://www.mvideo.ru/bff/products/listing',
                            params=params, cookies=cookies, headers=headers).json()

    total_items = response.get('body').get('total')

    if total_items is None:
        return '[!] No items :('

    pages_count = 6  # math.ceil(total_items / 72)

    print(f'[INFO] Total positions: {total_items} | Total pages: {pages_count}')

    products_ids = {}
    products_description = {}
    products_prices = {}

    for i in range(pages_count):
        offset = f'{i * 72}'

        params = {
            'categoryId': '118',
            'offset': offset,
            'limit': '72',
            'filterParams': 'WyJ0b2xrby12LW5hbGljaGlpIiwiIiwiZGEiXQ==',
            'doTranslit': 'true',
        }

        response = requests.get('https://www.mvideo.ru/bff/products/listing',
                                params=params, cookies=cookies, headers=headers).json()

        products_ids_list = response.get('body').get('products')
        products_ids[i] = products_ids_list
        products_ids_str = ','.join(products_ids_list)

        params = {
            'productIds': products_ids_str,
            'addBonusRubles': 'true',
            'isPromoApplied': 'true',
        }

        response = requests.get('https://www.mvideo.ru/bff/products/prices',
                                params=params, cookies=cookies, headers=headers).json()
        material_prices = response.get('body').get('materialPrices')

        for item in material_prices:
            item_id = item.get('price').get('productId')
            item_base_price = item.get('price').get('basePrice')
            item_sale_price = item.get('price').get('salePrice')

            products_prices[item_id] = {
                'item_id': item_id,
                'item_basePrice': item_base_price,
                'item_salePrice': item_sale_price
            }

        print(f'[+] Finished {i + 1} of the {pages_count} pages')



    response1 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params1, cookies=cookies, headers=headers).json()
    response2 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params2, cookies=cookies, headers=headers).json()
    response3 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params3, cookies=cookies, headers=headers).json()
    response4 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params4, cookies=cookies, headers=headers).json()
    response5 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params5, cookies=cookies, headers=headers).json()
    response6 = requests.get('https://www.mvideo.ru/bff/products/listing',
                             params=params6, cookies=cookies, headers=headers).json()

    # print(response)

    products_ids1 = response1.get('body').get('products')
    products_ids2 = response2.get('body').get('products')
    products_ids3 = response3.get('body').get('products')
    products_ids4 = response4.get('body').get('products')
    products_ids5 = response5.get('body').get('products')
    products_ids6 = response6.get('body').get('products')

    json_data1 = {
        'productIds': products_ids1,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }

    json_data2 = {
        'productIds': products_ids2,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }
    json_data3 = {
        'productIds': products_ids3,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }
    json_data4 = {
        'productIds': products_ids4,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }
    json_data5 = {
        'productIds': products_ids5,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }
    json_data6 = {
        'productIds': products_ids6,
        'mediaTypes': [
            'images',
        ],
        'category': True,
        'status': True,
        'brand': True,
        'propertyTypes': [
            'KEY',
        ],
        'propertiesConfig': {
            'propertiesPortionSize': 5,
        },
        'multioffer': False,
    }

    # PAGE 1

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data1).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items1 = extracted.merge(properties_with_id, how='outer')
    # print(items1)

    # PAGE 2

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data2).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items2 = extracted.merge(properties_with_id, how='outer')
    # print(items2)

    # PAGE 3

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data3).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items3 = extracted.merge(properties_with_id, how='outer')
    # print(products_description3)

    # PAGE 4

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data4).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items4 = extracted.merge(properties_with_id, how='outer')
    # print(products_description4)

    # PAGE 5

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data5).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items5 = extracted.merge(properties_with_id, how='outer')
    # print(products_description5)

    # PAGE 6

    response = requests.post('https://www.mvideo.ru/bff/product-details/list',
                             cookies=cookies, headers=headers, json=json_data6).json()
    extracted = pd.json_normalize(response['body']['products'])
    properties_with_id = pd.json_normalize(response['body']['products'],
                                           record_path='propertiesPortion',
                                           meta='productId',
                                           record_prefix='propertiesPortion.',
                                           errors="ignore")
    extracted.drop(['propertiesPortion'], axis='columns', inplace=True)
    items6 = extracted.merge(properties_with_id, how='outer')
    # print(products_description6)

    df1 = pd.concat([items1, items2, items3, items4, items5, items6])
    df2 = pd.DataFrame(products_prices)
    df2 = df2.T

    print(df1, df2)
    


    # ПОЛОЖИТЬ КЛЮЧИ В Variables
    obs_endpoint_url                =   "https://storage.yandexcloud.net/"
    obs_access_key_id               =   Variable.get("obs_access_key_id")
    obs_secret_access_key           =   Variable.get("obs_secret_access_key")
    obs_bucket_target               =   "otus-project"

    obs_bucket_target_lob_name      =   "mvideo"


    session                         =   boto3.session.Session()

    s3 = session.client(
            service_name          =   's3',
            endpoint_url          =   obs_endpoint_url,
            aws_access_key_id     =   obs_access_key_id,
            aws_secret_access_key =   obs_secret_access_key)
    
    load_dttm                      =   datetime.today().strftime('%Y%m%d%H%M%S')

    csv_buffer_item  = StringIO()
    df1.to_csv(csv_buffer_item, sep=';', mode='w', index=False, encoding = 'utf-8')
    s3.put_object(Bucket=obs_bucket_target, 
                                Key='{}/{}/{}.csv'.format(obs_bucket_target_lob_name, "products", "items"), 
                                Body=csv_buffer_item.getvalue(), 
                                StorageClass='COLD')

    csv_buffer_price  = StringIO()
    df2.to_csv(csv_buffer_price, sep=';', mode='w', index=False, encoding = 'utf-8')
    s3.put_object(Bucket=obs_bucket_target, 
                                Key='{}/{}/{}.csv'.format(obs_bucket_target_lob_name, "products", "prices"), 
                                Body=csv_buffer_price.getvalue(), 
                                StorageClass='COLD')                            

    
    
def upload_data():

    obs_endpoint_url                =   "https://storage.yandexcloud.net/"
    obs_access_key_id               =   Variable.get("obs_access_key_id")
    obs_secret_access_key           =   Variable.get("obs_secret_access_key")
    obs_bucket_target               =   "otus-project"
    obs_bucket_target_lob_name      =   "mvideo"

    session                         =   boto3.session.Session()
    s3 = session.client(
            service_name          =   's3',
            endpoint_url          =   obs_endpoint_url,
            aws_access_key_id     =   obs_access_key_id,
            aws_secret_access_key =   obs_secret_access_key)

    # object_key = 'products.csv'
    products = s3.get_object(Bucket=obs_bucket_target,
                                         Key='{}/{}/{}.csv'.format(obs_bucket_target_lob_name, "products", "items"))
    body = products['Body']
    csv_string = body.read().decode('utf-8')
    df1 = pd.read_csv(StringIO(csv_string), sep=';',  encoding = 'utf-8')


    prices = s3.get_object(Bucket=obs_bucket_target,
                                         Key='{}/{}/{}.csv'.format(obs_bucket_target_lob_name, "products", "prices"))
    body = prices['Body']
    csv_string = body.read().decode('utf-8')
    df2 = pd.read_csv(StringIO(csv_string), sep=';',  encoding = 'utf-8')

    print(df1, df2)

    pg_conn = Variable.get("pg_conn")
    engine = create_engine(f'{pg_conn}')

    df1.to_sql(
    'products',
    engine,
    schema='mvideo',
    if_exists='append',
    index=False,
    dtype={
           "item_stars": sqlalchemy.types.NUMERIC},
    )
    
    df2.to_sql('prices', engine, schema='mvideo', if_exists='append', index=False)



args = {
    'owner': 'airflow',
    'catchup': 'False',
}

with DAG(
    dag_id='pl_mvideo_api',
    default_args=args,
    schedule_interval='0 6 * * 2',
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['4otus', 'API'],
    catchup=False,
) as dag:
    task_get_data = PythonOperator(
        task_id='get_data', 
        python_callable=get_data, 
        provide_context=True,
        dag=dag
    )

    task_upload_data = PythonOperator(
        task_id='upload_data', 
        python_callable=upload_data, 
        provide_context=True,
        dag=dag
    )

    task_get_data >> task_upload_data 
