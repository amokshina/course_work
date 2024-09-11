import psycopg2
from pydantic import BaseModel
from typing import List, Optional, AsyncIterator
import json
import csv
import random
import time
import hashlib
from datetime import datetime
import asyncio 
import aiohttp
import asyncpg
import aiofiles
import aiocsv
from math import ceil
import logging
import  requests

logging.basicConfig(level=logging.INFO) 

class AZS_info(BaseModel):
    objectId: int
    title: str
    rating: Optional[float] = None
    reviewsCount: Optional[int] = 0
    hoursShortStatus: Optional[str] = None
    address: Optional[str] = None
    region: Optional[str] = None
    coordinates: str
    date_of_pars: datetime
    filename_with_ts: str
    src_id: int = 713 # убрать
        
class s_AZS_address(BaseModel):
    coordinates: str
    address: json 
    filename_with_ts: Optional[str] = None
    src_id: int = 713 # убрать

    class Config:
        arbitrary_types_allowed = True

class AZS_info_loader():
    MAX_COUNT = 1000
       
    headers = [ 
                        {'User-Agent': 'Mozilla/5.0 (Linux; Android 8.1.0; 01618) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Safari/537.36'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 LikeWise/95.6.3045.46'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 5.0.2; SM-G925FQ Build/KOT49H) AppleWebKit/601.8 (KHTML, like Gecko)  Chrome/48.0.2596.174 Mobile Safari/533.9'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.0; WOW64; en-US) AppleWebKit/534.18 (KHTML, like Gecko) Chrome/53.0.2515.135 Safari/601.7 Edge/8.62737'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 10.4;; en-US Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_9; en-US) Gecko/20100101 Firefox/52.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; en-US Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux x86_64) AppleWebKit/602.31 (KHTML, like Gecko) Chrome/52.0.2200.101 Safari/536'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.3;) Gecko/20130401 Firefox/56.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 4.4; Lenovo P775 Build/Lenovo) AppleWebKit/601.16 (KHTML, like Gecko) Chrome/49.0.1058.109 Mobile Safari/603.3'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_4_2; like Mac OS X) AppleWebKit/601.49 (KHTML, like Gecko) Chrome/55.0.1379.344 Mobile Safari/602.5'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.5; x64; en-US) AppleWebKit/534.22 (KHTML, like Gecko) Chrome/54.0.1014.230 Safari/600'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; WOW64; en-US) AppleWebKit/603.23 (KHTML, like Gecko) Chrome/54.0.3178.219 Safari/537'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_9_6; like Mac OS X) AppleWebKit/534.18 (KHTML, like Gecko) Chrome/52.0.3507.255 Mobile Safari/603.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_4_0; like Mac OS X) AppleWebKit/534.23 (KHTML, like Gecko) Chrome/53.0.2636.350 Mobile Safari/601.5'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.3; WOW64; en-US) AppleWebKit/602.38 (KHTML, like Gecko) Chrome/49.0.3482.294 Safari/602.7 Edge/15.20865'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.4; Win64; x64) AppleWebKit/602.42 (KHTML, like Gecko) Chrome/55.0.2249.153 Safari/536.2 Edge/11.24923'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 4.4; LG-D951 Build/KOT49I) AppleWebKit/602.40 (KHTML, like Gecko) Chrome/49.0.1969.223 Mobile Safari/537.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; x64; en-US) AppleWebKit/537.12 (KHTML, like Gecko) Chrome/48.0.2797.136 Safari/600'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 11.0; Windows; U; Windows NT 6.3; x64; en-US Trident/7.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.3;) AppleWebKit/600.14 (KHTML, like Gecko) Chrome/55.0.3144.294 Safari/602.1 Edge/11.83083'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 9_7_4; en-US) Gecko/20100101 Firefox/54.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows; U; Windows NT 6.2; WOW64 Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.0;; en-US) Gecko/20130401 Firefox/65.4'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows; U; Windows NT 6.0; x64 Trident/6.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPod; CPU iPod OS 10_8_7; like Mac OS X) AppleWebKit/600.49 (KHTML, like Gecko) Chrome/54.0.3545.238 Mobile Safari/602.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; en-US) Gecko/20130401 Firefox/70.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 5.1.1; Nexus 8 Build/LMY48B) AppleWebKit/534.39 (KHTML, like Gecko) Chrome/54.0.3293.321 Mobile Safari/603.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows; U; Windows NT 6.3; WOW64 Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3; like Mac OS X) AppleWebKit/533.30 (KHTML, like Gecko) Chrome/47.0.2197.120 Mobile Safari/536.9'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.0; Pixel XL Build/NME91E) AppleWebKit/533.7 (KHTML, like Gecko) Chrome/55.0.2770.298 Mobile Safari/602.2'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.1; WOW64) Gecko/20130401 Firefox/48.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1; like Mac OS X) AppleWebKit/535.13 (KHTML, like Gecko) Chrome/50.0.2816.308 Mobile Safari/602.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2; like Mac OS X) AppleWebKit/600.29 (KHTML, like Gecko) Chrome/52.0.2865.400 Mobile Safari/601.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_8_9; en-US) Gecko/20130401 Firefox/53.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6) Gecko/20100101 Firefox/60.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 5.0.2; LG-D836 Build/LMY47X) AppleWebKit/602.44 (KHTML, like Gecko) Chrome/48.0.2220.225 Mobile Safari/535.1'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Linux x86_64) Gecko/20130401 Firefox/73.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.4;) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/50.0.2334.314 Safari/537'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 7_2_7; en-US) AppleWebKit/603.9 (KHTML, like Gecko) Chrome/52.0.2266.106 Safari/601'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux x86_64) Gecko/20100101 Firefox/55.9'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_5_7; like Mac OS X) AppleWebKit/600.17 (KHTML, like Gecko) Chrome/54.0.2589.236 Mobile Safari/536.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.2; WOW64) AppleWebKit/602.29 (KHTML, like Gecko) Chrome/49.0.3441.268 Safari/537'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 7.1.1; Pixel XL Build/NRD90M) AppleWebKit/603.34 (KHTML, like Gecko) Chrome/55.0.1430.110 Mobile Safari/533.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; en-US) AppleWebKit/537.42 (KHTML, like Gecko) Chrome/50.0.2011.115 Safari/536.5 Edge/12.21289'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1.1; LG-H910 Build/NRD90C) AppleWebKit/533.33 (KHTML, like Gecko) Chrome/50.0.3221.119 Mobile Safari/600.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 5.1.1; SM-G928F Build/LRX22G) AppleWebKit/603.36 (KHTML, like Gecko) Chrome/49.0.1399.111 Mobile Safari/602.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 8.0; Windows; U; Windows NT 6.3; x64 Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows; Windows NT 6.3;; en-US Trident/6.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; x64; en-US) AppleWebKit/603.19 (KHTML, like Gecko) Chrome/47.0.2235.395 Safari/603.2 Edge/10.90197'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_8; en-US) AppleWebKit/601.10 (KHTML, like Gecko) Chrome/53.0.2370.346 Safari/601'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 4.4; Nexus5 V6.1 Build/KOT49H) AppleWebKit/537.28 (KHTML, like Gecko) Chrome/55.0.3848.239 Mobile Safari/534.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 4.4.1; Nexus5 V6.1 Build/KOT49H) AppleWebKit/537.25 (KHTML, like Gecko) Chrome/48.0.1038.146 Mobile Safari/601.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 6.0; Nexus 5P Build/MMB29K) AppleWebKit/533.24 (KHTML, like Gecko) Chrome/53.0.1024.167 Mobile Safari/533.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 6.0.1; HTC One0P8B2 Build/MRA58K) AppleWebKit/535.37 (KHTML, like Gecko) Chrome/49.0.1326.332 Mobile Safari/603.0'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.2; WOW64; en-US Trident/5.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 7.0; Windows; U; Windows NT 10.1; Win64; x64 Trident/4.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux x86_64; en-US) AppleWebKit/533.1 (KHTML, like Gecko) Chrome/55.0.1424.274 Safari/600'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 4.4.4; SAMSUNG SM-E500F Build/KTU84P) AppleWebKit/601.16 (KHTML, like Gecko) Chrome/55.0.3401.262 Mobile Safari/603.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Linux i661 ; en-US) AppleWebKit/602.10 (KHTML, like Gecko) Chrome/53.0.2866.319 Safari/535'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.0; Win64; x64 Trident/6.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux i582 x86_64) Gecko/20130401 Firefox/48.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.47 (KHTML, like Gecko) Chrome/53.0.2001.138 Safari/535.1 Edge/9.12587'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Linux x86_64) Gecko/20130401 Firefox/52.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows; U; Windows NT 6.1; x64; en-US Trident/5.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux i672 x86_64; en-US) Gecko/20130401 Firefox/60.1'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux i645 x86_64; en-US) Gecko/20130401 Firefox/52.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_6_7; like Mac OS X) AppleWebKit/602.11 (KHTML, like Gecko) Chrome/53.0.1612.244 Mobile Safari/537.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 9_6_4) AppleWebKit/536.41 (KHTML, like Gecko) Chrome/53.0.3278.265 Safari/533'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPad; CPU iPad OS 9_4_0 like Mac OS X) AppleWebKit/534.5 (KHTML, like Gecko) Chrome/49.0.2624.237 Mobile Safari/536.2'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux x86_64) AppleWebKit/537.35 (KHTML, like Gecko) Chrome/51.0.3528.122 Safari/601'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPod; CPU iPod OS 7_2_6; like Mac OS X) AppleWebKit/601.7 (KHTML, like Gecko) Chrome/53.0.1452.282 Mobile Safari/537.2'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux i643 x86_64) Gecko/20130401 Firefox/60.1'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 4.4.1; HTC Onemini 2 dual sim Build/KOT49H) AppleWebKit/600.18 (KHTML, like Gecko) Chrome/48.0.3704.225 Mobile Safari/535.2'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux i642 x86_64) Gecko/20100101 Firefox/47.1'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Linux i666 ; en-US) Gecko/20100101 Firefox/46.5'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 11.0; Windows; U; Windows NT 6.0; WOW64 Trident/7.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64 Trident/6.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Linux x86_64) Gecko/20100101 Firefox/61.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Linux x86_64) Gecko/20100101 Firefox/47.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 10.5; WOW64; en-US) AppleWebKit/536.22 (KHTML, like Gecko) Chrome/55.0.2389.245 Safari/535.6 Edge/15.37336'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux i570 x86_64; en-US) AppleWebKit/601.42 (KHTML, like Gecko) Chrome/55.0.3328.216 Safari/603'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1; Xperia Build/NDE63X) AppleWebKit/600.22 (KHTML, like Gecko) Chrome/47.0.2593.285 Mobile Safari/600.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 11.0; Windows; U; Windows NT 6.0; WOW64 Trident/7.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Linux x86_64) AppleWebKit/537.39 (KHTML, like Gecko) Chrome/51.0.2638.203 Safari/603'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_7_7; en-US) AppleWebKit/533.23 (KHTML, like Gecko) Chrome/52.0.1714.206 Safari/533'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.3; x64) AppleWebKit/536.50 (KHTML, like Gecko) Chrome/52.0.1695.286 Safari/534.5 Edge/14.18672'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPod; CPU iPod OS 8_3_1; like Mac OS X) AppleWebKit/533.49 (KHTML, like Gecko) Chrome/52.0.3201.292 Mobile Safari/602.9'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_4) AppleWebKit/601.13 (KHTML, like Gecko) Chrome/53.0.1743.135 Safari/533'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Android 4.4; E:number:20-23:33 Build/24.0.B.1.34) AppleWebKit/536.20 (KHTML, like Gecko) Chrome/54.0.2138.260 Mobile Safari/533.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (Android; Android 5.0.2; SAMSUNG SM-G925M Build/KOT49H) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/50.0.2986.317 Mobile Safari/601.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_2; like Mac OS X) AppleWebKit/537.34 (KHTML, like Gecko) Chrome/51.0.1604.387 Mobile Safari/600.1'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) Gecko/20130401 Firefox/60.2'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Linux x86_64; en-US) AppleWebKit/602.7 (KHTML, like Gecko) Chrome/47.0.2771.174 Safari/603'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SAMSUNG SM-G925L Build/LRX22G) AppleWebKit/603.17 (KHTML, like Gecko) Chrome/52.0.2802.279 Mobile Safari/600.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_6_3; like Mac OS X) AppleWebKit/602.13 (KHTML, like Gecko) Chrome/47.0.3327.305 Mobile Safari/533.6'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.4; x64; en-US) AppleWebKit/603.40 (KHTML, like Gecko) Chrome/52.0.2060.112 Safari/535.7 Edge/14.21260'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux i682 ; en-US) AppleWebKit/600.34 (KHTML, like Gecko) Chrome/50.0.2854.164 Safari/602'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; Windows NT 6.2; WOW64) Gecko/20130401 Firefox/69.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (Linux; U; Linux x86_64) Gecko/20100101 Firefox/61.8'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 10.0;) AppleWebKit/534.22 (KHTML, like Gecko) Chrome/50.0.3787.250 Safari/535.8 Edge/14.56578'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 7_2_9; en-US) AppleWebKit/603.40 (KHTML, like Gecko) Chrome/47.0.1185.213 Safari/601'}
                        ,{'User-Agent': 'Mozilla/5.0 (U; Linux x86_64) AppleWebKit/537.40 (KHTML, like Gecko) Chrome/50.0.3561.171 Safari/535'}
                        ,{'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows; Windows NT 6.2; WOW64 Trident/5.0)'}
                        ,{'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_1) Gecko/20130401 Firefox/72.7'}
                        ,{'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_3_9; like Mac OS X) AppleWebKit/603.23 (KHTML, like Gecko) Chrome/47.0.3403.157 Mobile Safari/603.7'}
                    ]

    def __init__(self, info_file = '', coordinates_file=''): 
        self.info_file = info_file
        self.coordinates_file = coordinates_file

        self.pars_date_i = self.get_pars_date(info_file)

        self.con_pool = None

        try:
            with open(coordinates_file, 'r', encoding='utf-8' ) as coord_f:
                try:
                    self.dict_coord = json.load(coord_f) 
                except json.JSONDecodeError as e:
                    logging.warning(f"Ошибка при декодировании JSON: {e}")
                    return 
                except Exception as ex:
                    raise
        except FileNotFoundError:
            self.dict_coord = {}

    @staticmethod
    def get_pars_date(filename):
        date_str = filename.split('_')[1]
        time_str = filename.split('_')[2].split('.')[0]

        datetime_str = f"{date_str} {time_str}"

        timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H-%M-%S')

        return timestamp

    @staticmethod
    def coordinates_encode(coordinates):
        return hashlib.md5((str(coordinates[0]) + ':' + str(coordinates[1])).encode()).hexdigest()
    

    def get_place_by_coordinates(self, coordinates, retries=20, delay=2):  
        hash_coord = self.coordinates_encode(coordinates)
        try:
            if self.dict_coord.get(hash_coord, None):
                return self.dict_coord.get(hash_coord)
        except MemoryError as er:
            print(f"memory error with get coord: {e}. Coordinates: {coordinates}" )
        
        attempt = 0
        lat, lon = coordinates[0], coordinates[1] 
        url = f"https://nominatim.openstreetmap.org/search?q={lat}%2C+{lon}&format=jsonv2&addressdetails=1&limit=1"
        while attempt <= retries:
            try: 
                ran_head = self.headers[random.randrange(0, len(self.headers), 1)] # выбор рандомного user agent
                response = requests.get(url, headers = ran_head)
                if response.status_code != 200:
                    if response.status_code == 403:
                        print(f'403! coordinates = {coordinates} attempt = {attempt}.')
                else:
                    data = response.json()
                    if data:
                        address = data[0].get('address')
                        self.dict_coord.update({hash_coord: address})
                        return address

            except requests.exceptions.ProxyError as er:
                print(f"Proxy error: {er}")
                time.sleep(60)
            
            except requests.exceptions.RequestException as e:
                print(f"Ошибка запроса: {e}. attempt = {attempt}. url = {url}. response = {response}") 
            
            except Exception as ex:
                raise
            
            attempt += 1
            time.sleep(delay)


    def coord_to_file(self):
        with open (coord_file, 'w', encoding='utf-8') as f:  
            f.write(json.dumps(self.dict_coord, ensure_ascii=False))

    def truncate_tables():
        truncate_query = """
            truncate sandbox.copy_buf_AZS_info;
            truncate sandbox.copy_buf_s_AZS_address;      
        """
        try:
            with psycopg2.connect(dbname='',  
                                user='', 
                                password='', 
                                host='') as conn:
                with conn.cursor() as cursor: 
                    cursor.execute(truncate_query)                                 
        except Exception as e:
            logging.warning(f"Error with connection or cursor: {e}" )

    @staticmethod
    def get_number_of_lines_in_thread(file_path, threads):
        with open(file_path, 'r', encoding='utf-8') as f:
            columns = ['objectId', 'info']
            file_reader = csv.DictReader(f, fieldnames=columns)
            next(file_reader) # пропуск первой строки с названиями колонок
            file_lines = sum(1 for line in file_reader)
            logging.debug(f"File lines: {file_lines}")
            return ceil(file_lines/threads)

    async def read_file_lines(self, start, end) -> AsyncIterator[str]:
        async with aiofiles.open(self.info_file, 'r', encoding='utf-8') as f:
            columns = ['objectId', 'info']
            file_reader = aiocsv.AsyncDictReader(f, fieldnames=columns)
            await f.readline() 
            index = -1
            async for line in file_reader:
                index += 1
                if start<=index<end:
                    logging.debug(f"read_file_lines start:{start}, end:{end}. index:{index}")
                    yield line

    async def process_line(self, line, data_to_insert_s_AZS_address, data_to_insert_AZS_info):
        json_data = line.get('info')
        try:
            json_data = json.loads(json_data.strip())
            place = await asyncio.to_thread(self.get_place_by_coordinates, json_data.get('coordinates')[::-1]) # reverse
            country = place.get('country')

            if place.get('country') == 'Россия':
                hash_coord = self.coordinates_encode(json_data.get('coordinates')[::-1])
                azs_info = AZS_info( 
                    objectId=json_data.get('oid'),
                    title=json_data.get('title'),
                    rating=json_data.get('rating', None),
                    reviewsCount=json_data.get('reviewsCount', None),
                    hoursShortStatus=json_data.get('hoursShortStatus', None),
                    address=json_data.get('address', None),
                    region = place.get('state', None), 
                    coordinates = hash_coord,
                    date_of_pars = self.pars_date_i,
                    filename_with_ts = self.info_file
                    # src_id =
                    )
                
                s_azs_address = s_AZS_address(
                    coordinates = hash_coord,
                    address = json.dumps(place, ensure_ascii=False),
                    filename_with_ts = self.info_file
                    # src_id =
                )

                data_to_insert_AZS_info.append((azs_info.objectId, azs_info.title, azs_info.rating, azs_info.reviewsCount, azs_info.hoursShortStatus, 
                                                                    azs_info.address, azs_info.region, azs_info.coordinates, azs_info.date_of_pars, azs_info.filename_with_ts, azs_info.src_id))
                    
                data_to_insert_s_AZS_address.append((s_azs_address.coordinates, s_azs_address.address, s_azs_address.filename_with_ts, s_azs_address.src_id))

                if len(data_to_insert_s_AZS_address) > 1000:
                    await self.load_data(schema='sandbox', table='copy_buf_s_azs_address', values=data_to_insert_s_AZS_address)

                if len(data_to_insert_AZS_info) > 1000:
                    await self.load_data(schema='sandbox', table='copy_buf_azs_info', values=data_to_insert_AZS_info)

            elif country is None: logging.info(f"COUNTRY IS NONE. {json_data}")

        except json.JSONDecodeError as e:
            logging.warning(f"Ошибка при декодировании JSON: {e}")
        
        except Exception as ex:
            raise

    async def load_data(self, schema, table, values):
        attempt = 0
        max_retries = 10

        num_columns = len(values[0])

        # placeholders = ','.join(f'${i+1}' for i in range(num_columns))
        # query = f"INSERT INTO {schema}.{table} VALUES ({placeholders})"

        while attempt < max_retries:
            try:
                async with self.con_pool.acquire() as con:
                    async with con.transaction():
                       # await con.executemany(query, values)
                        await con.copy_records_to_table(
                            table,
                            schema_name = schema,
                            records = values
                        )
                        values.clear()
                        logging.debug(f"Data inserted into {schema}.{table}")
                break  

            except asyncpg.exceptions.DeadlockDetectedError as e:
                attempt += 1
                logging.warning(f"Deadlock. Attempt {attempt}. Error: {e}")
                await asyncio.sleep(1)

            except Exception as ex:
                logging.warning(f"Exception with load_data: {ex}")
                break 

    async def _process_lines(self, start, end):
        data_to_insert_s_AZS_address = []
        data_to_insert_AZS_info = []
        async for line in self.read_file_lines(start, end):
            await self.process_line(line, data_to_insert_s_AZS_address, data_to_insert_AZS_info) 
        if data_to_insert_s_AZS_address or data_to_insert_AZS_info:
            logging.debug(f"call load_data in _process_lines {start}, {end}" )
            await self.load_data(schema='sandbox', table='copy_buf_s_azs_address', values=data_to_insert_s_AZS_address)
            await self.load_data(schema='sandbox', table='copy_buf_azs_info', values=data_to_insert_AZS_info)

    async def run_insert(self, parralel_task: int = 10):       
        max_conn_pool = (50 if parralel_task > 50 else parralel_task)
        try:
            self.con_pool = await asyncpg.create_pool(host=
                                                    ,port=
                                                    ,user=
                                                    ,password=
                                                    ,database=
                                                    ,min_size=1
                                                    ,max_size=max_conn_pool)
        except Exception as e:
            logging.warning(f"Error creating connection pool: {e}")

        AZS_info_loader.truncate_tables() ## потом убрать

        lines_per_task = AZS_info_loader.get_number_of_lines_in_thread(self.info_file, parralel_task)

        dt_start = datetime.now()
        lst_task = []
        for i in range(parralel_task):
            start = i*lines_per_task
            end = start + lines_per_task 
            lst_task.append(asyncio.create_task(self._process_lines(start, end)))

        result = await asyncio.gather(*lst_task, return_exceptions=False)

        self.coord_to_file()

        try:   # вызов функции sql
            async with self.con_pool.acquire() as con: 
                async with con.transaction():
                    await con.execute("select sandbox.fn_load_copy_testing_info()")
                    logging.info('function_call')
        except Exception as e:
            logging.warning(f"Error with call func: {e}")

        dt_end = datetime.now()
        print("---------TIME------------")
        print('dt_start-', dt_start, 'dt_end-', dt_end)

    
if __name__ == '__main__':
    filename_i = "info_2024-07-16_10-13-34.csv"
    coord_file = 'coordinates.json'
    azs_info_loader = AZS_info_loader(filename_i, coord_file)
    asyncio.run(azs_info_loader.run_insert(parralel_task=10))