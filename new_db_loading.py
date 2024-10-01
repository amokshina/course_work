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
import os


csv.field_size_limit(10**9)
logging.basicConfig(level=logging.DEBUG) 

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
        
class s_AZS_address(BaseModel):
    coordinates: str
    address: json 
    filename_with_ts: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class AZS_review(BaseModel):
    object_id: int
    comment_type: str # "/ugc/review",
    comment_time: datetime #1706891238267
    author_id: Optional[str] = None
    profession_level_num: Optional[int] = None
    rating: Optional[int] = None
    comment_text: Optional[str] = None
    likes_num: Optional[int] = None
    dislikes_num: Optional[int] = None
    date_of_pars: datetime
    file_name_with_ts: str

class s_AZS_rev_users(BaseModel):
    author_id: str
    author_name: str
    verified: Optional[bool] = None
    file_name_with_ts: Optional[str] = None

class AZS_rev_categ(BaseModel):
    object_id: int
    categ_id: int
    reviews_num: int
    pos_rev_num: int
    neg_rev_num: int
    date_of_pars: datetime
    file_name_with_ts: Optional[str] = None
    
class s_AZS_categ(BaseModel):
    categ_id: int
    title: str
    file_name_with_ts: Optional[str] = None

class AZS_info_loader():
    MAX_COUNT = 1000
    headers =   [ 
                        {'User-Agent': 'Mozilla/5.0 (Linux; Android 8.1.0; 01618) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Safari/537.36'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 LikeWise/95.6.3045.46'}
                ]

    def __init__(self, db_params: dict, file_path: str, coordinates_file: str): 
        self.db_params = db_params
        
        self.coordinates_file = coordinates_file

        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.file_size = os.path.getsize(file_path)
        self.file_type = os.path.splitext(self.file_name)[1][1:]  

        self.pars_date_i = self.get_pars_date(self.file_name)
        
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
    

    def get_place_by_coordinates(self, coordinates: list[float], retries=20, delay=2):  
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

    async def insert_file_info(self):
        async with self.con_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO ctr.reviews_file (file_name, file_path, date_pars, file_size, file_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (file_path, file_name) DO NOTHING;
            ''', self.file_name, self.coordinates_file, self.pars_date_i, self.file_size, self.file_type)


    async def truncate_tables(self, schema: str, tables: list):
        async with self.con_pool.acquire() as conn:
            for table in tables:
                try:
                    print(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
                    await conn.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
                except Exception as e:
                    logging.warning(f"Error with connection or cursor: {e}" )  

    @staticmethod
    def get_number_of_lines_in_thread(file_path: str, threads: int):
        with open(file_path, 'r', encoding='utf-8') as f:
            columns = ['objectId', 'info']
            file_reader = csv.DictReader(f, fieldnames=columns)
            next(file_reader) # пропуск первой строки с названиями колонок
            file_lines = sum(1 for line in file_reader)
            logging.debug(f"File lines: {file_lines}")
            return ceil(file_lines/threads)

    async def read_file_lines(self, start, end) -> AsyncIterator[str]:
        async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
            columns = ['objectId', 'info']
            file_reader = aiocsv.AsyncDictReader(f, fieldnames=columns)
            await f.readline() 
            index = -1
            async for line in file_reader:
                index += 1
                if start<=index<end:
                    logging.debug(f"read_file_lines start:{start}, end:{end}. index:{index}")
                    yield line

    async def process_line(self, line, data_to_insert_s_AZS_address: list , data_to_insert_AZS_info: list):
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
                    filename_with_ts = self.file_name
                    )
                
                s_azs_address = s_AZS_address(
                    coordinates = hash_coord,
                    address = json.dumps(place, ensure_ascii=False),
                    filename_with_ts = self.file_name
                )

                data_to_insert_AZS_info.append((azs_info.objectId, azs_info.title, azs_info.rating, azs_info.reviewsCount, azs_info.hoursShortStatus, 
                                                                    azs_info.address, azs_info.region, azs_info.coordinates, azs_info.date_of_pars, azs_info.filename_with_ts))
                    
                data_to_insert_s_AZS_address.append((s_azs_address.coordinates, s_azs_address.address, s_azs_address.filename_with_ts))

                if len(data_to_insert_s_AZS_address) > 1000:
                    await self.load_data(schema='buffer', table='s_azs_address', values=data_to_insert_s_AZS_address)

                if len(data_to_insert_AZS_info) > 1000:
                    await self.load_data(schema='buffer', table='azs_info', values=data_to_insert_AZS_info)

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
                logging.warning(f"Exception with load_data: {ex}.  {schema} . {table} ")
                break 

    async def _process_lines(self, start, end):
        data_to_insert_s_AZS_address = []
        data_to_insert_AZS_info = []
        async for line in self.read_file_lines(start, end):
            await self.process_line(line, data_to_insert_s_AZS_address, data_to_insert_AZS_info) 
        if data_to_insert_s_AZS_address or data_to_insert_AZS_info:
            logging.debug(f"call load_data in _process_lines {start}, {end}" )
            await self.load_data(schema='buffer', table='s_azs_address', values=data_to_insert_s_AZS_address)
            await self.load_data(schema='buffer', table='azs_info', values=data_to_insert_AZS_info)

    async def run_insert(self, parralel_task: int = 10):       
        max_conn_pool = (50 if parralel_task > 50 else parralel_task)
        try:
            self.con_pool = await asyncpg.create_pool(
                                                    user=self.db_params['user'],
                                                    password=self.db_params['password'],
                                                    database=self.db_params['database'],
                                                    host=self.db_params['host'],
                                                    port=self.db_params['port'],
                                                    min_size=1,  
                                                    max_size=max_conn_pool  
                                                )
        except Exception as e:
            logging.warning(f"Error creating connection pool: {e}")

        await self.truncate_tables(schema='buffer', tables=['s_azs_address','azs_info'])
        # self.truncate_tables()

        lines_per_task = AZS_info_loader.get_number_of_lines_in_thread(self.file_path, parralel_task)

        dt_start = datetime.now()
        lst_task = []
        for i in range(parralel_task):
            start = i*lines_per_task
            end = start + lines_per_task 
            lst_task.append(asyncio.create_task(self._process_lines(start, end)))

        result = await asyncio.gather(*lst_task, return_exceptions=False)

        await self.insert_file_info()

        self.coord_to_file()

        # try:   # вызов функции sql
        #     async with self.con_pool.acquire() as con: 
        #         async with con.transaction():
        #             await con.execute("select sandbox.fn_load_copy_testing_info()") # ПОМЕНЯТЬ ФУНКЦИЮ
        #             logging.info('function_call')
        # except Exception as e:
        #     logging.warning(f"Error with call func: {e}")

        dt_end = datetime.now()
        print("---------TIME------------")
        print('dt_start-', dt_start, 'dt_end-', dt_end)


if __name__ == '__main__':
    filename_i = 'C:\\Users\\an23m\\course_work\\data\\info_2024-09-22_22-08-25.csv'
    filename_r = 'C:\\Users\\an23m\\course_work\\data\\reviews_2024-09-22_22-11-48.csv'
    coord_file = 'data\coordinates.json'
    db_params = {
        'user': 'postgres',
        'password': 'f8ysz789',
        'database': 'azs',
        'host': 'localhost',
        'port': '5432'
    }
    azs_info_loader = AZS_info_loader(db_params, filename_i, coord_file)
    asyncio.run(azs_info_loader.run_insert(parralel_task=10))