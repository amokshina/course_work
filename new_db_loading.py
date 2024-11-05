from pydantic import BaseModel
from typing import List, Optional, AsyncIterator
import json
import csv
import random
import time
import hashlib
from datetime import datetime
import asyncio 
import asyncpg
import aiofiles
import aiocsv
from math import ceil
import logging
import  requests
import os
from abc import ABC, abstractmethod


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

class AZSLoaderTemplate(ABC):
    headers =   [ 
                        {'User-Agent': 'Mozilla/5.0 (Linux; Android 8.1.0; 01618) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Safari/537.36'}
                        ,{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 LikeWise/95.6.3045.46'}
                ]

    def __init__(self, db_params: dict, file_path: str):
        self.db_params = db_params
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.file_size = os.path.getsize(file_path)
        self.file_type = os.path.splitext(self.file_name)[1][1:]  
        self.pars_date = self.get_pars_date(self.file_name)
        
        self.con_pool = None

        with open(file_path, 'r', encoding='utf-8') as file:
            # Читаем первую строку как заголовок
            reader = csv.reader(file)
            first_row = next(reader)  # Это первая строка из файла
            self.columns = [col.strip() for col in first_row]  # Преобразуем в список столбцов, убирая лишние пробелы

    @staticmethod
    def get_pars_date(filename):
        date_str = filename.split('_')[1]
        time_str = filename.split('_')[2].split('.')[0]

        datetime_str = f"{date_str} {time_str}"

        timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H-%M-%S')

        return timestamp
    
    def get_number_of_lines_in_thread(self, threads: int):
        with open(self.file_path, 'r', encoding='utf-8') as f:
            file_reader = csv.DictReader(f, fieldnames=self.columns)
            next(file_reader) # пропуск первой строки с названиями колонок
            file_lines = sum(1 for line in file_reader)
            logging.debug(f"File lines: {file_lines}")
            return ceil(file_lines/threads)

    async def run_insert(self, parallel_task: int = 10):
        """Шаблонный метод для выполнения вставки данных"""
        await self._initialize_db_connection(parallel_task)
        await self._process_file_in_parallel(parallel_task)
        await self.insert_file_info()
        await self.close_functions()

    async def _initialize_db_connection(self, parallel_task: int):
        """Шаг инициализации пула соединений с базой данных"""
        max_conn_pool = min(50, parallel_task)
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

    async def truncate_tables(self, tables: list, schema: str = 'buffer'):
        async with self.con_pool.acquire() as conn:
            for table in tables:
                try:
                    print(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
                    await conn.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE')
                except Exception as e:
                    logging.warning(f"Error with connection or cursor: {e}" )  

    async def _process_file_in_parallel(self, parallel_task: int):
        data_to_insert = self.initialize_data_storage()

        await self.truncate_tables(tables = list(data_to_insert.keys()))

        lines_per_task = self.get_number_of_lines_in_thread(parallel_task)
        tasks = [
            asyncio.create_task(self._process_lines(start, start + lines_per_task, data_to_insert))
            for start in range(0, parallel_task * lines_per_task, lines_per_task)
        ]
        await asyncio.gather(*tasks)


    async def _process_lines(self, start, end, data_to_insert):
        async for line in self.read_file_lines(start, end):
            await self.process_line(line, data_to_insert) 

        await self.load_data_if_needed(data_to_insert)

    @abstractmethod
    def initialize_data_storage(self):
        """Инициализация хранилища данных. Здесь нужно сделать нормальные названия. В точности как у таблиц""" 
        pass

    async def read_file_lines(self, start, end) -> AsyncIterator[str]:
        async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
            file_reader = aiocsv.AsyncDictReader(f, fieldnames=self.columns)
            await f.readline() 
            index = -1
            async for line in file_reader:
                index += 1
                if start<=index<end:
                    logging.debug(f"read_file_lines start:{start}, end:{end}. index:{index}")
                    yield line

    @abstractmethod
    async def process_line(self, line, data_to_insert):
        pass

    async def load_data_if_needed(self, data_to_insert):
        for table_name, values in data_to_insert.items():
            if values:  # Проверяем, есть ли данные для загрузки
                logging.debug(f"Loading data into {table_name}")
                await self.load_data(schema='buffer', table=table_name, values=values)
                values.clear()  # Очистка списка после загрузки (если это необходимо)

    async def load_data(self, schema, table, values):
        attempt = 0
        max_retries = 10
        values_copy = list(values)

        while attempt < max_retries:
            try:
                async with self.con_pool.acquire() as con:
                    async with con.transaction():
                       # await con.executemany(query, values)
                        await con.copy_records_to_table(
                            table,
                            schema_name = schema,
                            records = values_copy
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

    async def insert_file_info(self):
        """Метод для вставки информации о файле в БД"""
        async with self.con_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO ctr.reviews_file (file_name, file_path, date_pars, file_size, file_type)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (file_path, file_name) DO NOTHING;
            ''', self.file_name, self.file_path, self.pars_date, self.file_size, self.file_type)
    
    @abstractmethod
    async def close_functions(self):
        pass


class AZSInfoLoader(AZSLoaderTemplate):
    def __init__(self, db_params: dict, file_path: str, coordinates_file: str):
        super().__init__(db_params, file_path)

        self.coordinates_file = coordinates_file
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
        with open (self.coordinates_file, 'w', encoding='utf-8') as f:  
            f.write(json.dumps(self.dict_coord, ensure_ascii=False))

    async def close_functions(self):
        self.coord_to_file()
        try: 
            async with self.con_pool.acquire() as con: 
                async with con.transaction():
                    await con.execute("select testing.fn_load_info()")
                    logging.info('testing.fn_load_info() called')
        except Exception as e:
            logging.warning(f"Error with call func: {e}") 

    def initialize_data_storage(self):
        """Инициализация хранилища данных для вставки в БД."""
        return {
            'azs_info': [],  # Хранилище для информации об АЗС
            's_azs_address': []  # Хранилище для адресов
        }

    async def process_line(self, line, data_to_insert):
        """Обработка строки файла и добавление данных в хранилище."""
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
                    date_of_pars = self.pars_date,
                    filename_with_ts = self.file_name
                    )
                
                s_azs_address = s_AZS_address(
                    coordinates = hash_coord,
                    address = json.dumps(place, ensure_ascii=False),
                    filename_with_ts = self.file_name
                )

                data_to_insert['azs_info'].append((azs_info.objectId, azs_info.title, azs_info.rating, azs_info.reviewsCount, azs_info.hoursShortStatus, 
                                                                    azs_info.address, azs_info.region, azs_info.coordinates, azs_info.date_of_pars, azs_info.filename_with_ts))
                    
                data_to_insert['s_azs_address'].append((s_azs_address.coordinates, s_azs_address.address, s_azs_address.filename_with_ts))

                if len(data_to_insert['s_azs_address']) > 1000:
                    await self.load_data(schema='buffer', table='s_azs_address', values=data_to_insert['s_azs_address'])
                
                if len(data_to_insert['azs_info']) > 1000:
                    await self.load_data(schema='buffer', table='azs_info', values=data_to_insert['azs_info'])

            elif country is None: logging.info(f"COUNTRY IS NONE. {json_data}")

        except json.JSONDecodeError as e:
            logging.warning(f"Ошибка при декодировании JSON: {e}")
        
        except Exception as ex:
            raise

class AZSReviewLoader(AZSLoaderTemplate):
    def initialize_data_storage(self):
        """Инициализация хранилища данных для вставки в БД."""
        return {
            'azs_review': [],
            's_azs_rev_users': [],
            's_azs_categ': set(), 
            'azs_rev_categ': []
        }

    async def process_line(self, line, data_to_insert):
        """Обработка строки файла и добавление данных в хранилище."""
        obj_id = line.get('objectId')
        logging.debug(f"processing line {obj_id}")
        json_reviews = line.get('reviews')
        json_reviews = json.loads(json_reviews)  # получили список jsonов который  надо записать в _AZS_review

        for rev in json_reviews:
            if rev.get('text'):
                time = rev.get('time') / 1000 # 1687022256564 - в миллисекундах
                timestamp = datetime.fromtimestamp(time)
                try:
                    azs_review = AZS_review (
                        object_id = obj_id ,
                        comment_type = rev.get('type'),
                        comment_time = timestamp,   
                        author_id = rev.get('author').get('publicId') if rev.get('author').get('publicId') else '0000',
                        profession_level_num = rev.get('author').get('professionLevelNum'),
                        rating = rev.get('rating').get('val', None) if rev.get('rating') else None,
                        comment_text = rev.get('text'),
                        likes_num = rev.get('reactions').get('likesCount'),
                        dislikes_num = rev.get('reactions').get('dislikesCount'),
                        date_of_pars = self.pars_date,
                        file_name_with_ts = self.file_name
                    )
                    data_to_insert['azs_review'].append((azs_review.object_id, azs_review.comment_type, azs_review.comment_time, azs_review.author_id, azs_review.profession_level_num,
                                                        azs_review.rating, azs_review.comment_text, azs_review.likes_num, azs_review.dislikes_num, azs_review.date_of_pars, azs_review.file_name_with_ts))
                    if azs_review.author_id != '0000':
                        s_azs_rev_user = s_AZS_rev_users(
                            author_id = rev.get('author').get('publicId'),
                            author_name = rev.get('author').get('name'),
                            verified = rev.get('author').get('verified'),
                            file_name_with_ts = self.file_name
                        )
                        data_to_insert['s_azs_rev_users'].append((s_azs_rev_user.author_id, s_azs_rev_user.author_name, s_azs_rev_user.verified, s_azs_rev_user.file_name_with_ts))

                except Exception as e:
                    logging.warning(e, rev)  

        if len(data_to_insert['azs_review']) > 1000:
            await self.load_data(schema='buffer', table='azs_review', values=data_to_insert['azs_review'])
            
        if len(data_to_insert['s_azs_rev_users']) > 1000:
            await self.load_data(schema='buffer', table='s_azs_rev_users', values=data_to_insert['s_azs_rev_users'])
      

        json_category_stats = line.get('categoryAspectsStats')  # получили список jsonов которые надо записать в s_AZS_categ и AZS_rev_categ 
        if json_category_stats != 'null':  
            json_category_stats = json.loads(json_category_stats)
            for cat in json_category_stats:
                azs_rev_categ = AZS_rev_categ( 
                    object_id = obj_id,
                    categ_id = cat.get('id'),
                    reviews_num = cat.get('reviewsCount'),
                    pos_rev_num = cat.get('positiveReviewsCount'),
                    neg_rev_num = cat.get('negativeReviewsCount'),
                    date_of_pars = self.pars_date,
                    file_name_with_ts = self.file_name
                    )
                
                s_azs_categ = s_AZS_categ(
                    categ_id = cat.get('id'),
                    title = cat.get('key'),
                    file_name_with_ts = self.file_name
                )
                
                data_to_insert['azs_rev_categ'].append((azs_rev_categ.object_id, azs_rev_categ.categ_id, azs_rev_categ.reviews_num, azs_rev_categ.pos_rev_num, azs_rev_categ.neg_rev_num,
                                                        azs_rev_categ.date_of_pars, azs_rev_categ.file_name_with_ts))

                data_to_insert['s_azs_categ'].add((s_azs_categ.categ_id, s_azs_categ.title, s_azs_categ.file_name_with_ts)) 

        if len(data_to_insert['azs_rev_categ']) > 1000:
            await self.load_data(schema='buffer', table='azs_rev_categ', values=data_to_insert['azs_rev_categ'])               
        
    async def close_functions(self):
        try: 
            async with self.con_pool.acquire() as con: 
                async with con.transaction():
                    await con.execute("select testing.fn_load_reviews()")
                    print('testing.fn_load_reviews() called')
        except Exception as e:
            logging.warning(f"Error with call func: {e}") 

async def main():
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
    # azs_info_loader = AZSInfoLoader(db_params=db_params, file_path=filename_i, coordinates_file=coord_file)
    # await azs_info_loader.run_insert(parallel_task=10)

    azs_review_loader = AZSReviewLoader(db_params=db_params, file_path=filename_r)
    await azs_review_loader.run_insert(parallel_task=10)

if __name__ == '__main__':
    asyncio.run(main())
    