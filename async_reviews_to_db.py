import psycopg2
from pydantic import BaseModel
from typing import Optional, AsyncIterator
import json
import csv
from datetime import datetime
import asyncio 
import asyncpg
import aiofiles
import aiocsv
from math import ceil
import logging

logging.basicConfig(level=logging.INFO) 

csv.field_size_limit(10**9)

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
    src_id: int = 713 # убрать

class s_AZS_rev_users(BaseModel):
    author_id: str
    author_name: str
    verified: Optional[bool] = None
    file_name_with_ts: Optional[str] = None
    src_id: int = 713 # убрать

class AZS_rev_categ(BaseModel):
    object_id: int
    categ_id: int
    reviews_num: int
    pos_rev_num: int
    neg_rev_num: int
    date_of_pars: datetime
    file_name_with_ts: Optional[str] = None
    src_id: int = 713 # убрать
    
class s_AZS_categ(BaseModel):
    categ_id: int
    title: str
    file_name_with_ts: Optional[str] = None
    src_id: int = 713 # убрать

class AZS_loader():
    def __init__(self, rev_file = ''): 
        self.rev_file = rev_file

        self.pars_date_r = self.get_pars_date(rev_file)

        self.data_to_insert_s_AZS_categ = set()

        self.con_pool = None

    @staticmethod
    def get_pars_date(filename):
        date_str = filename.split('_')[1]
        time_str = filename.split('_')[2].split('.')[0]

        datetime_str = f"{date_str} {time_str}"

        timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H-%M-%S')

        return timestamp

    def truncate_tables():
        truncate_query = """
            truncate sandbox.copy_buf_AZS_review;
            truncate sandbox.copy_buf_AZS_rev_categ;
            truncate sandbox.copy_buf_s_AZS_categ;
            truncate sandbox.copy_buf_s_AZS_rev_users;     
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
            columns = ['objectId', 'reviews', 'categoryAspectsStats', 'pager'] 
            file_reader = csv.DictReader(f, fieldnames=columns)
            next(file_reader) # пропуск первой строки с названиями колонок
            file_lines = sum(1 for line in file_reader)
            logging.debug(f"File lines: {file_lines}")
            return ceil(file_lines/threads)

    async def read_file_lines(self, start, end) -> AsyncIterator[str]:
        async with aiofiles.open(self.rev_file, 'r', encoding='utf-8') as f:
            columns = ['objectId', 'reviews', 'categoryAspectsStats', 'pager'] 
            file_reader = aiocsv.AsyncDictReader(f, fieldnames=columns)
            await f.readline() # пропуск первой строки с названиями колонок

            index = -1
            async for line in file_reader:
                index += 1
                if start<=index<end:
                    logging.debug(f"read_file_lines start:{start}, end:{end}. index:{index}")
                    yield line

    async def process_line(self, line, data_to_insert_AZS_review, data_to_insert_s_AZS_rev_users, data_to_insert_AZS_rev_categ):
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
                        date_of_pars = self.pars_date_r,
                        file_name_with_ts = self.rev_file
                        # src_id =
                    )
                    data_to_insert_AZS_review.append((azs_review.object_id, azs_review.comment_type, azs_review.comment_time, azs_review.author_id, azs_review.profession_level_num,
                                                        azs_review.rating, azs_review.comment_text, azs_review.likes_num, azs_review.dislikes_num, azs_review.date_of_pars, azs_review.file_name_with_ts,
                                                        azs_review.src_id))
                    if azs_review.author_id != '0000':
                        s_azs_rev_user = s_AZS_rev_users(
                            author_id = rev.get('author').get('publicId'),
                            author_name = rev.get('author').get('name'),
                            verified = rev.get('author').get('verified'),
                            file_name_with_ts = self.rev_file
                            # src_id =
                        )
                        data_to_insert_s_AZS_rev_users.append((s_azs_rev_user.author_id, s_azs_rev_user.author_name, s_azs_rev_user.verified, s_azs_rev_user.file_name_with_ts, s_azs_rev_user.src_id))

                except Exception as e:
                    logging.warning(e, rev)  

        if len(data_to_insert_AZS_review) > 1000:
            await self.load_data(schema='sandbox', table='copy_buf_azs_review', values=data_to_insert_AZS_review)
        if len(data_to_insert_s_AZS_rev_users) > 1000:
            await self.load_data(schema='sandbox', table='copy_buf_s_azs_rev_users', values=data_to_insert_s_AZS_rev_users)
        
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
                    date_of_pars = self.pars_date_r,
                    file_name_with_ts = self.rev_file
                    # src_id =
                    )
                
                s_azs_categ = s_AZS_categ(
                    categ_id = cat.get('id'),
                    title = cat.get('key'),
                    file_name_with_ts = self.rev_file
                    # src_id =
                )
                
                data_to_insert_AZS_rev_categ.append((azs_rev_categ.object_id, azs_rev_categ.categ_id, azs_rev_categ.reviews_num, azs_rev_categ.pos_rev_num, azs_rev_categ.neg_rev_num,
                                                        azs_rev_categ.date_of_pars, azs_rev_categ.file_name_with_ts, azs_rev_categ.src_id))

                self.data_to_insert_s_AZS_categ.add((s_azs_categ.categ_id, s_azs_categ.title, s_azs_categ.file_name_with_ts, s_azs_categ.src_id)) 

        if len(data_to_insert_AZS_rev_categ) > 1000:
            await self.load_data(schema='sandbox', table='copy_buf_azs_rev_categ', values=data_to_insert_AZS_rev_categ)               
        
    async def load_data(self, schema, table, values):
        attempt = 0
        max_retries = 10

        num_columns = len(values[0])

        while attempt < max_retries:
            try:
                async with self.con_pool.acquire() as con:
                    async with con.transaction():
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
        data_to_insert_AZS_review = []
        data_to_insert_s_AZS_rev_users = []
        data_to_insert_AZS_rev_categ  = []
        async for line in self.read_file_lines(start, end):
            await self.process_line(line, data_to_insert_AZS_review, data_to_insert_s_AZS_rev_users, data_to_insert_AZS_rev_categ)
        if data_to_insert_AZS_rev_categ:
            await self.load_data(schema='sandbox', table='copy_buf_azs_rev_categ', values=data_to_insert_AZS_rev_categ) 
        if data_to_insert_AZS_review:
            await self.load_data(schema='sandbox', table='copy_buf_azs_review', values=data_to_insert_AZS_review)
        if data_to_insert_s_AZS_rev_users:
            await self.load_data(schema='sandbox', table='copy_buf_s_azs_rev_users', values=data_to_insert_s_AZS_rev_users)


    async def run_insert(self, parralel_task: int = 10):       
        max_conn_pool = (50 if parralel_task > 50 else parralel_task)
        try:
            self.con_pool = await asyncpg.create_pool(host=''
                                                    # ,port=
                                                    ,user=''
                                                    ,password=''
                                                    ,database=''
                                                    ,min_size=1
                                                    ,max_size=max_conn_pool)
        except Exception as e:
            logging.warning(f"Error creating connection pool: {e}")

        AZS_loader.truncate_tables() ## потом убрать

        lines_per_task = AZS_loader.get_number_of_lines_in_thread(self.rev_file, parralel_task)

        dt_start = datetime.now()
        lst_task = []
        for i in range(parralel_task):
            start = i*lines_per_task
            end = start + lines_per_task
            lst_task.append(asyncio.create_task(self._process_lines(start, end)))

        result = await asyncio.gather(*lst_task, return_exceptions=False)

        await self.load_data(schema='sandbox', table='copy_buf_s_azs_categ', values=list(self.data_to_insert_s_AZS_categ)) 

        try:         # вызов функции sql
            async with self.con_pool.acquire() as con: 
                async with con.transaction():
                    await con.execute("select sandbox.fn_load_copy_testing_reviews()")
                    print('function_call')
        except Exception as e:
            logging.warning(f"Error with call func: {e}")

        dt_end = datetime.now()
        print("---------TIME------------")
        print('dt_start-', dt_start, 'dt_end-', dt_end)

    
if __name__ == '__main__':
    filename_r = "reviews_2024-07-16_17-27-42.csv"
    #filename_r = "reviews_2024-07-22_10-02-57.csv"
    azs_loader = AZS_loader(filename_r)
    asyncio.run(azs_loader.run_insert(parralel_task=10))
