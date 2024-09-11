import toml
import json
import requests
import random
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import csv

import pars_azs_new_api

class AZSreviews_parcer():
    def __init__(self, config_path, ids_path = None, outfile_path = ''): 
        config = toml.load(config_path)
        self.base_url = json.loads(config['url']['url_reviews']).get('url')
        self.in_file = config['filename']['in_file']
        self.headers = config['user_agents']['headers']

        self.config_path = config_path
        self.file_path = outfile_path
        self.dict_ids = {} # создаем dict со всеми существующими id
        if ids_path:
            self.ids_path = ids_path
        else: 
            self.ids_path = self.in_file

        with open(self.ids_path, 'r', encoding='utf-8') as ids_f:
            self.dict_ids = json.load(ids_f) 

    # Функция для генерации URL с заданными параметрами
    def generate_url(self, objectId, page_num, limit=10):
        parsed_url = urlparse(self.base_url) 
        query_params = parse_qs(parsed_url.query) 

        query_params['offset'] = [page_num*limit]
        query_params['objectId'] = [f"/org/{objectId}"]
        query_params['limit'] = [limit]

        new_query_string = urlencode(query_params, doseq=True) # преобразование словаря в строку запросов
        new_url = urlunparse(parsed_url._replace(query=new_query_string))
        return new_url

    def generate_outfile_name(self):
        out_file = self.file_path + datetime.now().strftime("reviews_%Y-%m-%d_%H-%M-%S.csv")
        return out_file
   
    def load_to_file(parcer, out_file):
        max_attempts = 100
        
        with open(out_file, 'w', encoding='utf-8', newline='') as out_f:
            columns = ['objectId', 'reviews', 'categoryAspectsStats', 'pager']
            csv_writer = csv.DictWriter(out_f, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            for objectId in parcer.dict_ids.keys():
                page = -1

                rec = { 'objectId' : objectId, 
                        'reviews': None, 
                        'categoryAspectsStats': None, 
                        'pager': None}
                reviews = []

                cnt_flag = True

                while True:
                    page += 1
                    if page >= 100: break # смещение больше 1000 не позволяет api
                    url = parcer.generate_url(objectId, page)
                    ran_head = parcer.headers[random.randrange(0, len(parcer.headers), 1)]                               
                    ran_head = {'User-Agent': ran_head}
                    attempt = 1
                       

                    while attempt <= max_attempts:
                        try: 
                            response = requests.get(url=url, headers = ran_head)                            
                            response.raise_for_status() # вызовет http error

                            if response.status_code != 200:
                                print(f"Ошибка запроса: {response.status_code} для URL: {url}")
                                attempt += 1
                                continue

                            json_data = response.json()

                            views = json_data.get('view', {}).get('views', None)

                            if views:
                                if page == 0:
                                    pag = json_data.get('pager', {})
                                    rev_c = int(parcer.dict_ids.get(objectId))
                                    if pag.get('reviewCount') != rev_c: parcer.dict_ids.update({objectId: pag.get('reviewCount')}) # в осн таблице бывают пропуски
                                    if pag.get('realCount') == 0:
                                        cnt_flag = False
                                        break
                                    rec.update({'pager' : json.dumps(pag, ensure_ascii=False)}) 
                                    catAsSt= json_data.get('categoryAspectStats', None)
                                    rec.update({'categoryAspectsStats' : json.dumps(catAsSt, ensure_ascii=False)}) 
                                    
                                for elem in views:
                                    reviews.append(elem) 
                            break
                        
                        except requests.exceptions.HTTPError as err:
                            if response.status_code == 500: 
                                print(f"Id: {objectId}. Попытка {attempt}: Ошибка 500. Повторная попытка через 1 секунду...") 
                                attempt += 1
                                time.sleep(1)
                            else: 
                                print(f"Ошибка запроса: {err}") 
                                break 
                        except requests.exceptions.RequestException as e:
                            print(f"Ошибка запроса: {e} Код: {response.status_code}") 
                            print(url)
                            break

                    if attempt > max_attempts:
                        print(f"Максимальное количество попыток для {objectId}")    
                    elif not views: break
                
                if cnt_flag:
                    rec.update({'reviews' : json.dumps(reviews, ensure_ascii=False)})
                    csv_writer.writerow(rec)
                    print('write')
                
        
        with open (parcer.ids_path, 'w', encoding='utf-8') as ids_f:  
            ids_f.write(json.dumps(parcer.dict_ids, ensure_ascii=False))

def main():
    parser = AZSreviews_parcer('config.toml')                             
    a = parser.generate_outfile_name()
    AZSreviews_parcer.load_to_file(parser, a)

if __name__ == '__main__':
    main()