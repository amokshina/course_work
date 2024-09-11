import toml
import json 
import requests
from itertools import product
import random
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import csv
import numpy as np


class AZSinfo_parcer():
    def __init__(self, config_path, outfile_path = '', ids_path = None): 
        config = toml.load(config_path)
        self.base_url = json.loads(config['url']['url_info']).get('url')
        self.headers = config['user_agents']['headers']
        self.start_lat = config['coordinates']['start_lat']  
        self.start_lon =  config['coordinates']['start_lon']  
        self.end_lat =  config['coordinates']['end_lat']   
        self.end_lon =  config['coordinates']['end_lon']
        self.step_lat =  config['coordinates']['step_lat']   
        self.step_lon =  config['coordinates']['step_lon'] 

        self.config_path = config_path
        self.file_path = outfile_path

        self.dict_ids = {} # создаем dict со всеми существующими id
        if ids_path:
            self.ids_path = ids_path
            with open(ids_path, 'r', encoding='utf-8') as ids_f:
                self.dict_ids = json.load(ids_f) 
        else: 
            self.ids_path = self.file_path + datetime.now().strftime("objectIds_%Y-%m-%d_%H-%M.json") # если параметр не был передан создаем файл со всеми айди


    def generate_coordinates(self):
        # Формируем список всех комбинаций значений lat и lon
        latitudes = np.arange(self.start_lat, self.end_lat + self.step_lat, self.step_lat)
        longitudes = np.arange(self.start_lon, self.end_lon + self.step_lon, self.step_lon)
        coordinates = list(product(latitudes, longitudes))
        return coordinates

    # Функция для генерации URL с заданными параметрами
    def generate_url(self, min_lon, max_lat, max_lon, min_lat, page_num):
        parsed_url = urlparse(self.base_url)
        query_params = parse_qs(parsed_url.query) 

        query_params['bbox'] = [f"{min_lon},{max_lat}~{max_lon},{min_lat}"] # координаты видимой области на карте
        query_params['geo_page'] = [str(page_num)] # номер страницы 

        new_query_string = urlencode(query_params, doseq=True) 
        new_url = urlunparse(parsed_url._replace(query=new_query_string))
        return new_url

    def generate_outfile_name(self):
        # Генерация имени файла с текущей датой и временем
        out_file = self.file_path + datetime.now().strftime("info_%Y-%m-%d_%H-%M-%S.csv")
        return out_file

    def load_to_file(parcer, out_file): 
        coordinates = parcer.generate_coordinates()
        itr = ((lat, lon) for lat, lon in coordinates) # итератор для координат

        max_attempts = 100 # количество попыток для доступа к url 
        count_repeat = 100 
        
        coord = next(itr)
        lat = coord[0]
        lon = coord[1]
        min_lat, max_lat = str(lat), str(lat + parcer.step_lat)
        min_lon, max_lon = str(lon), str(lon + parcer.step_lon)
        
        page = -1
  
        with open(out_file, 'w', encoding='utf-8', newline='') as out_f:
            columns = ['objectId', 'info']
            csv_writer = csv.DictWriter(out_f, fieldnames=columns, quoting=csv.QUOTE_MINIMAL) 
            csv_writer.writeheader()
            while True:
                try:
                    while True:
                        page += 1
                        attempt = 1

                        while attempt <= max_attempts:
                            try: 
                                ran_head = parcer.headers[random.randrange(0, len(parcer.headers), 1)] # выбор рандомного user agent
                                ran_head = {'User-Agent': ran_head}
                                url = parcer.generate_url(min_lon, max_lat, max_lon, min_lat, page)

                                response = requests.get(url, headers = ran_head)

                                response.raise_for_status() # вызовет http error

                                if response.status_code != 200:
                                    print(f"Ошибка запроса: {response.status_code} для URL: {url}")
                                    attempt += 1
                                    continue

                                data = response.json()
 
                                items = data.get('ajax-updater', {}).get('params', {}).get('items', False) # получение всех азс доступных на данной странице

                                if items:
                                    for elem in items:
                                        objectId = elem['oid'] # формируем ключ
                                        
                                        elem.pop('pinIcon', None)
                                        rec = ({'objectId' : objectId, 'info': json.dumps(elem, ensure_ascii=False)}) 
                                        csv_writer.writerow(rec) # запись в основной файл
                                        
                                        if objectId not in parcer.dict_ids:                                                
                                            # Добавление id в список всех id
                                            parcer.dict_ids.update({objectId: elem.get('reviewsCount', 0)})     
                                break # заканчиваем перебор попыток
                                                           
                            except requests.exceptions.HTTPError as err:
                                if response.status_code == 500: 
                                    print(f"Id: {objectId}. Попытка {attempt}: Ошибка 500. Повторная попытка через 1 секунду...") 
                                    attempt += 1
                                    time.sleep(1)
                                else: 
                                    print(f"Ошибка запроса: {err}") 
                                    break 
                            except requests.exceptions.RequestException as e:
                                print(f"Ошибка запроса: {e}") 
                                break

                        if attempt > max_attempts:
                            print(f"Максимальное количество попыток для {url}")    
                            break # заканчиваем цикл while True: (заканчиваем разбор для конкретной координаты)                    
                        elif not items: break # заканчиваем цикл while True: (заканчиваем разбор для конкретной координаты)                                                                            
                    
                    coord = next(itr)
                    page = -1
                    lat = coord[0]
                    lon = coord[1]
                    min_lat, max_lat = str(lat), str(lat + parcer.step_lat)
                    min_lon, max_lon = str(lon), str(lon + parcer.step_lon)                   

                except StopIteration:
                    print("stop iterator")
                    # когда были пройдены все элементы, обновляем имя последнего файла (будет использоваться при парсинге отзывов) в config
                    config = toml.load(parcer.config_path)
                    config['filename']['in_file'] = parcer.ids_path
                    with open (parcer.config_path, 'w', encoding='utf-8') as file:
                        toml.dump(config, file)
                    break    

                except Exception as ex:
                    if count_repeat > 0:
                        count_repeat -=1
                        print('Error!', ex)
                        continue
                    else:
                        print('count_repeat = 0')
                        break

        with open (parcer.ids_path, 'w', encoding='utf-8') as ids_f:  
            ids_f.write(json.dumps(parcer.dict_ids, ensure_ascii=False))
                
def main():
    parser = AZSinfo_parcer('config.toml')         #   c нуля -> формируем новую базу айди
    # parser = AZSinfo_parcer('config.toml', ids_path='objectIds_2024-07-09_17-16.json')         # с имеющейся базой айди                    
    a = parser.generate_outfile_name()
    AZSinfo_parcer.load_to_file(parser, a)

if __name__ == '__main__':
    main()