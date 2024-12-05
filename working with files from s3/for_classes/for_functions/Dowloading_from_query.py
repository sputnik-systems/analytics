import requests
import time

class Dowloading_from_query:
    def __init__ (self):
        self.headers = None
        self.FOLDER_ID = None
        self.query_name = None
        self.query_text = None
        self.TEMP_FILENAME = None
        self.query_description = None
    
    def get_query_parameters(self,parameters):
        self.headers = parameters['headers']
        self.FOLDER_ID = parameters['FOLDER_ID']
        self.query_name = parameters['query_name']
        self.query_text = parameters['query_text']
        self.TEMP_FILENAME = parameters['TEMP_FILENAME']
        self.query_description = parameters['query_description']
        
    def create_query(self): #функция создает новый запрос и возвращает id для запроса результата
        body = {
            "name":self.query_name, 
            "TYPE":"ANALYTICS", 
            "text":self.query_text, 
            "description":self.query_description
        }
        response = requests.post(
            f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries?project={self.FOLDER_ID}',
            headers=self.headers,
            json=body
        )
        if response.status_code == 200:
            return response.json()["id"]
        return f' Code: {response},  text: {response.text}'
    
    def get_request(self,offset): # фунция возвращает ответ запроса. Максимум 1000 строк.
        offset = offset
        get_query_results_url = f'https://api.yandex-query.cloud.yandex.net/api/fq/v1/queries/{self.request_id}/results/0?project={self.FOLDER_ID}&offset={str(offset)}&limit=1000'
        response = requests.get(
            get_query_results_url,
            headers = self.headers
        )
        return response
    
    def if_cell_is_list(self,cell, type_of_empty_values = None): # функция участвует в преобразовании данных при создании файла
        if isinstance(cell, list):
            if len(cell) == 0:
                return type_of_empty_values
            else: 
                return cell[0]
        else:
            return cell
    
    def write_temp_file(self, type_of_empty_values = None):
        self.request_id = self.create_query()
        while str(self.get_request(0)) == '<Response [400]>':
            time.sleep(10)

        offset = 0
        response = self.get_request(offset) #запрашиваем данные запроса
        print(response)
        columns = [rows['name'] for rows in response.json()['columns']] #выделяем названия столбцов
        special_str = ""
        for j in columns:
            special_str = f"{special_str}{str(j)},"
        temp_file = open(self.TEMP_FILENAME, 'w', encoding='utf-8')
        temp_file.write(special_str[:-1]+'\n')

        while response.status_code == 200 and len(response.json()['rows']) != 0:  #Цикл делает запросы по 10000, пока не кончатся данные
            response = self.get_request(offset)
            response_rows = response.json()['rows']
            rows = [[self.if_cell_is_list(cell,type_of_empty_values) for cell in row] for row in response_rows]  #Преобразуются строки
            # Открывает созданный файл и добавляет в него строки
            for row in rows:
                special_str = ''
                for word in row:
                    if isinstance(word, type(None)):
                        special_str += ","
                    if isinstance(word, str):
                        special_str += "'{0}',".format(word.replace("'", ""))
                    else:
                        special_str += "{0},".format(word)
                temp_file.write(special_str[:-1]+'\n')  
            offset +=1000 # увеличивает смещение

    