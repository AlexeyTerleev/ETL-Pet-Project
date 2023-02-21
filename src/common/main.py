import scraping_from_realtby
import spark_delta

import json


def main():

    lst = scraping_from_realtby.extract()
    with open('../../data/realtby.json', 'w', encoding='utf-8') as file:
        json.dump(lst, file, ensure_ascii=False)

    #spark_delta.json2df()


if __name__ == '__main__':
    main()

'''
запихнуть все в airflow:
    1) Считывание в json
    2) Создание таблицы postgresql
    3) Повторное считывание, обновление данных

Улучшить визуализацию

'''
