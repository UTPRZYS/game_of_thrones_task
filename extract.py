import requests
import json
import pandas as pd

def get_books():
    pagesize = 2
    pagenumber=1

    urlform = r'https://www.anapioficeandfire.com/api/books?page={pagenumber}&pageSize={pagesize}'
    print(urlform.format(pagenumber=pagenumber, pagesize=pagesize))

    session = requests.Session()
    session.verify = False

    response = session.get(urlform.format(pagenumber=pagenumber, pagesize=pagesize))
    print('Status code', response.status_code)
    data_json = response.json()

    #with open(os.join('files',f'books_{pagenumber}_{pagesize}'),'w', encoding='utf-8') as fh:
    #    fh.write(data_json)

    return data_json

def process_books(books):
    df = pd.json_normalize(data = books)
    print(df.head())
    df.to_csv('books_structure.csv', index=False)

if __name__ == '__main__':
    #books = get_books()
    #process_books(books)
