from flask import Flask
from argparse import ArgumentParser
from sys import exit
from os import path, makedirs
from operations import CreateData, ReadData, DeleteData

secret_key='s845db8485cd93sfe545j546jkdvvnvwe691djsnw'
default_path='db'
default_name='db'

#for command line
parser = ArgumentParser()
parser.add_argument('--datastore')
args = parser.parse_args()
if args.datastore:
    db_path = args.datastore
else:
    db_path = default_path
try:
    directory_created = makedirs(db_path, mode=0o777, exist_ok=True)
except:
    print("some error occured in creating directory")
    exit(0)

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = secret_key

# API Endpoints
app.add_url_rule('/datastore/create', view_func=CreateData.as_view('create', db_path), methods=['POST'])
app.add_url_rule('/datastore/read', view_func=ReadData.as_view('read', db_path), methods=['GET'])
app.add_url_rule('/datastore/delete', view_func=DeleteData.as_view('delete', db_path), methods=['DELETE'])

if __name__ == '__main__':
    app.run(host='localhost', port=5000)


