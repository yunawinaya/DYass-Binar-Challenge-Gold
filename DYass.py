# Import library for ReGex, SQLite, and Pandas
import re
import sqlite3
import pandas as pd

# Import library for Flask
from flask import Flask, jsonify
from flask import request, make_response
from flask_swagger_ui import get_swaggerui_blueprint

# Swagger UI Definition
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False # Mengubah order JSON menjadi urutan yang benar
SWAGGER_URL = '/swagger'
API_URL = '/static/restapi.yml'
SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "DYass (Data Yassification)"
    }
)
app.register_blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)

# Connect to db & csv
conn = sqlite3.connect('data/output.db', check_same_thread=False)
df_alay = pd.read_csv('data/new_kamusalay.csv', names=['alay','cleaned'], encoding ='latin-1')
df_abussive = pd.read_csv('data/abusive.csv', encoding ='latin-1')

# Define and Execute query for unexistence data tables
# Tables will contain fields with dirty text (text & file) and cleaned text (text & file)
conn.execute('''CREATE TABLE IF NOT EXISTS data_text (text_id INTEGER PRIMARY KEY AUTOINCREMENT, dirty_text varchar(255), clean_text varchar(255));''')
conn.execute('''CREATE TABLE IF NOT EXISTS data_file (text_id INTEGER PRIMARY KEY AUTOINCREMENT, dirty_text varchar(255), clean_text varchar(255));''')
   
# Creating Function for Cleansing Process
def yass_lowercase(text): #Change uppercase characters to lowercase
    return text.lower()

def yass_misc(text): # Cleaning URL, Mention, Hashtag, user, x??, Line, and Tab
    text = re.sub(r'((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))|([#@]\S+)|user|x.{2}|\n|\t',' ',text)
    return text

def yass_symbol(text):
    text = re.sub(r'[^0-9a-zA-Z]+', ' ',text)
    return text

alay_mapping = dict(zip(df_alay['alay'], df_alay['cleaned'])) # Mapping for kamusalay
def yass_alay(text): # Cleaning by replacing 'alay' words
    wordlist = text.split()
    text_alay = [alay_mapping.get(x,x) for x in wordlist]
    clean_alay = ' '.join(text_alay)
    return clean_alay

abusive_mapping = set(df_abussive['ABUSIVE']) # Mapping for abbusive
def yass_abusive(text): #Cleaning by removing abbusive words
    wordlist = text.split()
    text_abusive = [x for x in wordlist if x not in abusive_mapping]
    clean_abusive = ' '.join(text_abusive)
    return clean_abusive

# Function for text cleansing
def yassification(text):
    text = yass_lowercase(text)
    text = yass_misc(text)
    text = yass_symbol(text)
    text = yass_alay(text)
    text = yass_abusive(text)
    return text

# Function for csv cleansing
def yassification_csv(input_file):
    column = input_file.iloc[:, 0]
    print(column)

    for data_file in column: # Define and execute query for insert original text and cleaned text to sqlite database
        data_clean = yassification(data_file)
        query = "insert into data_file (dirty_text,clean_text) values (?, ?)"
        val = (data_file, data_clean)
        conn.execute(query, val)
        conn.commit()
        print(data_file)

# Create Homepage
@app.route('/', methods=['GET'])
def get():
    return "Welcome to DYass!"

# Endpoint for Text Cleansing
# Input text to clean
@app.route('/text_yassification', methods=['POST'])
def text_cleaning():

    # Get text from user
    input_text = str(request.form['text'])

    # Cleaning text
    output_text = yassification(input_text)

    # Define and execute query for insert original text and cleaned text to sqlite database
    conn.execute("INSERT INTO data_text (dirty_text,clean_text) VALUES ('" + input_text + "', '" + output_text + "')")
    conn.commit()

    # Define API response
    json_response = {
        'description': "Yassification Success!",
        'clean text' : output_text,
        'dirty text' : input_text
    }
    response_data = jsonify(json_response)
    return response_data

# Show all texts
@app.route('/show_text', methods=['GET'])
def show_text():
    query_text = "select * from data_text"
    select_text = conn.execute(query_text)
    show_text = [
        dict(text_id=row[0], dirty_text=row[1], clean_text=row[2])
        for row in select_text.fetchall()
    ]
    return jsonify(show_text)

# Endpoint for Text Manipulation
@app.route("/text_yassification/<string:text_id>", methods=["GET","PUT","DELETE"])
def text_id(text_id):

    # Get text by ID
    if request.method == "GET":   
        
        query_text = "select * from data_text where text_id = ?"
        val = str(text_id)
        select_text = conn.execute(query_text, [val])
        show_text_id = [
            dict(text_id=row[0], dirty_text=row[1], clean_text=row[2])
            for row in select_text.fetchall()
        ]
        return jsonify(show_text_id)

    # Edit existing text
    elif request.method == "PUT":
        
        input_text = str(request.form["text"])
        output_text = yassification(input_text)
        query_text = "update data_text set dirty_text = ?, clean_text = ? where text_id = ?"
        val = (input_text, output_text, text_id)
        conn.execute(query_text, val)
        conn.commit()
        return "Success Update Data"

    # Delete existing text
    elif request.method == "DELETE":

        query_text = "delete from data_text where text_id = ?"
        val = text_id
        conn.execute(query_text, [val])
        conn.commit()
        return "Success Delete Data"

# Endpoint for File Cleansing
@app.route('/file_yassification', methods=['POST'])
def file_cleaning():

    # Get file
    file = request.files['file']
    try:
            datacsv = pd.read_csv(file, encoding='iso-8859-1')
    except:
            datacsv = pd.read_csv(file, encoding='utf-8')
    
    # Cleaning file
    yassification_csv(datacsv)

    # Define API response
    select_data = conn.execute("SELECT * FROM data_file")
    conn.commit
    data = [
        dict(text_id=row[0], dirty_text=row[1], clean_text=row[2])
    for row in select_data.fetchall()
    ]
    
    return jsonify(data)

# Endpoint for File Manipulation
@app.route("/file_yassification/<string:text_id>", methods=["GET","PUT","DELETE"])
def file_text_id(text_id):

    # Get text by ID
    if request.method == "GET":   
        
        query_text = "select * from data_file where text_id = ?"
        val = str(text_id)
        select_text = conn.execute(query_text, [val])
        show_text_id = [
            dict(text_id=row[0], dirty_text=row[1], clean_text=row[2])
            for row in select_text.fetchall()
        ]
        return jsonify(show_text_id)

    # Edit existing text
    elif request.method == "PUT":
        
        input_text = str(request.form["text"])
        output_text = yassification(input_text)
        query_text = "update data_file set dirty_text = ?, clean_text = ? where text_id = ?"
        val = (input_text, output_text, text_id)
        conn.execute(query_text, val)
        conn.commit()
        return "Success Update Data"

    # Delete existing text
    elif request.method == "DELETE":

        query_text = "delete from data_file where text_id = ?"
        val = text_id
        conn.execute(query_text, [val])
        conn.commit()
        return "Success Delete Data"


# Error Handling
@app.errorhandler(400)
def handle_400_error(_error):
    "Return a http 400 error to client"
    return make_response(jsonify({'error': 'Misunderstood'}), 400)


@app.errorhandler(401)
def handle_401_error(_error):
    "Return a http 401 error to client"
    return make_response(jsonify({'error': 'Unauthorised'}), 401)


@app.errorhandler(404)
def handle_404_error(_error):
    "Return a http 404 error to client"
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.errorhandler(500)
def handle_500_error(_error):
    "Return a http 500 error to client"
    return make_response(jsonify({'error': 'Server error'}), 500)

# Run Server
if __name__ == '__main__':
   app.run(debug=True)

