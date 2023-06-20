import os
import pandas as pd
import json
import pyodbc
from sqlalchemy import create_engine, MetaData, Table, text, delete, inspect, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine("mssql+pyodbc://sa:changeme@DESKTOP-RIAC343\\SQLEXPRESS/characters?driver=ODBC+Driver+17+for+SQL+Server")
metadata = MetaData()
charactersTable = Table('characters', metadata, autoload_with = engine)
Session = sessionmaker(bind = engine)
session = Session()
Base = declarative_base()
inspector = inspect(engine)

# ANSI Color Schemes:
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RESET = '\033[0m'


# SELECT * from Characters
def getCharacters():
    query_string = "SELECT charName, charClass, account, password, emuAccount, emuPassword, [server], [location] FROM Characters, characterClasses WHERE characters.classID = characterClasses.classID;"
    query = text(query_string)

    results = session.execute(query).fetchall()
    characters = []
    for character in results:
        dictionary = {
            "charName": character[0],
            "charClass": character[1],
            "account": character[2],
            "password": character[3],
            "emuAccount": character[4],
            "emuPassword": character[5],
            "server": character[6],
            "location": character[7]
        }
        characters.append(dictionary)

    json_data = json.dumps(characters, indent = 1)

    with open('./charactersDB.json', 'w') as file:
        file.write(json_data)

# Convert JSON file into pandas DF
def jsonToPandas(selected_json_file):
    json_file_keys = []
    with open(f'{selected_json_file}', 'r') as file:
        json_data = json.load(file)
        df = pd.DataFrame(json_data)
    print(RED + "Your selected JSON file: " + RESET)
    for column_name, data_type in zip(df.columns, df.dtypes):
        # Need to create a list of the column names for the MAP func
        json_file_keys.append(column_name)
        if pd.api.types.is_string_dtype(data_type):
            print(YELLOW + "KEY: " + RESET + GREEN + f"{column_name}" + ' ' + RESET + YELLOW + "Data Type: " + RESET + GREEN + "string" + RESET)
        elif pd.api.types.is_integer_dtype(data_type):
            print(YELLOW + "KEY: " + RESET + GREEN + f"{column_name}" + ' ' + RESET + YELLOW + "Data Type: " + RESET + GREEN + "int" + RESET)
        else:
            print(YELLOW + "KEY: " + RESET + GREEN + f"{column_name}" + ' ' + RESET + YELLOW + "Data Type: " + RESET + GREEN + f"{data_type}" + RESET)
    return json_file_keys

# Fetch table names
def scanForTableNames():
    table_names = inspector.get_table_names()
    for table in table_names:
        print(str(table))
    return table_names

# Create new table
def createNewTable():

    input_columns = []
    add_more_columns = True
    
    print('')
    table_name = input(YELLOW + 'Enter the desired table name: ' + RESET)
   
    while add_more_columns == True:
        col_name = input(YELLOW + 'Please enter column name: ' + RESET)
        data_type = input(YELLOW + 'Please enter data type:  (str/int)' + RESET)
        input_columns.append({"column_name": col_name, "data_type": data_type})
        exit_loop = input(YELLOW + 'Would you like to add more columns? (y/n)' + RESET).lower()
        if exit_loop == 'n':
            add_more_columns = False

    # Checks the input from a previous question
    
    class NewTable(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key = True)
        columns = input_columns
    
    for column in NewTable.columns:
        if column['data_type'] == 'str':
            data_type = Column(String(255))
        else:
            data_type = Column(Integer())
        column_name = column['column_name']
        setattr(NewTable, column_name, data_type)

    Base.metadata.create_all(engine)
        
# Scan dir for JSON files
def scanForJSONFiles():
    print('')
    for filename in os.listdir('./'):
        if os.path.isfile(os.path.join('./', filename)) and filename.endswith('.json'):
            print(filename)

# Select a JSON file
def selectJSONFile():
    # can we parse the selected JSON file, and return {file_name: file_name, len: len} ?
    is_file_valid = False
    while is_file_valid == False:
        user_input = input(YELLOW + 'Please select a JSON file: ' + RESET)
        if os.path.isfile(user_input):
            is_file_valid = True
            return user_input
        else:
            print(YELLOW  + "Invalid input, please try again" + RESET)

# Select a Table
def selectTable():
    table_names = scanForTableNames()
    is_file_valid = False
    while is_file_valid == False:
        user_input = input(YELLOW + 'Please select a Table: ' + RESET)
        if user_input in table_names:
            is_file_valid = True
            return user_input
        else:
            print(RED + "Invalid input, please try again")
        
# Display column names and data types from selected table
def columnNamesAndDataTypes(selected_table):
    column_names_and_data_types = {}
    column_names_list = []
    selected_table_columns = inspector.get_columns(selected_table)
    print(RED + 'Column names and data types from selected SQL table: ' + RESET)
    # Iterate over the columns + data types and print them on the screen:
    # Need to create a list of the column names to check against a list of the key names:

    for column in selected_table_columns:
        column_name = column['name']
        column_data_type = str(column['type'])
        column_names_list.append(str(column['name']))
        if column_data_type.startswith('VARCHAR'):
            column_data_type = 'varChar(255)'

        print(YELLOW + f" Column name: " + RESET + GREEN + f"{column_name}" + RESET + " " + YELLOW + "Data Type: " + RESET + GREEN + f"{column_data_type}" + RESET)
        column_names_and_data_types[column_name] = column_data_type

    print('')
    return column_names_and_data_types
        
# Map your KEYS to the columns in the selected table:
def mapJSONtoTable(json_file_keys, column_names_and_data_types):
    key_orders = []
    json_file_keys_slice = list(json_file_keys) 
    map_while_loop = True
    outer_loop_break = False
    
    while map_while_loop == True:
        for column in column_names_and_data_types:
        # Skip the ID column. I cannot figure out how to create a table without it
            if column == 'id':
                continue

            is_valid_input = False
            while is_valid_input == False:

                print('')
                print(RED + 'Remaining KEYS from your file to map:  ' + RESET)
                print(YELLOW + str(json_file_keys_slice) + RESET)

                key = input(RED + f"Select which KEY from YOUR FILE to map to" + RESET + " " + GREEN + f"{column},"  + RESET + RED + "that has data type" + RESET + " " + GREEN + f"{column_names_and_data_types[column]}: " + RESET)
                # Test if the user entered column is valid:
                if key in json_file_keys_slice:
                    is_valid_input = True
                    key_orders.append(key)
                    json_file_keys_slice.remove(key)
                    
                    if len(json_file_keys_slice) == 0:
                        map_while_loop = False
                        outer_loop_break = True
                        break
                else:
                    print('')
                    print(RED + 'Invalid input, please try again: ' + RESET)
            if outer_loop_break:
                break

        if outer_loop_break == False:
            break
        
    return key_orders

def insertJSONtoSQL(json_file_keys, selected_table, selected_json_file):
    
    # Iterate over the 'json_file_keys' and concatenate an INSERT string
    # Read the 'selected_json_file'
    with open(selected_json_file) as file: # open an array of dicts
        # parse the file from string into JSON
        data = json.load(file)
        for element in data: # for each element / dict:
            query_string_values = ''
            for index, key in enumerate(json_file_keys):
                
                if index == 0:
                    if element[key] == '':
                        query_string_values = query_string_values + "'NULL'"
                        continue
                    query_string_values = query_string_values + f"'{element[key]}'"
                else:
                    if element[key] == '':
                        query_string_values = query_string_values + ", 'NULL'"
                        continue
                    string = element[key].replace("'", "''")
                    query_string_values = query_string_values + f", '{string}'"
            
            query_string = f"INSERT INTO {selected_table} VALUES ({query_string_values})"
            query = text(query_string)
            session.execute(query)
            session.commit()
            
# Start Prompt:
def startPrompt():
    # Print the local JSON files for the user:
    scanForJSONFiles()
    print('')
    selected_json_file = selectJSONFile()
    
    yes_or_no = input(YELLOW + 'Would you like to create a new table? (y/n)' + RESET).lower()
    if yes_or_no == 'y':
        createNewTable()
        # there is a nested func inside selectedTable, should probably remove the nested func, call it separatly, and pass the return val as a param
        selected_table = selectTable()
    else:
        print('')
        selected_table = selectTable()

    

    print('')
    # Print the column names and data types to the user:
    column_names_and_data_types = columnNamesAndDataTypes(selected_table)
    print('')
    # Now, iterate over the JSON file and display the column names (I wonder if data type can be detected)
    json_file_keys = jsonToPandas(selected_json_file)
    if len(json_file_keys) != len(column_names_and_data_types) - 1:
        print(RED + 'ERROR, keys and columns number doesnt match' +  RESET)
        print(RED + 'Will not be able to perform SQL insert. Aborting...' + RESET)
        return
    # must match the column lengths (could store a dicts that contain the lengths)
    mapJSONtoTable(json_file_keys, column_names_and_data_types)
    
    insertJSONtoSQL(json_file_keys, selected_table, selected_json_file)
    

    
   
startPrompt()


    

    