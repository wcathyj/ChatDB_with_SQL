
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, delete, inspect
import random
import string
import os
import tkinter as tk
from tkinter import filedialog
import urllib.parse
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from q4_without_test import get_sql_database_info, generate_sql_query


#%%
# MySQL connection details
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = urllib.parse.quote_plus('f7h4DE2F@$')
MYSQL_DB = 'mydata'
MYSQL_PORT = 3306

# Create the SQLAlchemy engine with pymysql
global engine
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

# Simulated in-memory "database"
database = {}


#%%
## test the connection to the database

def create_new_database():
    """Create a new database with user input name"""
    try:
        # Get database name from user
        db_name = input("Enter new database name: ").strip()
        
        # Basic validation
        if not db_name or ' ' in db_name:
            print("Invalid database name. Name should not be empty or contain spaces.")
            return
        
        # Create database
        engine_temp = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/mysql")
        with engine_temp.connect() as conn:
            conn.execute("commit")  # Commit any open transactions
            # Check if database exists
            result = conn.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'")
            if result.fetchone():
                print(f"Database '{db_name}' already exists!")
                return
            
            # Create new database
            conn.execute(f"CREATE DATABASE {db_name}")
            print(f"Database '{db_name}' created successfully!")
            
            # Update global engine to use new database
            global engine
            engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}")
            
    except Exception as e:
        print(f"Error creating database: {str(e)}")
  
    
    
def delete_database():
    """Delete an existing database with user confirmation"""
    try:
        # Connect and get user databases
        engine_no_db = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/")
        with engine_no_db.connect() as conn:
            # Get user databases (excluding system ones)
            system_dbs = ['information_schema', 'mysql', 'performance_schema', 'sys']
            result = conn.execute("SHOW DATABASES")
            databases = [row[0] for row in result if row[0] not in system_dbs]
            
            if not databases:
                print("No user databases found.")
                return
            
            # Show databases
            print("\nAvailable databases:")
            for i, db in enumerate(databases, 1):
                print(f"{i}. {db}")
            
            # Get user choice
            choice = input("\nEnter database number to delete (or 'cancel'): ").strip()
            if choice.lower() == 'cancel':
                print("Operation cancelled.")
                return
                
            try:
                db_to_delete = databases[int(choice)-1]
                if input(f"\nConfirm delete '{db_to_delete}'? (yes/no): ").lower() == 'yes':
                    conn.execute(f"DROP DATABASE {db_to_delete}")
                    print(f"\nDatabase '{db_to_delete}' deleted successfully!")
                else:
                    print("Deletion cancelled.")
            except (ValueError, IndexError):
                print("Invalid selection.")
                
    except Exception as e:
        print(f"Error: {str(e)}")    
    
    
    
def connect_to_database():
    """Simulate connecting to the 'database' by displaying its current contents."""
    if database:
        print("Bot: Connected to the in-memory database. Here are the users:")
        for user_id, user_data in database.items():
            print(f"User {user_id}: {user_data['name']}, Age {user_data['age']}")
    else:
        print("Bot: No in-memory database found. Please create the sample database first.")

def create_mysql_table():
    """Create a sample table in MySQL database."""
    try:
        metadata = MetaData()
        sample_table = Table('sample_table', metadata,
                             Column('id', Integer, primary_key=True),
                             Column('name', String(50)),
                             Column('age', Integer),
                             Column('email', String(100)))
        metadata.create_all(engine)
        print("Bot: MySQL table 'sample_table' created successfully!")
    except Exception as e:
        print(f"Bot: Error creating MySQL table: {str(e)}")

def insert_sample_data():
    """Insert sample data into MySQL table."""
    data = {
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Mike'],
        'age': [28, 34, 29],
        'email': ['john@example.com', 'jane@example.com', 'mike@example.com']
    }
    df = pd.DataFrame(data)
    
    try:
        df.to_sql('sample_table', engine, if_exists='replace', index=False)
        print("Bot: Sample data inserted into MySQL database successfully!")
    except Exception as e:
        print(f"Bot: Error inserting data into MySQL: {str(e)}")

def delete_all_mysql_data():
    """Delete all data from the MySQL table."""
    try:
        metadata = MetaData()
        sample_table = Table('sample_table', metadata, autoload_with=engine)
        
        with engine.begin() as connection:
            delete_statement = delete(sample_table)
            connection.execute(delete_statement)
        
        print("Bot: All data has been deleted from the MySQL table 'sample_table'.")
    except Exception as e:
        print(f"Bot: Error deleting data from MySQL: {str(e)}")



def select_file():
    """Open a file dialog and return the selected file path."""
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    file_path = filedialog.askopenfilename(
        title="Select a file",
        filetypes=[("Excel files", "*.xlsx"), ("CSV files", "*.csv")]
    )
    return file_path

def upload_file_to_database():
    """Upload an Excel or CSV file to the MySQL database using a file dialog."""
    file_path = select_file()
    if not file_path:
        print("Bot: No file selected. Operation cancelled.")
        return

    try:
        # Determine file type and read accordingly
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            raise ValueError("Unsupported file format. Please use .xlsx or .csv files.")

        # Get the file name without extension to use as table name
        table_name = os.path.splitext(os.path.basename(file_path))[0]

        # Upload the dataframe to MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Bot: File '{file_path}' has been successfully uploaded to the database as table '{table_name}'.")
        print(f"Bot: The table contains {len(df)} rows and {len(df.columns)} columns.")
    except Exception as e:
        print(f"Bot: Error uploading file to database: {str(e)}")



#%%
#1.Explore databases

def list_databases():
    """List all available databases in the connected MySQL server."""
    with engine.connect() as connection:
        result = connection.execute("SHOW DATABASES;")
        databases = [row[0] for row in result]
    return databases

def select_database():
    """Allow users to select a database to query."""
    databases = list_databases()
    print("Available Databases:")
    for i, db in enumerate(databases, start=1):
        print(f"{i}. {db}")

    while True:
        try:
            choice = int(input("Select a database by entering its number: "))
            if 1 <= choice <= len(databases):
                selected_db = databases[choice - 1]
                print(f"You have selected: {selected_db}")
                return selected_db
            else:
                print("Invalid choice. Please select a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def list_sql_tables(engine):
    """List tables and their attributes for a selected SQL database."""

    inspector = inspect(engine)

    tables_info = {}
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        tables_info[table_name] = {
            "columns": [column["name"] for column in columns]
        }

    return tables_info


def select_table(tables):
    """Allow users to select a table to query."""
    print("Available Tables:")
    for i, (table_name, attributes) in enumerate(tables.items(), start=1):
        print(f"{i}. {table_name} ({', '.join(attributes['columns'])})")

    while True:
        try:
            choice = int(input("Select a table by entering its number: "))
            if 1 <= choice <= len(tables):
                selected_table = list(tables.keys())[choice - 1]
                print(f"You have selected: {selected_table}")
                return selected_table
            else:
                print("Invalid choice. Please select a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")



def get_sql_sample_data(tables, engine):
    """Get sample data from a selected SQL table."""
    selected_table = select_table(tables)
    with engine.connect() as connection:
        query = f"SELECT * FROM {selected_table} LIMIT 5"
        result = connection.execute(query)
        sample_data = [dict(row) for row in result]

    print(f"Sample data from {selected_table}:")
    for row in sample_data:
        print(row)
    return sample_data

#%%
#2 
def define_query_patterns():
    """Define patterns for SQL queries based on different constructs"""
    return {
        "group by": {
            "patterns": [
                {
                    "questions": [
                        "Show me total {metric} by {category}",
                        "What is the total {metric} for each {category}",
                        "Calculate total {metric} grouped by {category}",
                        "Break down {metric} by {category}",
                        "Can you sum up {metric} for different {category}"
                    ],
                    "template": "SELECT {category}, SUM({metric}) as total_{metric}\nFROM {table}\nGROUP BY {category}",
                    "requires": ["numeric", "categorical"]
                },
                {
                    "questions": [
                        "What is the average {metric} for each {category}",
                        "Show average {metric} by {category}",
                        "Calculate mean {metric} for different {category}",
                        "Display average {metric} grouped by {category}",
                        "What's the typical {metric} for each {category}"
                    ],
                    "template": "SELECT {category}, AVG({metric}) as avg_{metric}\nFROM {table}\nGROUP BY {category}",
                    "requires": ["numeric", "categorical"]
                },
                {
                    "questions": [
                        "How many records are there for each {category}",
                        "Count the number of entries by {category}",
                        "Show record count for each {category}",
                        "Give me the count breakdown by {category}",
                        "What's the distribution of records across {category}"
                    ],
                    "template": "SELECT {category}, COUNT(*) as count\nFROM {table}\nGROUP BY {category}",
                    "requires": ["categorical"]
                }
            ]
        },
        "having": {
            "patterns": [
                {
                    "questions": [
                        "Which {category} have more than {threshold} records",
                        "Find {category} with record count over {threshold}",
                        "Show {category} containing at least {threshold} entries",
                        "List {category} that appear more than {threshold} times",
                        "Display frequently occurring {category} (more than {threshold} times)"
                    ],
                    "template": "SELECT {category}, COUNT(*) as count\nFROM {table}\nGROUP BY {category}\nHAVING count > {threshold}",
                    "requires": ["categorical"]
                }
            ]
        },
        "where": {
            "patterns": [
                {
                    "questions": [
                        "Find records where {filter_col} is {operator} {value}",
                        "Show entries with {filter_col} {operator} {value}",
                        "List all data where {filter_col} {operator} {value}",
                        "Get records that have {filter_col} {operator} {value}",
                        "Display items with {filter_col} {operator} {value}"
                    ],
                    "template": "SELECT *\nFROM {table}\nWHERE {filter_col} {operator} {value}",
                    "requires": ["any"]
                }
            ]
        },
        "order by": {
            "patterns": [
                {
                    "questions": [
                        "Sort the records by {sort_col} in {direction} order",
                        "Show data ordered by {sort_col} {direction}",
                        "List entries sorted by {sort_col} {direction}",
                        "Display results arranged by {sort_col} {direction}",
                        "Get records ranked by {sort_col} {direction}"
                    ],
                    "template": "SELECT {cols}\nFROM {table}\nORDER BY {sort_col} {direction}",
                    "requires": ["any"]
                }
            ]
        }
    }

def get_column_types(engine, table_name):
    """Get column types for a table"""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    
    numeric_cols = []
    categorical_cols = []
    all_cols = []
    
    for column in columns:
        col_name = column['name']
        col_type = str(column['type']).lower()
        all_cols.append(col_name)
        
        if any(type_name in col_type for type_name in ['int', 'float', 'decimal', 'numeric']):
            numeric_cols.append(col_name)
        else:
            categorical_cols.append(col_name)
            
    return {
        'numeric': numeric_cols,
        'categorical': categorical_cols,
        'all': all_cols
    }

def generate_construct_example(engine, construct_name):
    """Generate an example query using patterns"""
    patterns = define_query_patterns()
    if construct_name not in patterns:
        return "Construct not supported"
    
    # Get a random table
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if not tables:
        return "No tables available"
        
    table_name = random.choice(tables)
    columns = get_column_types(engine, table_name)
    
    # Get a random pattern for the construct
    pattern = random.choice(patterns[construct_name]["patterns"])
    
    try:
        # Prepare parameters
        params = {
            'table': table_name
        }
        
        if "categorical" in pattern["requires"]:
            if not columns['categorical']:
                return "Table needs categorical columns"
            params['category'] = random.choice(columns['categorical'])
            
        if "numeric" in pattern["requires"]:
            if not columns['numeric']:
                return "Table needs numeric columns"
            params['metric'] = random.choice(columns['numeric'])
            
        if "cols" in pattern["template"]:
            num_cols = min(3, len(columns['all']))
            params['cols'] = ', '.join(random.sample(columns['all'], num_cols))
            
        if "filter_col" in pattern["template"]:
            params['filter_col'] = random.choice(columns['all'])
            if params['filter_col'] in columns['numeric']:
                params['operator'] = random.choice(['>', '<', '>=', '<=', '='])
                params['value'] = random.randint(1, 100)
            else:
                params['operator'] = '='
                params['value'] = "'example_value'"
                
        if "sort_col" in pattern["template"]:
            params['sort_col'] = random.choice(columns['all'])
            params['direction'] = random.choice(['ASC', 'DESC'])
            
        if "threshold" in pattern["template"]:
            params['threshold'] = random.randint(2, 10)
        
        # Generate query and random question
        query = pattern["template"].format(**params)
        question = random.choice(pattern["questions"]).format(**params)
        
        return {
            "question": question,
            "query": query
        }
        
    except Exception as e:
        return f"Error generating query: {str(e)}"

def show_construct_example(engine):
    """Show a random example for a SQL construct"""
    constructs = list(define_query_patterns().keys())
    construct = random.choice(constructs)
    
    result = generate_construct_example(engine, construct)
    
    if isinstance(result, str):
        print(result)
        return
        
    print(f"SQL Construct: {construct.upper()}")
    print(f"\nQuestion: {result['question']}")
    print("\nQuery:")
    print(result['query'])
    
    print("\nWould you like to execute this query? (y/n)")
    if input().lower() == 'y':
        try:
            with engine.connect() as connection:
                query_result = connection.execute(result['query'])
                rows = query_result.fetchall()
                if rows:
                    print("\nResults (first 5 rows):")
                    for row in rows[:5]:
                        print(row)
                    if len(rows) > 5:
                        print("... (more results available)")
                else:
                    print("No results found.")
        except Exception as e:
            print(f"Error executing query: {str(e)}")






#%%
#3
def define_specific_constructs():
    """Define patterns for specific SQL constructs"""
    return {
        "group by": {
            "patterns": [
                {
                    "question": "Total {metric} by {category}",
                    "template": "SELECT {category}, SUM({metric}) as total_{metric}\nFROM {table}\nGROUP BY {category}",
                    "requires": ["numeric", "categorical"]
                },
                {
                    "question": "Count of records by {category}",
                    "template": "SELECT {category}, COUNT(*) as count\nFROM {table}\nGROUP BY {category}",
                    "requires": ["categorical"]
                },
                {
                    "question": "Average {metric} for each {category}",
                    "template": "SELECT {category}, AVG({metric}) as avg_{metric}\nFROM {table}\nGROUP BY {category}",
                    "requires": ["numeric", "categorical"]
                }
            ]
        },
        "having": {
            "patterns": [
                {
                    "question": "{category} groups with more than {threshold} records",
                    "template": "SELECT {category}, COUNT(*) as count\nFROM {table}\nGROUP BY {category}\nHAVING count > {threshold}",
                    "requires": ["categorical"]
                }
            ]
        },
        "order by": {
            "patterns": [
                {
                    "question": "Records sorted by {sort_col}",
                    "template": "SELECT {cols}\nFROM {table}\nORDER BY {sort_col} {direction}",
                    "requires": ["any"]
                }
            ]
        }
    }

def generate_specific_construct_query(engine, construct_name):
    """Generate a query for a specific SQL construct"""
    patterns = define_specific_constructs()
    construct = construct_name.lower()
    
    if construct not in patterns:
        return f"Unsupported construct. Available constructs: {', '.join(patterns.keys())}"
    
    # Get available tables
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if not tables:
        return "No tables available in the database"
        
    table_name = random.choice(tables)
    
    # Get column information
    columns = {}
    for col in inspector.get_columns(table_name):
        col_type = str(col['type']).lower()
        if 'numeric' not in columns:
            columns['numeric'] = []
        if 'categorical' not in columns:
            columns['categorical'] = []
        if any(type_name in col_type for type_name in ['int', 'float', 'decimal', 'numeric']):
            columns['numeric'].append(col['name'])
        else:
            columns['categorical'].append(col['name'])
    columns['all'] = columns['numeric'] + columns['categorical']
    
    # Get a random pattern for the construct
    pattern = random.choice(patterns[construct]["patterns"])
    
    try:
        # Prepare parameters
        params = {'table': table_name}
        
        # Handle different requirements
        if "categorical" in pattern["requires"]:
            if not columns['categorical']:
                return f"No categorical columns found in table {table_name}"
            params['category'] = random.choice(columns['categorical'])
            
        if "numeric" in pattern["requires"]:
            if not columns['numeric']:
                return f"No numeric columns found in table {table_name}"
            params['metric'] = random.choice(columns['numeric'])
        
        if "cols" in pattern["template"]:
            num_cols = min(3, len(columns['all']))
            params['cols'] = ', '.join(random.sample(columns['all'], num_cols))
            
        if "sort_col" in pattern["template"]:
            params['sort_col'] = random.choice(columns['all'])
            params['direction'] = random.choice(['ASC', 'DESC'])
            
        if "threshold" in pattern["template"]:
            params['threshold'] = random.randint(2, 10)
        
        # Generate query
        query = pattern["template"].format(**params)
        question = pattern["question"].format(**params)
        
        return {
            "question": question,
            "query": query
        }
        
    except Exception as e:
        return f"Error generating query: {str(e)}"

def show_specific_construct_examples(engine, construct):
    """Show examples for a specific SQL construct"""
    result = generate_specific_construct_query(engine, construct)
    
    if isinstance(result, str):  # Error message
        print(result)
        return
        
    print(f"\nSQL Construct: {construct.upper()}")
    print(f"Question: {result['question']}")
    print("\nQuery:")
    print(result['query'])
    
    print("\nWould you like to execute this query? (y/n)")
    if input().lower() == 'y':
        try:
            with engine.connect() as connection:
                query_result = connection.execute(result['query'])
                rows = query_result.fetchall()
                if rows:
                    print("\nResults (first 5 rows):")
                    for row in rows[:5]:
                        print(row)
                    if len(rows) > 5:
                        print("... (more results available)")
                else:
                    print("No results found.")
        except Exception as e:
            print(f"Error executing query: {str(e)}")

def handle_specific_construct_request(engine, user_input):
    """Handle requests for specific SQL construct examples"""
    # Map common phrases to constructs
    construct_keywords = {
        "group by": ["group by", "groupby", "grouping"],
        "having": ["having"],
        "order by": ["order by", "orderby", "sort"]
    }
    
    user_input = user_input.lower()
    
    # Identify requested construct
    for construct, keywords in construct_keywords.items():
        if any(keyword in user_input for keyword in keywords):
            show_specific_construct_examples(engine, construct)
            return True
            
    return False



#%%
def chatbot():
    global engine
    
    # Initial engine setup
    try:
        engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/mysql")
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return

    """Main function to run the chatbot and handle user interactions."""
    responses = {
        "hello": "Hello! Nice to meet you.",
        "who are you": "I am a chatbot with both in-memory and MySQL database capabilities.",
        "what can you do": "I can manage an in-memory database, interact with a MySQL database, and answer some predefined questions.",

        "help": [
            "Sure thing! Here are some commands you can try:\n"
            "- 'create sample database': Creates an in-memory sample database\n"
            "- 'connect to database': Shows the in-memory database contents\n"
            "- 'create mysql table': Creates a table in MySQL\n"
            "- 'insert sample data': Adds sample data to MySQL\n"
            "- 'delete all mysql data': Clears all data from MySQL table\n"
            "- 'delete punctuation': Removes punctuation from your text\n"
            "- 'upload file': Opens a file dialog to select and upload an Excel or CSV file to the database\n"
            "You can also just chat with me or ask for jokes!"
        ]
    }
    
    print("Bot: Hello! I'm your friendly database and text processing chatbot. Type 'help' for a list of commands, or just chat with me! (Type 'exit' to quit)")
    
    while True:
        user_input = input("\nYou: ").strip().lower()
        lower_input = user_input
        # print(lower_input)
        
        if lower_input == "exit":
            print(random.choice(responses["goodbye"]))
            break
        if "example" in user_input and any(keyword in user_input for keyword in ["group by", "having", "order by"]):
            handle_specific_construct_request(engine, user_input)
            continue
        #now users can ask:
            #"Show me group by examples"
            # "Can you give me examples of having clause"
            # "I need some order by examples"
    
        elif lower_input == "create database":
            create_new_database()
        elif lower_input == "delete database":
            delete_database()
        elif lower_input == "connect to database":
            connect_to_database()
        elif lower_input == "create mysql table":
            create_mysql_table()
        elif lower_input == "insert sample data":
            insert_sample_data()
        elif lower_input == "delete all mysql data":
            delete_all_mysql_data()
        elif lower_input == "delete punctuation":
            print("Bot: Please enter the text you want to remove punctuation from:")
        elif lower_input == "upload file":
            print("Bot: Opening file selection dialog...")
            upload_file_to_database()
        elif lower_input == "list all databases":
            print(list_databases())
        elif lower_input == "query with construct":
            show_construct_example(engine)  
            
            
            
        elif lower_input == "select database":
            selected_db = select_database()
            engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{selected_db}")   
            # Fetch and list tables and their attributes for the selected database
            tables = list_sql_tables(engine)  # Ensure list_sql_tables(engine) returns a dictionary of tables
            if tables:  # Check if tables are available
                print("Available Tables and their Attributes:")
                for table_name, attributes in tables.items():
                    print(f"{table_name}: {', '.join(attributes['columns'])}")
        
                print("Bot: Type 'give me some sample data' to get sample data from the selected database.")
                while True:
                    user_input = input("You: ").strip().lower()
                    if user_input == "give me some sample data":
                        get_sql_sample_data(tables, engine)  # Retrieve and display sample data
                        break
                    else:
                        print("Bot: Please type 'give me some sample data' to get sample data.")
            else:
                print("No tables found in the selected database.")
                
                
                
        #4  
        else:
            try:
                # First check if it looks like a query
                if any(word in lower_input for word in ['show', 'find', 'what', 'how', 'get', 'count', 'calculate', 'list']):
                    # First check if a database is selected
                    current_db = None
                    try:
                        with engine.connect() as connection:
                            result = connection.execute("SELECT DATABASE()")
                            current_db = result.scalar()
                    except:
                        current_db = None
        
                    if not current_db or current_db == 'mysql':  # Also prompt if we're still in default mysql database
                        print("Bot: Please select a database first.")
                        print("\nAvailable databases:")
                        databases = list_databases()
                        # Filter out system databases
                        user_databases = [db for db in databases if db not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
                        
                        if not user_databases:
                            print("No user databases found.")
                            return
                            
                        for i, db in enumerate(user_databases, 1):
                            print(f"{i}. {db}")
                        
                        while True:
                            try:
                                choice = input("\nEnter database number: ").strip()
                                if choice.lower() == 'cancel':
                                    print("Operation cancelled.")
                                    return
                                choice = int(choice)
                                if 1 <= choice <= len(user_databases):
                                    selected_db = user_databases[choice-1]
                                    # Update engine with new database
                                    engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{selected_db}")
                                    
                                    # Verify the tables exist in the selected database
                                    inspector = inspect(engine)
                                    tables = inspector.get_table_names()
                                    if not tables:
                                        print(f"\nNo tables found in database {selected_db}")
                                        return
                                        
                                    print(f"\nUsing database: {selected_db}")
                                    print("Available tables:", ", ".join(tables))
                                    break
                                else:
                                    print("Invalid choice. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
        
                    # Now process the query
                    try:
                        database_info = get_sql_database_info(engine)
                        result = generate_sql_query(lower_input, database_info)
                        
                        #result = generate_sql_from_question(engine, lower_input)
                        if 1 == 0:
                            print(f"Bot: {result}")
                        else:
                            print("\nBot: Generated SQL Query:")
                            print(result)
                            print("\nWould you like to execute this query? (y/n)")
                            if input().lower() == 'y':
                                try:
                                    with engine.connect() as connection:
                                        query_result = connection.execute(result)
                                        rows = query_result.fetchall()
                                        if rows:
                                            print("\nResults:")
                                            columns = query_result.keys()
                                            print("-" * 80)
                                            print("| " + " | ".join(f"{col:<15}" for col in columns) + " |")
                                            print("-" * 80)
                                            for row in rows[:5]:
                                                formatted_row = [str(val) if val is not None else 'NULL' for val in row]
                                                print("| " + " | ".join(f"{val:<15}" for val in formatted_row) + " |")
                                            print("-" * 80)
                                            if len(rows) > 5:
                                                print(f"\nShowing first 5 of {len(rows)} results")
                                        else:
                                            print("No results found.")
                                except Exception as e:
                                    print(f"Error executing query: {str(e)}")
                    except Exception as e:
                        print(f"Bot: Error generating query: {str(e)}")
                else:
                    # Handle non-query inputs
                    response = responses.get(lower_input)
                    if response:
                        print(f"Bot: {response}")
                    else:
                        print("Bot: I don't understand that command. Type 'help' for available commands.")
            except Exception as e:
                print(f"Bot: Error: {str(e)}")
   


#%%
if __name__ == "__main__":
    chatbot()