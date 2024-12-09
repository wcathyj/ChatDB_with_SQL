# ChatDB_with_SQL

# SQL Query Assistant 

A Python-based chatbot that helps users interact with MySQL databases, provides SQL query examples, and converts natural language questions into SQL queries.

## Prerequisites

- Python 3.x
- MySQL Server installed and running
- Required Python packages (install using pip):
  ```bash
  pip install pandas sqlalchemy pymysql fuzzywuzzy python-Levenshtein tkinter
  ```

## Configuration

1. Update the MySQL connection details in `pro_final.py`:
   ```python
   MYSQL_HOST = 'localhost'
   MYSQL_USER = 'root'
   MYSQL_PASSWORD = 'your_password'
   MYSQL_DB = 'mydata'
   MYSQL_PORT = 3306
   ```

2. Ensure your MySQL server is running before starting the program.

## Running the Program

1. Open a terminal or command prompt
2. Navigate to the project directory
3. Run the program:
   ```bash
   python pro_final.py
   ```

## Available Commands

The chatbot supports the following commands:

- `create database`: Create a new MySQL database
- `delete database`: Delete an existing database
- `connect to database`: Connect to a database
- `create mysql table`: Create a sample table
- `insert sample data`: Insert sample data into the table
- `delete all mysql data`: Delete all data from the current table
- `upload file`: Upload Excel/CSV file to database
- `list all databases`: Show all available databases
- `select database`: Select a database to work with
- `query with construct`: Get SQL query examples with different constructs
- `help`: Show all available commands

### SQL Construct Examples

Request specific SQL construct examples using:
- "Show me group by examples"
- "Can you give me examples of having clause"
- "I need some order by examples"

### Natural Language Queries

The chatbot can convert natural language questions into SQL queries. Examples:
- "Show me total sales by category"
- "Find all customers where age is greater than 30"
- "Count orders per customer"

## Workspace Structure

```
project/
├── pro_final.py         # Main program file
├── q4_without_test.py   # Query generation module
└── README.md           # This file
```

## Features

1. **Database Management**
   - Create and delete databases
   - Create tables and manage data
   - Upload data from Excel/CSV files

2. **SQL Learning**
   - Interactive SQL construct examples
   - Explanation of different SQL clauses
   - Sample query generation

3. **Natural Language Processing**
   - Convert English questions to SQL queries
   - Support for various query types (SELECT, GROUP BY, etc.)
   - Fuzzy matching for column and table names

4. **Data Exploration**
   - View database schema
   - Sample data preview
   - List available tables and columns

## Error Handling

The program includes comprehensive error handling for:
- Database connection issues
- Invalid SQL queries
- File upload problems
- Invalid user input

## Notes

- The program uses an in-memory database alongside MySQL
- Make sure to have proper MySQL permissions
- Backup important data before using delete operations
- Large file uploads may take some time to process

## Troubleshooting

1. If you encounter database connection errors:
   - Verify MySQL is running
   - Check connection credentials
   - Ensure proper user permissions

2. For file upload issues:
   - Verify file format (xlsx/csv)
   - Check file permissions
   - Ensure file is not open in another program

## Contributing

Feel free to fork the repository and submit pull requests for improvements.

## License

This project is available under the MIT License.
