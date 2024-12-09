#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 04:23:34 2024

@author: w
"""

import re
from typing import Dict, List, Optional
from fuzzywuzzy import process
from sqlalchemy import create_engine, inspect, MetaData
from typing import Dict, List
import re
import urllib.parse


class QueryRecognizer:
    def __init__(self):
        
     
        
        self.orderby_patterns = [
             (r'sort\s+by\s+([\w_]+)\s+desc(?:ending)?', 'DESC'),
             (r'sort\s+by\s+([\w_]+)\s+asc(?:ending)?', 'ASC'),
             (r'order\s+by\s+([\w_]+)', 'ASC'),
             (r'sort\s+by\s+([\w_]+)', 'ASC')
         ]
         
        
        # JOIN 
        self.join_patterns = [
            r'inner\s+join\s+([\w_]+)',  # "inner join <table>"
            r'left\s+join\s+([\w_]+)',   # "left join <table>"
            r'right\s+join\s+([\w_]+)',  # "right join <table>"
            r'join\s+with\s+([\w_]+)',    # "join with <table>"
            r'relate\s+to\s+([\w_]+)',    # "relate to <table>"
            r'connected\s+with\s+([\w_]+)' # "connected with <table>"
        ]
        
        
        self.agg_patterns = {
            'average': 'AVG',      
            'avg': 'AVG',          
            'minimum': 'MIN',      
            'max': 'MAX',          
            'sum': 'SUM',         
            'count': 'COUNT'       
        }
        

        # GROUP BY 
        self.group_by_patterns = [
            r'group\s+by\s+([\w_]+)',   # "group by"
            r'\bper\b\s+([\w_]+)',  # “per category”
            r'\bfor each\b\s+([\w_]+)'  #  “for each type”
        ]
        
        
        self.where_patterns = {
            # Match equality conditions
            r'([\w_]+)\s+is\s+([\w\s]+)': lambda f, v: f"{f} = '{v.strip()}'",
            r'([\w_]+)\s+equals?\s+([\w\s]+)': lambda f, v: f"{f} = '{v.strip()}'",
            
            # Match greater than conditions
            r'([\w_]+)\s+greater\s+than\s+(\d+(?:\.\d+)?)': lambda f, v: f"{f} > {v}",
            r'([\w_]+)\s+more\s+than\s+(\d+(?:\.\d+)?)': lambda f, v: f"{f} > {v}",  # 'more than' correctly captures the number
        
            # Match less than or below conditions
            r'([\w_]+)\s+less\s+than\s+(\d+(?:\.\d+)?)': lambda f, v: f"{f} < {v}",
            r'([\w_]+)\s+under\s+(\d+(?:\.\d+)?)': lambda f, v: f"{f} < {v}",
        
            # Match string containment conditions
            r'([\w_]+)\s+contains?\s+([\w\s]+)': lambda f, v: f"{f} LIKE '%%{v.strip()}%%'",  # Proper string concatenation
            r'([\w_]+)\s+starts?\s+with\s+([\w\s]+)': lambda f, v: f"{f} LIKE '{v.strip()}%%'",  # Starts with
            r'([\w_]+)\s+ends?\s+with\s+([\w\s]+)': lambda f, v: f"{f} LIKE '%%{v.strip()}'"   # Ends with
        }


        
        # SQL 
        self.query_parts = {
            'select': [],
            'from': [],
            'join': [],
            'where': [],
            'group_by': [],
            'having': [],
            'order_by': []
        }

#%%
    def split_message_to_parts(self, message: str) -> List[str]:
        """
        Parse a message into distinct segments, keeping quoted substrings intact.
        """
        parts, current, quote_char = [], [], None
    
        for char in message:
            if char in ('"', "'"):
                if quote_char is None:  # Starting a quoted string
                    quote_char = char
                elif char == quote_char:  # Ending the quoted string
                    quote_char = None
                current.append(char)
            elif char.isspace() and quote_char is None:  # Outside of quotes
                if current:
                    parts.append(''.join(current))
                    current = []
            else:
                current.append(char)
    
        if current:  # Append any leftover segment
            parts.append(''.join(current))
    
        return parts


#%%
    def recognize_patterns(self, message: str, database_info: Dict) -> Dict:
        """Recognize patterns in the message and map them to SQL query parts."""
        message_parts = self.split_message_to_parts(message.lower())
        
        # First, identify the main table and columns
        self._identify_table_and_columns(message_parts, database_info)
        
        # Recognize aggregations
        self._recognize_aggregations(message, database_info)
        
        # Recognize joins
        self._recognize_joins(message, database_info)
        
        # Recognize where conditions
        self._recognize_where_conditions(message, database_info)
        
        # Recognize group by
        self._recognize_group_by(message, database_info)
        
        # Recognize order by
        self._recognize_order_by(message, database_info)
        
        return self.query_parts

#%%
 
    def _identify_table_and_columns(self, message_parts: List[str], database_info: Dict):
        """Identify tables and columns from the message."""
        remaining_parts = message_parts.copy()
    
        # Try to match table names
        for part in remaining_parts:
            clean_part = part.strip('"\'')
            table_matches = process.extract(clean_part, list(database_info.keys()), limit=1)
    
            if table_matches and table_matches[0][1] >= 80:  # 80% similarity threshold
                matched_table = table_matches[0][0]
                if matched_table not in self.query_parts['from']:
                    self.query_parts['from'].append(matched_table)
                remaining_parts.remove(part)  # Remove matched table part
        
        # Attempt to match columns for identified tables
        for part in remaining_parts:
            clean_part = part.strip('"\'')
            if clean_part in database_info:
                if clean_part not in self.query_parts['from']:
                    self.query_parts['from'].append(clean_part)
                if not self.query_parts['select']:
                    self.query_parts['select'].append('*')
                
#%%  

    def _fuzzy_match_column(self, column_name: str, database_info: Dict, similarity_threshold: int = 80) -> Optional[str]:
        """
        Fuzzy match a column name against all available columns in the database.
        Returns the best matching column name or None if no good match is found.
        """
        # Early return if there's no main table
        if not self.query_parts.get('from'):
            return None
    
        main_table = self.query_parts['from'][0]
        columns = database_info.get(main_table, {}).get('columns', [])
    
        # If no columns available for the table, return None
        if not columns:
            return None
    
        # Perform fuzzy matching
        best_match, best_score = process.extractOne(column_name, columns)
    
        # Return the match if it meets the threshold
        return best_match if best_score >= similarity_threshold else None
    
 #%%   
       
    def _fuzzy_match_table(self, table_name: str, database_info: Dict, similarity_threshold: int = 80) -> Optional[str]:
        """
        Fuzzy match a table name against all tables in the database.
        Returns the best matching table name or None if no good match is found.
        """
        # Retrieve all available table names
        available_tables = list(database_info.keys())
    
        # If no tables exist in the database, return None
        if not available_tables:
            return None
    
        # Perform fuzzy matching for the table name
        best_match, best_score = process.extractOne(table_name, available_tables)
    
        # Return the table name if the match meets the threshold
        return best_match if best_score >= similarity_threshold else None
    
#%%
    def _recognize_aggregations(self, message: str, database_info: Dict):
        """Recognize aggregation functions and comparison conditions in the message with fuzzy column matching."""
        words = message.lower().split()
        have_groupby = False
        
        # First handle regular aggregations
        for agg_word, agg_func in self.agg_patterns.items():
            if agg_word in words:
                idx = words.index(agg_word)
                if idx + 1 < len(words):
                    potential_column = words[idx + 1]
                    if agg_func == 'COUNT':
                        self.query_parts['select'] = [f"COUNT(1) as count"]
                    else:
                        matched_column = self._fuzzy_match_column(potential_column, database_info)
                        if matched_column:
                            self.query_parts['select'].append(f"{agg_func}({matched_column})")
                    
                    # Remove matched aggregation and column
                    del words[idx:idx + 2]
                    have_groupby = True
        
        # Handle comparison patterns if GROUP BY exists
        if have_groupby:
            self._recognize_comparisons(message, database_info)

    def _recognize_comparisons(self, message: str, database_info: Dict):
        """Recognize comparison conditions in the message."""
        comparison_patterns = [
            (r'greater\s+th[ae]n\s+(\d+)', '>'),
            (r'more\s+th[ae]n\s+(\d+)', '>'),
            (r'above\s+(\d+)', '>'),
            (r'exceeds?\s+(\d+)', '>'),
            (r'over\s+(\d+)', '>'),
            (r'>\s*(\d+)', '>'),
            (r'less\s+th[ae]n\s+(\d+)', '<'),
            (r'under\s+(\d+)', '<'),
            (r'below\s+(\d+)', '<'),
            (r'fewer\s+th[ae]n\s+(\d+)', '<'),
            (r'<\s*(\d+)', '<'),
            (r'equal\s+to\s+(\d+)', '='),
            (r'equals?\s+(\d+)', '='),
            (r'exactly\s+(\d+)', '='),
            (r'is\s+(\d+)', '='),
            (r'=\s*(\d+)', '='),
            (r'at\s+least\s+(\d+)', '>='),
            (r'minimum\s+of\s+(\d+)', '>='),
            (r'not\s+less\s+th[ae]n\s+(\d+)', '>='),
            (r'>=\s*(\d+)', '>='),
            (r'at\s+most\s+(\d+)', '<='),
            (r'maximum\s+of\s+(\d+)', '<='),
            (r'not\s+more\s+th[ae]n\s+(\d+)', '<='),
            (r'<=\s*(\d+)', '<='),
            (r'not\s+equal\s+to\s+(\d+)', '!='),
            (r'different\s+from\s+(\d+)', '!='),
            (r'!=\s*(\d+)', '!='),
            (r'<>\s*(\d+)', '!=')
        ]

        for pattern, operator in comparison_patterns:
            match = re.search(pattern, message)
            if match:
                value = match.group(1)
                self._handle_comparison(value, operator)

    def _handle_comparison(self, value: str, operator: str):
        """Handle the comparison condition for aggregation queries."""
        for agg in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
            if any(agg in select for select in self.query_parts.get('select', [])):
                if 'COUNT(1) as count' in self.query_parts['select']:
                    self.query_parts.setdefault('having', []).append(f"COUNT(1) {operator} {value}")
                else:
                    agg_expr = next(select.split(' as ')[0] for select in self.query_parts['select']
                                    if any(agg in select for agg in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']))
                    self.query_parts.setdefault('having', []).append(f"{agg_expr} {operator} {value}")
                break

    def _recognize_joins(self, message: str, database_info: Dict):
        """Recognize join conditions in the message with fuzzy table matching."""
        for pattern in self.join_patterns:
            for match in re.finditer(pattern, message.lower()):
                matched_table = self._fuzzy_match_table(match.group(1), database_info)
                if matched_table and matched_table not in self.query_parts['join']:
                    self.query_parts['join'].append(matched_table)

    def _recognize_where_conditions(self, message: str, database_info: Dict):
        """Recognize where conditions in the message with fuzzy column matching."""
        for pattern, formatter in self.where_patterns.items():
            for match in re.finditer(pattern, message.lower()):
                matched_field = self._fuzzy_match_column(match.group(1), database_info)
                if matched_field:
                    try:
                        condition = formatter(matched_field, match.group(2))
                        if condition not in self.query_parts['where']:
                            self.query_parts['where'].append(condition)
                    except Exception as e:
                        print(f"Error formatting condition: {str(e)}")
                        continue

    def _recognize_group_by(self, message: str, database_info: Dict):
        """Recognize group by clauses in the message with fuzzy column matching."""
        for pattern in self.group_by_patterns:
            for match in re.finditer(pattern, message.lower()):
                matched_field = self._fuzzy_match_column(match.group(1), database_info)
                if matched_field and matched_field not in self.query_parts['group_by']:
                    self.query_parts['group_by'].append(matched_field)

    def _recognize_order_by(self, message: str, database_info: Dict):
        """Recognize order by clauses in the message with fuzzy column matching."""
        for pattern, direction in self.orderby_patterns:
            for match in re.finditer(pattern, message.lower()):
                matched_field = self._fuzzy_match_column(match.group(1), database_info)
                if matched_field:
                    self.query_parts['order_by'].append((matched_field, direction))
                    
                    
                    
  
#%%
def find_common_columns(table1: str, table2: str, database_info: Dict) -> Optional[str]:
    """Find columns with the same name in both tables for joining."""
    # Return None if either table is missing
    if table1 not in database_info or table2 not in database_info:
        return None
    
    # Get columns for both tables as sets
    table1_cols, table2_cols = set(database_info[table1]['columns']), set(database_info[table2]['columns'])
    
    # Find common columns between both tables
    common_cols = table1_cols & table2_cols
    
    # Return join condition if common columns exist
    if common_cols:
        return f"{table1}.{next(iter(common_cols))} = {table2}.{next(iter(common_cols))}"
    
    return None


def get_sql_database_info(engine) -> Dict[str, Dict[str, List[str]]]:
    """
    Retrieve database schema information, ensuring fixed column handling.
    """
    try:
        inspector = inspect(engine)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        database_info = {}

        # Loop through all tables to collect schema info
        for table_name in inspector.get_table_names():
            database_info[table_name] = {
                'columns': [],
                'foreign_keys': [],
                'primary_key': [],
                'relationships': [],
                'variations': []
            }

            # Get and store columns for the table
            columns = inspector.get_columns(table_name)
            database_info[table_name]['columns'] = [col['name'] for col in columns]
            
            # Retrieve primary keys
            pk = inspector.get_pk_constraint(table_name)
            if pk:
                database_info[table_name]['primary_key'] = pk.get('constrained_columns', [])

            # Retrieve foreign keys
            fks = inspector.get_foreign_keys(table_name)
            for fk in fks:
                for col_name, ref_col in zip(fk['constrained_columns'], fk['referred_columns']):
                    database_info[table_name]['foreign_keys'].append((col_name, fk['referred_table'], ref_col))
                    if fk['referred_table'] not in database_info[table_name]['relationships']:
                        database_info[table_name]['relationships'].append(fk['referred_table'])

            # Handle name variations (singular/plural)
            table_variations = [table_name] + [table_name[:-1]] if table_name.endswith('s') else [table_name]
            database_info[table_name]['variations'] = list(set(table_variations))

        return database_info

    except Exception as e:
        print(f"Error retrieving database info: {e}")
        return {}


def print_database_info(database_info: Dict) -> None:
    """
    Print the database schema information in a human-readable format.
    """
    print("\nDatabase Schema Overview:")
    print("=" * 80)

    for table_name, table_info in database_info.items():
        print(f"\nTable: {table_name}")
        print("-" * 40)

        # Print columns with primary key markers
        print("\nColumns:")
        for column in table_info['columns']:
            pk_marker = "*" if column in table_info['primary_key'] else " "
            print(f"  {pk_marker} {column}")

        # Print foreign keys if they exist
        if table_info['foreign_keys']:
            print("\nForeign Keys:")
            for fk in table_info['foreign_keys']:
                print(f"  {fk[0]} -> {fk[1]}.{fk[2]}")

        # Print relationships if they exist
        if table_info['relationships']:
            print("\nRelationships:")
            for rel in table_info['relationships']:
                print(f"  - {rel}")

        # Print table name variations if they exist
        if table_info['variations']:
            print("\nName Variations:")
            print(f"  {', '.join(table_info['variations'])}")

        print("-" * 40)




#%%        
def generate_sql_query(message: str, database_info: Dict) -> str:
    """Generate a SQL query based on the recognized patterns in the input message."""
    recognizer = QueryRecognizer()
    query_parts = recognizer.recognize_patterns(message, database_info)
    
    # Initialize SQL parts as a list to hold individual clauses
    sql_parts = []
    
    # SELECT Clause
    select_clause = "SELECT "
    if query_parts['select']:
        if query_parts['group_by'] and any(agg in select for agg in ['COUNT', 'AVG', 'SUM', 'MIN', 'MAX'] for select in query_parts['select']):
            select_clause += ', '.join(query_parts['group_by'] + query_parts['select'])
        else:
            select_clause += ', '.join(query_parts['select'])
    else:
        select_clause += "*"
    sql_parts.append(select_clause)
    
    # FROM Clause
    if query_parts['from']:
        sql_parts.append(f"FROM {query_parts['from'][0]}")
    
    # JOIN Clauses
    for join_table in query_parts['join']:
        join_condition = find_common_columns(query_parts['from'][0], join_table, database_info)
        if join_condition:
            sql_parts.append(f"JOIN {join_table} ON {join_condition}")
    
    # WHERE Clause
    if query_parts['where'] and not query_parts['group_by']:
        sql_parts.append(f"WHERE {' AND '.join(query_parts['where'])}")
    
    # GROUP BY Clause
    if query_parts['group_by']:
        sql_parts.append(f"GROUP BY {', '.join(query_parts['group_by'])}")
    
    # HAVING Clause
    if query_parts['having']:
        sql_parts.append(f"HAVING {' AND '.join(query_parts['having'])}")
    
    # ORDER BY Clause
    if query_parts['order_by']:
        sql_parts.append(f"ORDER BY {', '.join([f'{field} {direction}' for field, direction in query_parts['order_by']])}")

    return " ".join(sql_parts)



