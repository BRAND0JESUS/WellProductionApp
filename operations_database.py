import os
import sqlite3
import pandas as pd
from datetime import datetime
import json

class OperationsDatabase:
    """
    Manages a SQLite database for storing calculated operations and analysis results
    for the Well Production Application.
    """
    
    def __init__(self, db_path=None):
        """Initialize database connection"""
        self.db_path = db_path or os.path.join(os.getcwd(), "Data", "operations_results.db")
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to the SQLite database"""
        try:
            # Create directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # Connect to database (will be created if it doesn't exist)
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            
            # Create necessary tables if they don't exist
            self._create_tables()
            
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Table for operation metadata
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS operations (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_name TEXT NOT NULL,
                description TEXT,
                creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                parameters TEXT,
                status TEXT DEFAULT 'completed'
            )
            ''')
            
            # Table for monthly well type classification
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS well_monthly_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                well_name TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                well_type TEXT NOT NULL,
                oil_rate REAL,
                water_rate REAL,
                water_inj_rate REAL,
                UNIQUE (operation_id, well_name, year, month)
            )
            ''')
            
            # Table for reservoir-specific well type classification
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS well_reservoir_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                well_name TEXT NOT NULL,
                reservoir TEXT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                well_type TEXT NOT NULL,
                oil_rate REAL,
                water_rate REAL,
                water_inj_rate REAL,
                UNIQUE (operation_id, well_name, reservoir, year, month)
            )
            ''')
            
            # Table for overall well classification (for wells that are both producer and injector)
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS well_overall_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                well_name TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                well_type TEXT NOT NULL,
                primary_type TEXT,
                secondary_type TEXT,
                has_dual_function INTEGER,
                remarks TEXT,
                UNIQUE (operation_id, well_name, year, month)
            )
            ''')
            
            self.connection.commit()
            return True
        
        except Exception as e:
            print(f"Error creating tables: {e}")
            self.connection.rollback()
            return False
    
    def operation_exists(self, operation_name):
        """Check if an operation with the given name exists"""
        query = "SELECT COUNT(*) FROM operations WHERE operation_name = ?"
        self.cursor.execute(query, (operation_name,))
        count = self.cursor.fetchone()[0]
        return count > 0
    
    def create_operation(self, operation_name, description=None, parameters=None):
        """
        Create a new operation entry and return its ID.
        If an operation with the same name exists, delete the old one first.
        """
        try:
            # Check if an operation with this name already exists
            self.cursor.execute("SELECT operation_id FROM operations WHERE operation_name = ?", (operation_name,))
            existing_operation = self.cursor.fetchone()
            
            if existing_operation:
                # Delete the old operation and all its related data
                old_operation_id = existing_operation[0]
                self.delete_operation(old_operation_id)
                print(f"Operación anterior '{operation_name}' eliminada (ID: {old_operation_id})")
                
            # Create the new operation
            query = "INSERT INTO operations (operation_name, description, parameters) VALUES (?, ?, ?)"
            self.cursor.execute(query, (operation_name, description, parameters))
            self.connection.commit()
            
            # Get the ID of the last inserted record
            return self.cursor.lastrowid
        
        except Exception as e:
            print(f"Error creando operación: {e}")
            self.connection.rollback()
            return None
    
    def save_well_monthly_type(self, operation_id, df):
        """
        Save well monthly type classification data
        
        operation_id: ID of the operation
        df: DataFrame with columns well_name, year, month, well_type, 
            oil_rate, water_rate, water_inj_rate
        """
        try:
            # Check if dataframe is empty
            if df.empty:
                print("Warning: Empty dataframe for well_monthly_type")
                return True
            
            # Ensure DataFrame has the expected columns
            required_columns = ['well_name', 'year', 'month', 'well_type', 'oil_rate', 'water_rate', 'water_inj_rate']
            for col in required_columns:
                if col not in df.columns:
                    print(f"Missing required column: {col}")
                    df[col] = 0.0 if col in ['oil_rate', 'water_rate', 'water_inj_rate'] else None
            
            # Ensure all values are of the expected type
            df = df.copy()
            df['year'] = df['year'].astype(int)
            df['month'] = df['month'].astype(int)
            df['oil_rate'] = df['oil_rate'].astype(float)
            df['water_rate'] = df['water_rate'].astype(float)
            df['water_inj_rate'] = df['water_inj_rate'].astype(float)
            
            # Filter out rows with None/NaN well_name
            df = df[df['well_name'].notna()]
            
            # Prepare batch insert statement
            insert_query = """
            INSERT OR REPLACE INTO well_monthly_type 
                (operation_id, well_name, year, month, well_type, oil_rate, water_rate, water_inj_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Convert DataFrame to list of tuples for batch insert
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    operation_id,
                    row['well_name'],
                    int(row['year']),
                    int(row['month']),
                    row['well_type'],
                    float(row['oil_rate']),
                    float(row['water_rate']),
                    float(row['water_inj_rate'])
                ))
            
            # Execute batch insert in smaller chunks
            chunk_size = 500
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                self.cursor.executemany(insert_query, chunk)
                self.connection.commit()  # Commit after each chunk
                
            return True
        
        except Exception as e:
            print(f"Error saving well monthly type data: {e}")
            self.connection.rollback()
            return False
    
    def save_well_reservoir_type(self, operation_id, df):
        """
        Save well-reservoir monthly type classification data
        
        operation_id: ID of the operation
        df: DataFrame with columns well_name, reservoir, year, month, well_type, 
            oil_rate, water_rate, water_inj_rate
        """
        try:
            # Check if dataframe is empty
            if df.empty:
                print("Warning: Empty dataframe for well_reservoir_type")
                return True
            
            # Ensure DataFrame has the expected columns
            required_columns = ['well_name', 'reservoir', 'year', 'month', 'well_type', 
                               'oil_rate', 'water_rate', 'water_inj_rate']
            for col in required_columns:
                if col not in df.columns:
                    print(f"Missing required column: {col}")
                    df[col] = 0.0 if col in ['oil_rate', 'water_rate', 'water_inj_rate'] else None
                
            # Ensure all values are of the expected type
            df = df.copy()
            df['year'] = df['year'].astype(int)
            df['month'] = df['month'].astype(int)
            df['oil_rate'] = df['oil_rate'].astype(float)
            df['water_rate'] = df['water_rate'].astype(float)
            df['water_inj_rate'] = df['water_inj_rate'].astype(float)
            
            # Filter out rows with None/NaN well_name
            df = df[df['well_name'].notna()]
            
            # Handle null reservoirs
            df['reservoir'] = df['reservoir'].fillna('UNKNOWN')
            
            # Prepare batch insert statement
            insert_query = """
            INSERT OR REPLACE INTO well_reservoir_type 
                (operation_id, well_name, reservoir, year, month, well_type, oil_rate, water_rate, water_inj_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Convert DataFrame to list of tuples for batch insert
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    operation_id,
                    row['well_name'],
                    row['reservoir'],
                    int(row['year']),
                    int(row['month']),
                    row['well_type'],
                    float(row['oil_rate']),
                    float(row['water_rate']),
                    float(row['water_inj_rate'])
                ))
            
            # Execute batch insert in chunks to avoid memory issues
            chunk_size = 500
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                self.cursor.executemany(insert_query, chunk)
                self.connection.commit()  # Commit after each chunk
            
            return True
        
        except Exception as e:
            print(f"Error saving well reservoir type data: {e}")
            self.connection.rollback()
            return False
    
    def save_well_overall_type(self, operation_id, df):
        """
        Save well overall type classification data, including dual function wells
        
        operation_id: ID of the operation
        df: DataFrame with columns well_name, year, month, well_type, primary_type,
            secondary_type, has_dual_function, remarks
        """
        try:
            # Check if dataframe is empty
            if df.empty:
                print("Warning: Empty dataframe for well_overall_type")
                return True
            
            # Ensure DataFrame has the expected columns
            required_columns = ['well_name', 'year', 'month', 'well_type', 
                               'primary_type', 'secondary_type', 'has_dual_function', 'remarks']
            for col in required_columns:
                if col not in df.columns:
                    print(f"Missing required column: {col}")
                    df[col] = '' if col in ['remarks', 'primary_type', 'secondary_type'] else 0
                
            # Ensure all values are of the expected type
            df = df.copy()
            df['year'] = df['year'].astype(int)
            df['month'] = df['month'].astype(int)
            
            # Replace NaN values with appropriate defaults
            df['primary_type'] = df['primary_type'].fillna('UNKNOWN')
            df['secondary_type'] = df['secondary_type'].fillna('NONE')
            df['remarks'] = df['remarks'].fillna('')
            df['has_dual_function'] = df['has_dual_function'].fillna(0)
            
            # Filter out rows with None/NaN well_name
            df = df[df['well_name'].notna()]
            
            # Prepare batch insert statement
            insert_query = """
            INSERT OR REPLACE INTO well_overall_type 
                (operation_id, well_name, year, month, well_type, primary_type, secondary_type, has_dual_function, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Convert DataFrame to list of tuples for batch insert
            rows = []
            for _, row in df.iterrows():
                has_dual = int(row['has_dual_function'])
                rows.append((
                    operation_id,
                    row['well_name'],
                    int(row['year']),
                    int(row['month']),
                    row['well_type'],
                    row['primary_type'],
                    row['secondary_type'],
                    has_dual,
                    row['remarks']
                ))
            
            # Execute batch insert in chunks
            chunk_size = 500
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                self.cursor.executemany(insert_query, chunk)
                self.connection.commit()  # Commit after each chunk
                
            return True
        
        except Exception as e:
            print(f"Error saving well overall type data: {e}")
            self.connection.rollback()
            return False
    
    def get_well_monthly_type(self, operation_id=None, well_name=None):
        """
        Get well monthly type classification data
        
        operation_id: Optional ID of the operation to filter by
        well_name: Optional name of the well to filter by
        
        Returns: DataFrame with the requested data
        """
        query = "SELECT * FROM well_monthly_type WHERE 1=1"
        params = []
        
        if operation_id is not None:
            query += " AND operation_id = ?"
            params.append(operation_id)
        
        if well_name is not None:
            query += " AND well_name = ?"
            params.append(well_name)
        
        try:
            # Add order by clause
            query += " ORDER BY well_name, year, month"
            
            self.cursor.execute(query, params)
            columns = [description[0] for description in self.cursor.description]
            data = self.cursor.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Handle empty results
            if df.empty:
                return pd.DataFrame(columns=columns)
                
            return df
        
        except Exception as e:
            print(f"Error getting well monthly type data: {e}")
            return pd.DataFrame()
    
    def get_well_reservoir_type(self, operation_id=None, well_name=None, reservoir=None):
        """
        Get well-reservoir monthly type classification data
        
        operation_id: Optional ID of the operation to filter by
        well_name: Optional name of the well to filter by
        reservoir: Optional reservoir to filter by
        
        Returns: DataFrame with the requested data
        """
        query = "SELECT * FROM well_reservoir_type WHERE 1=1"
        params = []
        
        if operation_id is not None:
            query += " AND operation_id = ?"
            params.append(operation_id)
        
        if well_name is not None:
            query += " AND well_name = ?"
            params.append(well_name)
        
        if reservoir is not None:
            query += " AND reservoir = ?"
            params.append(reservoir)
        
        try:
            # Add order by clause
            query += " ORDER BY well_name, reservoir, year, month"
            
            # Fetch data in chunks for large datasets
            self.cursor.execute(query, params)
            columns = [description[0] for description in self.cursor.description]
            
            # Fetch all data
            data = self.cursor.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Handle empty results
            if df.empty:
                return pd.DataFrame(columns=columns)
                
            return df
        
        except Exception as e:
            print(f"Error getting well reservoir type data: {e}")
            return pd.DataFrame()
    
    def get_well_overall_type(self, operation_id=None, well_name=None):
        """
        Get well overall type classification data
        
        operation_id: Optional ID of the operation to filter by
        well_name: Optional name of the well to filter by
        
        Returns: DataFrame with the requested data
        """
        query = "SELECT * FROM well_overall_type WHERE 1=1"
        params = []
        
        if operation_id is not None:
            query += " AND operation_id = ?"
            params.append(operation_id)
        
        if well_name is not None:
            query += " AND well_name = ?"
            params.append(well_name)
        
        try:
            # Add order by clause
            query += " ORDER BY well_name, year, month"
            
            self.cursor.execute(query, params)
            columns = [description[0] for description in self.cursor.description]
            data = self.cursor.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Handle empty results
            if df.empty:
                return pd.DataFrame(columns=columns)
                
            # Convert has_dual_function to boolean for easier use in application
            if 'has_dual_function' in df.columns:
                df['has_dual_function'] = df['has_dual_function'].astype(bool)
                
            return df
        
        except Exception as e:
            print(f"Error getting well overall type data: {e}")
            return pd.DataFrame()
    
    def get_operations(self):
        """Get list of all operations"""
        query = "SELECT * FROM operations ORDER BY creation_date DESC"
        try:
            self.cursor.execute(query)
            columns = [description[0] for description in self.cursor.description]
            data = self.cursor.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)
            return df
        
        except Exception as e:
            print(f"Error getting operations: {e}")
            return pd.DataFrame()
    
    def get_latest_operation_id(self, operation_name):
        """Get the ID of the latest operation with the given name"""
        query = """
        SELECT operation_id FROM operations 
        WHERE operation_name = ? 
        ORDER BY creation_date DESC LIMIT 1
        """
        try:
            self.cursor.execute(query, (operation_name,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        
        except Exception as e:
            print(f"Error getting latest operation ID: {e}")
            return None
    
    def delete_operation(self, operation_id):
        """Delete an operation and all its related data"""
        try:
            # Begin transaction
            self.connection.execute("BEGIN TRANSACTION")
            
            # Delete from well_overall_type
            self.cursor.execute("DELETE FROM well_overall_type WHERE operation_id = ?", (operation_id,))
            
            # Delete from well_monthly_type
            self.cursor.execute("DELETE FROM well_monthly_type WHERE operation_id = ?", (operation_id,))
            
            # Delete from well_reservoir_type
            self.cursor.execute("DELETE FROM well_reservoir_type WHERE operation_id = ?", (operation_id,))
            
            # Delete from operations
            self.cursor.execute("DELETE FROM operations WHERE operation_id = ?", (operation_id,))
            
            # Commit transaction
            self.connection.commit()
            print(f"Operación ID: {operation_id} eliminada exitosamente")
            return True
            
        except Exception as e:
            print(f"Error eliminando operación: {e}")
            self.connection.rollback()
            return False