import pyodbc
import pandas as pd
import os
from datetime import datetime

class DatabaseManager:
    """
    Handles all database operations for the Well Production Application.
    Connects to Access database and provides methods to retrieve well data.
    """
    
    def __init__(self, db_path=None):
        """Initialize database connection"""
        self.db_path = db_path or os.path.join(os.getcwd(), "Data", "AC_SACHA_DIC_2024.mdb")
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to the Access database"""
        try:
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                f'DBQ={self.db_path};'
            )
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            return True
        except pyodbc.Error as e:
            print(f"Database connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None
    
    def get_well_locations(self):
        """Get all wells with their coordinates from MAESTRA table"""
        query = """
        SELECT WELL_LEGAL_NAME, COMPLETION_LEGAL_NAME, 
               COMPLETION_COORDINATE_X, COMPLETION_COORDINATE_Y
        FROM MAESTRA
        """
        try:
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error fetching well locations: {e}")
            return pd.DataFrame()
    
    def get_well_types(self):
        """Get well types from SC table"""
        query = """
        SELECT COMPLETION_LEGAL_NAME, TIPO_POZO, RESERVORIO
        FROM SC
        """
        try:
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error fetching well types: {e}")
            return pd.DataFrame()
    
    def get_production_data(self, well_names=None):
        """
        Get production data from MENSUAL table
        If well_names is provided, filter by those wells
        """
        query = """
        SELECT COMP_S_NAME, PROD_DT, VO_OIL_PROD, VO_GAS_PROD, 
               VO_WAT_PROD, DIAS_ON
        FROM MENSUAL
        """
        
        if well_names:
            well_list = ", ".join([f"'{name}'" for name in well_names])
            query += f" WHERE COMP_S_NAME IN ({well_list})"
        
        try:
            df = pd.read_sql(query, self.connection)
            # Convert date string to datetime
            df['PROD_DT'] = pd.to_datetime(df['PROD_DT'])
            return df
        except Exception as e:
            print(f"Error fetching production data: {e}")
            return pd.DataFrame()
    
    def get_injection_data(self, well_names=None):
        """
        Get injection data from INY_CALDAY table
        If well_names is provided, filter by those wells
        """
        query = """
        SELECT COMPLETION_LEGAL_NAME, Date, Water_INJ_CALDAY, press_iny
        FROM INY_CALDAY
        """
        
        if well_names:
            well_list = ", ".join([f"'{name}'" for name in well_names])
            query += f" WHERE COMPLETION_LEGAL_NAME IN ({well_list})"
        
        try:
            df = pd.read_sql(query, self.connection)
            # Convert date string to datetime
            df['Date'] = pd.to_datetime(df['Date'])
            return df
        except Exception as e:
            print(f"Error fetching injection data: {e}")
            return pd.DataFrame()
    
    def get_well_list(self):
        """Get a list of all well names"""
        query = """
        SELECT DISTINCT WELL_LEGAL_NAME 
        FROM MAESTRA
        """
        try:
            df = pd.read_sql(query, self.connection)
            return df['WELL_LEGAL_NAME'].tolist()
        except Exception as e:
            print(f"Error fetching well list: {e}")
            return []