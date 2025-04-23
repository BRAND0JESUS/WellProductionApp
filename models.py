from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

@dataclass
class Well:
    """Well data model to store well information"""
    well_name: str
    completion_name: str
    x_coordinate: float
    y_coordinate: float
    well_type: str = ""
    reservoir: str = ""
    selected: bool = False
    
    def __str__(self):
        return f"Well: {self.well_name} ({self.completion_name})"


class ProductionData:
    """Class to store and process production data for wells"""
    
    def __init__(self):
        self.data = pd.DataFrame()
    
    def load_from_dataframe(self, df: pd.DataFrame):
        """Load data from a pandas DataFrame"""
        self.data = df
    
    def get_monthly_oil_production(self, well_names: List[str] = None) -> pd.DataFrame:
        """Get monthly oil production for specified wells"""
        if self.data.empty:
            return pd.DataFrame()
            
        df = self.data.copy()
        
        if well_names:
            df = df[df['COMP_S_NAME'].isin(well_names)]
            
        # Group by month and sum production
        monthly = df.groupby(pd.Grouper(key='PROD_DT', freq='M')).agg({
            'VO_OIL_PROD': 'sum',
            'VO_GAS_PROD': 'sum',
            'VO_WAT_PROD': 'sum',
            'DIAS_ON': 'sum'
        }).reset_index()
        
        # Calculate daily rates (bbl/day) - calendar day rates
        # Get days in each month
        monthly['CALENDAR_DAYS'] = monthly['PROD_DT'].dt.daysinmonth
        
        # Calculate daily rates based on calendar days
        monthly['OIL_RATE'] = monthly['VO_OIL_PROD'] / monthly['CALENDAR_DAYS']
        monthly['GAS_RATE'] = monthly['VO_GAS_PROD'] / monthly['CALENDAR_DAYS']
        monthly['WATER_RATE'] = monthly['VO_WAT_PROD'] / monthly['CALENDAR_DAYS']
        monthly['LIQUID_RATE'] = monthly['OIL_RATE'] + monthly['WATER_RATE']
        
        # Calculate BSW (Basic Sediment and Water) percentage
        monthly['BSW'] = monthly['WATER_RATE'] / monthly['LIQUID_RATE'] * 100
        
        # Replace NaN with 0
        monthly.fillna(0, inplace=True)
        
        return monthly
        
    def get_decline_curve_data(self, well_names: List[str] = None) -> Dict:
        """
        Calculate decline curve parameters for oil production
        Returns dict with parameters and fitted curve
        """
        if self.data.empty:
            return {}
            
        df = self.get_monthly_oil_production(well_names)
        if df.empty:
            return {}
            
        # Convert to time series with months as X axis
        df['MONTHS'] = (df['PROD_DT'] - df['PROD_DT'].min()).dt.days / 30.0
        
        # Simple exponential decline fit
        try:
            # Remove zeros
            df_fit = df[df['OIL_RATE'] > 0]
            if len(df_fit) < 3:  # Need at least 3 points for a decent fit
                return {}
                
            log_rate = np.log(df_fit['OIL_RATE'])
            months = df_fit['MONTHS']
            
            # Linear regression on log(rate) vs time
            coeffs = np.polyfit(months, log_rate, 1)
            decline_rate = -coeffs[0] * 12  # Annual decline rate
            initial_rate = np.exp(coeffs[1])
            
            # Calculate fitted curve
            df['FITTED_RATE'] = initial_rate * np.exp(-coeffs[0] * df['MONTHS'])
            
            return {
                'initial_rate': initial_rate,
                'decline_rate': decline_rate,
                'months': df['MONTHS'].tolist(),
                'actual_rates': df['OIL_RATE'].tolist(),
                'fitted_rates': df['FITTED_RATE'].tolist()
            }
        except Exception as e:
            print(f"Error calculating decline curve: {e}")
            return {}


class InjectionData:
    """Class to store and process injection data for wells"""
    
    def __init__(self):
        self.data = pd.DataFrame()
    
    def load_from_dataframe(self, df: pd.DataFrame):
        """Load data from a pandas DataFrame"""
        self.data = df
    
    def get_monthly_injection(self, well_names: List[str] = None) -> pd.DataFrame:
        """Get monthly injection data for specified wells"""
        if self.data.empty:
            return pd.DataFrame()
            
        df = self.data.copy()
        
        if well_names:
            df = df[df['COMPLETION_LEGAL_NAME'].isin(well_names)]
            
        # Group by month and sum injection
        monthly = df.groupby(pd.Grouper(key='Date', freq='M')).agg({
            'Water_INJ_CALDAY': 'sum',
            'press_iny': 'mean'
        }).reset_index()
        
        # Calculate daily rates (bbl/day)
        monthly['CALENDAR_DAYS'] = monthly['Date'].dt.daysinmonth
        monthly['WATER_INJ_RATE'] = monthly['Water_INJ_CALDAY'] / monthly['CALENDAR_DAYS']
        
        # Replace NaN with 0
        monthly.fillna(0, inplace=True)
        
        return monthly


class WellDataStore:
    """Central data store for well information and selection state"""
    
    def __init__(self):
        self.wells: Dict[str, Well] = {}
        self.production_data = ProductionData()
        self.injection_data = InjectionData()
        self.selected_wells: List[str] = []
    
    def load_wells(self, wells_df, well_types_df):
        """
        Load wells from DataFrames
        Exclude wells that have 'PLA' in their name
        """
        for _, row in wells_df.iterrows():
            well_name = row['WELL_LEGAL_NAME']
            
            # Skip wells with "PLA" in their name
            if "PLA" in well_name:
                continue
                
            well = Well(
                well_name=well_name,
                completion_name=row['COMPLETION_LEGAL_NAME'],
                x_coordinate=row['COMPLETION_COORDINATE_X'],
                y_coordinate=row['COMPLETION_COORDINATE_Y']
            )
            self.wells[well.well_name] = well
        
        # Add well type information
        for _, row in well_types_df.iterrows():
            well_name = row['COMPLETION_LEGAL_NAME']
            for well in self.wells.values():
                if well.completion_name == well_name:
                    well.well_type = row['TIPO_POZO']
                    well.reservoir = row['RESERVORIO']
    
    def load_production_data(self, prod_df):
        """Load production data"""
        # Filter out production data for excluded wells (those with "PLA" in their name)
        filtered_df = prod_df[~prod_df['COMP_S_NAME'].str.contains('PLA', na=False)]
        self.production_data.load_from_dataframe(filtered_df)
    
    def load_injection_data(self, inj_df):
        """Load injection data"""
        # Filter out injection data for excluded wells (those with "PLA" in their name)
        filtered_df = inj_df[~inj_df['COMPLETION_LEGAL_NAME'].str.contains('PLA', na=False)]
        self.injection_data.load_from_dataframe(filtered_df)
    
    def select_well(self, well_name: str):
        """Select a well by name"""
        if well_name in self.wells:
            self.wells[well_name].selected = True
            if well_name not in self.selected_wells:
                self.selected_wells.append(well_name)
    
    def deselect_well(self, well_name: str):
        """Deselect a well by name"""
        if well_name in self.wells:
            self.wells[well_name].selected = False
            if well_name in self.selected_wells:
                self.selected_wells.remove(well_name)
    
    def toggle_well_selection(self, well_name: str):
        """Toggle selection state of a well"""
        if well_name in self.wells:
            if self.wells[well_name].selected:
                self.deselect_well(well_name)
            else:
                self.select_well(well_name)
    
    def clear_selection(self):
        """Clear all well selections"""
        for well in self.wells.values():
            well.selected = False
        self.selected_wells.clear()
    
    def get_selected_wells(self) -> List[Well]:
        """Get list of currently selected well objects"""
        return [self.wells[name] for name in self.selected_wells if name in self.wells]
    
    def is_well_selected(self, well_name: str) -> bool:
        """Check if a well is selected"""
        if well_name in self.wells:
            return self.wells[well_name].selected
        return False
    
    def get_production_for_selected(self) -> pd.DataFrame:
        """Get production data for selected wells"""
        if not self.selected_wells:
            return pd.DataFrame()
        
        # Get completion names for selected wells
        completion_names = [self.wells[name].completion_name for name in self.selected_wells 
                           if name in self.wells]
        
        return self.production_data.get_monthly_oil_production(completion_names)
    
    def get_injection_for_selected(self) -> pd.DataFrame:
        """Get injection data for selected wells"""
        if not self.selected_wells:
            return pd.DataFrame()
        
        # Get completion names for selected wells
        completion_names = [self.wells[name].completion_name for name in self.selected_wells 
                           if name in self.wells]
        
        return self.injection_data.get_monthly_injection(completion_names)