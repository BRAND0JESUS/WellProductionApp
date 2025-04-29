from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Set
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
    active: bool = False  # Added active status flag
    
    def __str__(self):
        return f"Well: {self.well_name} ({self.completion_name})"


class ProductionData:
    """Class to store and process production data for wells"""
    
    def __init__(self):
        self.data = pd.DataFrame()
    
    def load_from_dataframe(self, df: pd.DataFrame):
        """Load data from a pandas DataFrame"""
        self.data = df
    
    def get_monthly_oil_production(self, completion_names: List[str] = None) -> pd.DataFrame:
        """Get monthly oil production for specified completions"""
        if self.data.empty:
            return pd.DataFrame()
            
        df = self.data.copy()
        
        if completion_names:
            df = df[df['COMP_S_NAME'].isin(completion_names)]
            
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
    
    def is_well_active_in_december_2024(self, completion_name: str) -> bool:
        """
        Check if a well was active in December 2024
        Returns True if the well had production data > 0 in Dec 2024
        """
        if self.data.empty:
            return False
            
        # Filter for the specific completion
        completion_data = self.data[self.data['COMP_S_NAME'] == completion_name]
        
        # Check for December 2024 data
        dec_2024 = pd.Timestamp('2024-12-01')
        
        # Get data for December 2024
        dec_data = completion_data[
            (completion_data['PROD_DT'].dt.year == 2024) & 
            (completion_data['PROD_DT'].dt.month == 12)
        ]
        
        # Check if there's any production
        if dec_data.empty:
            return False
            
        # Check if there's any oil or water production > 0
        has_production = (
            (dec_data['VO_OIL_PROD'].sum() > 0) or 
            (dec_data['VO_WAT_PROD'].sum() > 0)
        )
        
        return has_production
        
    def get_decline_curve_data(self, completion_names: List[str] = None) -> Dict:
        """
        Calculate decline curve parameters for oil production
        Returns dict with parameters and fitted curve
        """
        if self.data.empty:
            return {}
            
        df = self.get_monthly_oil_production(completion_names)
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
    
    def get_monthly_injection(self, completion_names: List[str] = None) -> pd.DataFrame:
        """Get monthly injection data for specified completions"""
        if self.data.empty:
            return pd.DataFrame()
            
        df = self.data.copy()
        
        if completion_names:
            df = df[df['COMPLETION_LEGAL_NAME'].isin(completion_names)]
            
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
    
    def is_well_active_in_december_2024(self, completion_name: str) -> bool:
        """
        Check if an injection well was active in December 2024
        Returns True if the well had injection data > 0 in Dec 2024
        """
        if self.data.empty:
            return False
            
        # Filter for the specific completion
        completion_data = self.data[self.data['COMPLETION_LEGAL_NAME'] == completion_name]
        
        # Check for December 2024 data
        dec_data = completion_data[
            (completion_data['Date'].dt.year == 2024) & 
            (completion_data['Date'].dt.month == 12)
        ]
        
        # Check if there's any injection
        if dec_data.empty:
            return False
            
        # Check if there's any water injection > 0
        has_injection = dec_data['Water_INJ_CALDAY'].sum() > 0
        
        return has_injection


class WellDataStore:
    """Central data store for well information and selection state"""
    
    def __init__(self):
        self.wells: Dict[str, Well] = {}
        self.production_data = ProductionData()
        self.injection_data = InjectionData()
        self.selected_wells: List[str] = []
        
        # New dictionary to track well to completions mapping
        self.well_to_completions: Dict[str, List[str]] = {}
        # New dictionary to track completions to reservoirs
        self.completion_to_reservoir: Dict[str, str] = {}
    
    def load_wells(self, wells_df, well_types_df):
        """
        Load wells from DataFrames
        Exclude wells that have 'PLA' in their name
        Store completion mapping information
        """
        for _, row in wells_df.iterrows():
            well_name = row['WELL_LEGAL_NAME']
            completion_name = row['COMPLETION_LEGAL_NAME']
            
            # Skip wells with "PLA" in their name
            if "PLA" in well_name:
                continue
                
            well = Well(
                well_name=well_name,
                completion_name=completion_name,
                x_coordinate=row['COMPLETION_COORDINATE_X'],
                y_coordinate=row['COMPLETION_COORDINATE_Y']
            )
            
            # Store the well
            self.wells[well.well_name] = well
            
            # Track completions for each well
            if well_name not in self.well_to_completions:
                self.well_to_completions[well_name] = []
            
            if completion_name not in self.well_to_completions[well_name]:
                self.well_to_completions[well_name].append(completion_name)
        
        # Add well type and reservoir information
        for _, row in well_types_df.iterrows():
            completion_name = row['COMPLETION_LEGAL_NAME']
            reservoir = row['RESERVORIO']
            
            # Store completion to reservoir mapping
            if completion_name and reservoir:
                self.completion_to_reservoir[completion_name] = reservoir
            
            for well in self.wells.values():
                if well.completion_name == completion_name:
                    well.well_type = row['TIPO_POZO']
                    well.reservoir = reservoir
    
    def load_production_data(self, prod_df):
        """Load production data"""
        # Filter out production data for excluded wells (those with "PLA" in their name)
        filtered_df = prod_df[~prod_df['COMP_S_NAME'].str.contains('PLA', na=False)]
        self.production_data.load_from_dataframe(filtered_df)
        
        # Update active status for production wells
        self.update_well_activity_status()
    
    def load_injection_data(self, inj_df):
        """Load injection data"""
        # Filter out injection data for excluded wells (those with "PLA" in their name)
        filtered_df = inj_df[~inj_df['COMPLETION_LEGAL_NAME'].str.contains('PLA', na=False)]
        self.injection_data.load_from_dataframe(filtered_df)
        
        # Update active status for injection wells
        self.update_well_activity_status()
    
    def update_well_activity_status(self):
        """
        Update well activity status based on December 2024 production/injection data
        """
        for well_name, well in self.wells.items():
            active = False
            
            # Get completions for this well
            completions = self.well_to_completions.get(well_name, [])
            
            if completions:
                if well.well_type and well.well_type.upper() == "INYECTOR":
                    # Check injection data for any completion of this well
                    for completion in completions:
                        if self.injection_data.is_well_active_in_december_2024(completion):
                            active = True
                            break
                else:
                    # Check production data for any completion of this well
                    for completion in completions:
                        if self.production_data.is_well_active_in_december_2024(completion):
                            active = True
                            break
            
            # Update well status
            well.active = active
    
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
    
    def get_completions_for_reservoirs(self, reservoirs: Set[str]) -> List[str]:
        """Get completion names for the specified reservoirs"""
        completions = []
        for completion, reservoir in self.completion_to_reservoir.items():
            if reservoir in reservoirs:
                completions.append(completion)
        return completions
    
    def get_wells_for_reservoirs(self, reservoirs: Set[str]) -> Set[str]:
        """Get well names that have completions in any of the specified reservoirs"""
        wells = set()
        for well_name, completions in self.well_to_completions.items():
            for completion in completions:
                reservoir = self.completion_to_reservoir.get(completion)
                if reservoir and reservoir in reservoirs:
                    wells.add(well_name)
                    break
        return wells
    
    def get_completions_for_selected_wells_and_reservoirs(self, reservoirs: Set[str] = None) -> List[str]:
        """
        Get completion names for selected wells filtered by reservoirs if specified
        """
        if not self.selected_wells:
            return []
        
        completions = []
        
        for well_name in self.selected_wells:
            if well_name in self.well_to_completions:
                for completion in self.well_to_completions[well_name]:
                    completion_reservoir = self.completion_to_reservoir.get(completion)
                    
                    # Include completion if no reservoir filter or if matches reservoir filter
                    if (not reservoirs) or (completion_reservoir and completion_reservoir in reservoirs):
                        completions.append(completion)
        
        return completions
    
    def get_production_for_selected(self, reservoirs: Set[str] = None) -> pd.DataFrame:
        """
        Get production data for selected wells, filtered by reservoirs if specified
        """
        completions = self.get_completions_for_selected_wells_and_reservoirs(reservoirs)
        
        if not completions:
            return pd.DataFrame()
        
        return self.production_data.get_monthly_oil_production(completions)
    
    def get_injection_for_selected(self, reservoirs: Set[str] = None) -> pd.DataFrame:
        """
        Get injection data for selected wells, filtered by reservoirs if specified
        """
        completions = self.get_completions_for_selected_wells_and_reservoirs(reservoirs)
        
        if not completions:
            return pd.DataFrame()
        
        return self.injection_data.get_monthly_injection(completions)