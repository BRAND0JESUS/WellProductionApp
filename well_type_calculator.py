import pandas as pd
import numpy as np
from datetime import datetime


class WellTypeCalculator:
    """
    Calculates well type (producer, injector, or dual) based on production and injection data.
    Optimized for performance and simplified to classify wells at well level only.
    
    Important rules:
    1. If a well has production history and currently has no production or injection,
       it remains a producer until injection data appears.
    2. If a well has injection history and currently has no production or injection,
       it remains an injector until production data appears.
    """
    
    def __init__(self, data_store):
        """
        Initialize with a reference to the data store
        
        data_store: WellDataStore object containing production and injection data
        """
        self.data_store = data_store
        # Pre-compute mappings once to avoid repeated lookups
        self._completion_to_well = self._build_completion_to_well_map()
        
    def _build_completion_to_well_map(self):
        """Build completion to well mapping once for reuse"""
        completion_to_well = {}
        for well_name, completions in self.data_store.well_to_completions.items():
            for completion in completions:
                completion_to_well[completion] = well_name
        return completion_to_well
    
    def calculate_monthly_well_types(self):
        """
        Calculate monthly well type (PRODUCTION, INJECTION, or DUAL) for all wells
        based on their production and injection data.
        
        Returns: DataFrame with columns well_name, year, month, well_type, 
                 oil_rate, water_rate, water_inj_rate
        """
        # Process production data
        prod_monthly = self._process_production_data()
        
        # Process injection data
        inj_monthly = self._process_injection_data()
        
        # Get the historical well types (cumulative production and injection)
        historical_well_types = self._calculate_historical_well_types(prod_monthly, inj_monthly)
        
        # Combine the data to identify dual function wells
        combined = self._combine_and_classify_data(prod_monthly, inj_monthly, historical_well_types)
        
        # Filter out DUAL classification for compatibility with existing code
        # Reclassify as either PRODUCTION or INJECTION based on which has higher rate
        # We'll keep has_dual_function=1 for easy identification
        if 'well_type' in combined.columns and not combined.empty:
            # For DUAL wells, determine dominant type
            mask = combined['well_type'] == 'DUAL'
            if mask.any():
                combined.loc[mask, 'well_type'] = combined[mask].apply(
                    lambda row: 'PRODUCTION' if (row['oil_rate'] + row['water_rate'] >= row['water_inj_rate']) 
                                else 'INJECTION', 
                    axis=1
                )
        
        # Ensure all required columns exist
        if not combined.empty:
            # Make sure output has standard columns in proper order
            result_columns = ['well_name', 'year', 'month', 'well_type', 
                             'oil_rate', 'water_rate', 'water_inj_rate']
            
            # Keep has_dual_function but make sure it doesn't disrupt existing code
            if 'has_dual_function' in combined.columns:
                result_columns.append('has_dual_function')
                
            if 'remarks' in combined.columns:
                result_columns.append('remarks')
                
            # Reorder columns and ensure all required ones exist
            for col in result_columns:
                if col not in combined.columns:
                    combined[col] = 0 if col in ('oil_rate', 'water_rate', 'water_inj_rate') else ''
                    
            combined = combined[result_columns]
        
        return combined
    
    def _process_production_data(self):
        """Process production data to monthly format by well"""
        # Get production data
        prod_data = self.data_store.production_data.data.copy()
        
        if prod_data.empty:
            return pd.DataFrame(columns=['well_name', 'year', 'month', 'oil_rate', 'water_rate'])
        
        # Add well_name column based on completion name
        prod_data['well_name'] = prod_data['COMP_S_NAME'].map(self._completion_to_well)
        
        # Drop rows where well_name is null (unknown completions)
        prod_data = prod_data[prod_data['well_name'].notna()]
        
        # Extract year and month
        prod_data['year'] = prod_data['PROD_DT'].dt.year
        prod_data['month'] = prod_data['PROD_DT'].dt.month
        
        # Calculate daily rates
        days_in_month = prod_data['PROD_DT'].dt.daysinmonth
        prod_data['oil_rate'] = prod_data['VO_OIL_PROD'] / days_in_month
        prod_data['water_rate'] = prod_data['VO_WAT_PROD'] / days_in_month
        
        # Group by well, year, month and sum rates
        prod_monthly = prod_data.groupby(['well_name', 'year', 'month']).agg({
            'oil_rate': 'sum',
            'water_rate': 'sum'
        }).reset_index()
        
        return prod_monthly
    
    def _process_injection_data(self):
        """Process injection data to monthly format by well"""
        # Get injection data
        inj_data = self.data_store.injection_data.data.copy()
        
        if inj_data.empty:
            return pd.DataFrame(columns=['well_name', 'year', 'month', 'water_inj_rate'])
        
        # Add well_name column based on completion name
        inj_data['well_name'] = inj_data['COMPLETION_LEGAL_NAME'].map(self._completion_to_well)
        
        # Drop rows where well_name is null (unknown completions)
        inj_data = inj_data[inj_data['well_name'].notna()]
        
        # Extract year and month
        inj_data['year'] = inj_data['Date'].dt.year
        inj_data['month'] = inj_data['Date'].dt.month
        
        # Calculate daily rates
        days_in_month = inj_data['Date'].dt.daysinmonth
        inj_data['water_inj_rate'] = inj_data['Water_INJ_CALDAY'] / days_in_month
        
        # Group by well, year, month and sum rates
        inj_monthly = inj_data.groupby(['well_name', 'year', 'month']).agg({
            'water_inj_rate': 'sum'
        }).reset_index()
        
        return inj_monthly
        
    def _calculate_historical_well_types(self, prod_monthly, inj_monthly):
        """
        Calculate historical well types - if a well ever had production or injection
        
        Returns: Dictionary mapping well_name to {'has_production_history': bool, 'has_injection_history': bool}
        """
        # Initialize empty dictionary
        historical_types = {}
        
        # Get all well names from both production and injection data
        all_wells = set()
        if not prod_monthly.empty:
            all_wells.update(prod_monthly['well_name'].unique())
        if not inj_monthly.empty:
            all_wells.update(inj_monthly['well_name'].unique())
        
        # Initialize all wells with no history
        for well in all_wells:
            historical_types[well] = {
                'has_production_history': False,
                'has_injection_history': False
            }
        
        # Mark wells with production history
        if not prod_monthly.empty:
            # Calculate total production for each well
            prod_totals = prod_monthly.groupby('well_name').agg({
                'oil_rate': 'sum',
                'water_rate': 'sum'
            })
            
            # Mark wells with production > 0
            for well, row in prod_totals.iterrows():
                if row['oil_rate'] > 0 or row['water_rate'] > 0:
                    if well in historical_types:
                        historical_types[well]['has_production_history'] = True
        
        # Mark wells with injection history
        if not inj_monthly.empty:
            # Calculate total injection for each well
            inj_totals = inj_monthly.groupby('well_name').agg({
                'water_inj_rate': 'sum'
            })
            
            # Mark wells with injection > 0
            for well, row in inj_totals.iterrows():
                if row['water_inj_rate'] > 0:
                    if well in historical_types:
                        historical_types[well]['has_injection_history'] = True
        
        return historical_types
    
    def _combine_and_classify_data(self, prod_monthly, inj_monthly, historical_well_types):
        """
        Combine production and injection data and classify wells as
        PRODUCTION, INJECTION, or DUAL each month
        
        Takes into account historical well types to maintain classification
        when there is no current data
        """
        # If both dataframes are empty, return empty result
        if prod_monthly.empty and inj_monthly.empty:
            return pd.DataFrame(columns=[
                'well_name', 'year', 'month', 'well_type', 
                'oil_rate', 'water_rate', 'water_inj_rate', 'has_dual_function'
            ])
            
        # If one is empty but not the other
        if prod_monthly.empty:
            result = inj_monthly.copy()
            result['oil_rate'] = 0.0
            result['water_rate'] = 0.0
            result['well_type'] = 'INJECTION'
            result['has_dual_function'] = 0
            return result.sort_values(['well_name', 'year', 'month']).reset_index(drop=True)
            
        if inj_monthly.empty:
            result = prod_monthly.copy()
            result['water_inj_rate'] = 0.0
            result['well_type'] = 'PRODUCTION'
            result['has_dual_function'] = 0
            return result.sort_values(['well_name', 'year', 'month']).reset_index(drop=True)
        
        # Merge production and injection data on well_name, year, month
        merged = pd.merge(
            prod_monthly, 
            inj_monthly, 
            on=['well_name', 'year', 'month'], 
            how='outer'
        )
        
        # Fill NaN values with 0
        merged.fillna({
            'oil_rate': 0.0,
            'water_rate': 0.0,
            'water_inj_rate': 0.0
        }, inplace=True)
        
        # Vectorized classification for better performance
        # Calculate has_prod and has_inj as boolean masks (current month)
        has_current_prod = (merged['oil_rate'] > 0) | (merged['water_rate'] > 0)
        has_current_inj = merged['water_inj_rate'] > 0
        
        # Initialize with default values
        merged['well_type'] = 'UNKNOWN'
        merged['has_dual_function'] = 0
        
        # CASE 1: Wells with both production and injection in the current month
        # These are always DUAL regardless of history
        merged.loc[has_current_prod & has_current_inj, 'well_type'] = 'DUAL'
        merged.loc[has_current_prod & has_current_inj, 'has_dual_function'] = 1
        
        # CASE 2: Wells with only production in the current month
        merged.loc[has_current_prod & ~has_current_inj, 'well_type'] = 'PRODUCTION'
        
        # CASE 3: Wells with only injection in the current month
        merged.loc[~has_current_prod & has_current_inj, 'well_type'] = 'INJECTION'
        
        # CASE 4: Wells with neither production nor injection in the current month
        # These need special handling based on history
        
        # Create a mask for wells with no current production or injection
        no_current_data = ~has_current_prod & ~has_current_inj
        
        if no_current_data.any():
            # For each well in this category, check its history
            for well_name in merged.loc[no_current_data, 'well_name'].unique():
                if well_name in historical_well_types:
                    history = historical_well_types[well_name]
                    
                    # Apply the business rule:
                    # If well has production history and no injection history, keep as PRODUCTION
                    if history['has_production_history'] and not history['has_injection_history']:
                        well_rows = (merged['well_name'] == well_name) & no_current_data
                        merged.loc[well_rows, 'well_type'] = 'PRODUCTION'
                    
                    # If well has injection history and no production history, keep as INJECTION
                    elif not history['has_production_history'] and history['has_injection_history']:
                        well_rows = (merged['well_name'] == well_name) & no_current_data
                        merged.loc[well_rows, 'well_type'] = 'INJECTION'
                    
                    # If well has both production and injection history, determine based on which is more recent
                    # This would require additional data processing - for now, default to PRODUCTION
                    elif history['has_production_history'] and history['has_injection_history']:
                        well_rows = (merged['well_name'] == well_name) & no_current_data
                        merged.loc[well_rows, 'well_type'] = 'PRODUCTION'
        
        # Sort the results
        result = merged.sort_values(['well_name', 'year', 'month']).reset_index(drop=True)
        
        # Add remarks
        result['remarks'] = ''
        
        # Add remarks for dual function wells
        dual_wells = result['has_dual_function'] == 1
        if dual_wells.any():
            # Just for dual wells - more efficient than running apply on all rows
            dual_df = result[dual_wells].copy()
            remarks = (
                "Dual function well: Production rate = " + 
                (dual_df['oil_rate'] + dual_df['water_rate']).round(2).astype(str) + 
                " bbl/d, Injection rate = " + 
                dual_df['water_inj_rate'].round(2).astype(str) + 
                " bbl/d"
            )
            result.loc[dual_wells, 'remarks'] = remarks.values
        
        return result
    
    def calculate_reservoir_well_types(self):
        """
        Modified to just return the monthly well types with a dummy reservoir column
        to maintain API compatibility with the existing code.
        No actual reservoir-specific calculations are performed.
        
        Returns: DataFrame with columns well_name, reservoir, year, month, well_type,
                oil_rate, water_rate, water_inj_rate
        """
        # Get monthly types first
        monthly_types = self.calculate_monthly_well_types()
        
        if monthly_types.empty:
            # Return empty DataFrame with the correct columns
            return pd.DataFrame(columns=[
                'well_name', 'reservoir', 'year', 'month', 'well_type',
                'oil_rate', 'water_rate', 'water_inj_rate'
            ])
        
        # Add 'UNKNOWN' reservoir column to all rows for compatibility
        monthly_types['reservoir'] = 'UNKNOWN'
        
        # Ensure proper column order for compatibility
        result_columns = [
            'well_name', 'reservoir', 'year', 'month', 'well_type',
            'oil_rate', 'water_rate', 'water_inj_rate'
        ]
        
        # Add any extra columns that exist in monthly_types
        for col in monthly_types.columns:
            if col not in result_columns and col != 'well_month_id':  # Exclude temporary columns
                result_columns.append(col)
                
        # Reorder and return
        result = monthly_types[result_columns].copy()
        
        return result
    
    def calculate_overall_well_types(self, monthly_types_df):
        """
        Simplified version that just returns the monthly types with the required columns
        for compatibility with existing code.
        
        monthly_types_df: DataFrame with well types
        
        Returns: DataFrame with columns well_name, year, month, well_type, 
                primary_type, secondary_type, has_dual_function, remarks
        """
        if monthly_types_df.empty:
            # Return empty DataFrame with the correct columns
            return pd.DataFrame(columns=[
                'well_name', 'year', 'month', 'well_type', 
                'primary_type', 'secondary_type', 'has_dual_function', 'remarks'
            ])
        
        # Create a copy of the input DataFrame
        result = monthly_types_df.copy()
        
        # If the 'reservoir' column exists, drop it since we don't need it
        if 'reservoir' in result.columns:
            result = result.drop('reservoir', axis=1)
        
        # Add required columns if they don't exist
        if 'has_dual_function' not in result.columns:
            result['has_dual_function'] = 0
            # Identify dual function wells
            dual_mask = (result['oil_rate'] > 0) & (result['water_rate'] > 0) & (result['water_inj_rate'] > 0)
            result.loc[dual_mask, 'has_dual_function'] = 1
        
        if 'primary_type' not in result.columns:
            result['primary_type'] = result['well_type']
        
        if 'secondary_type' not in result.columns:
            result['secondary_type'] = 'NONE'
            # Update secondary type for dual wells
            dual_mask = result['has_dual_function'] == 1
            if dual_mask.any():
                is_production_primary = result['well_type'] == 'PRODUCTION'
                result.loc[dual_mask & is_production_primary, 'secondary_type'] = 'INJECTION'
                result.loc[dual_mask & ~is_production_primary, 'secondary_type'] = 'PRODUCTION'
        
        if 'remarks' not in result.columns:
            result['remarks'] = ''
            # Add remarks for different well types
            dual_mask = result['has_dual_function'] == 1
            prod_mask = (result['well_type'] == 'PRODUCTION') & ~dual_mask
            inj_mask = (result['well_type'] == 'INJECTION') & ~dual_mask
            
            # Remarks for dual wells
            if dual_mask.any():
                dual_df = result[dual_mask]
                dual_remarks = (
                    "Dual function well. Total production: " + 
                    (dual_df['oil_rate'] + dual_df['water_rate']).round(2).astype(str) + 
                    " bbl/d, Total injection: " + 
                    dual_df['water_inj_rate'].round(2).astype(str) + 
                    " bbl/d."
                )
                result.loc[dual_mask, 'remarks'] = dual_remarks.values
            
            # Remarks for production wells
            if prod_mask.any():
                prod_df = result[prod_mask]
                prod_remarks = (
                    "Producing well. Oil rate: " + 
                    prod_df['oil_rate'].round(2).astype(str) + 
                    " bbl/d, Water rate: " + 
                    prod_df['water_rate'].round(2).astype(str) + 
                    " bbl/d."
                )
                result.loc[prod_mask, 'remarks'] = prod_remarks.values
            
            # Remarks for injection wells
            if inj_mask.any():
                inj_df = result[inj_mask]
                inj_remarks = (
                    "Injection well. Injection rate: " + 
                    inj_df['water_inj_rate'].round(2).astype(str) + 
                    " bbl/d."
                )
                result.loc[inj_mask, 'remarks'] = inj_remarks.values
        
        # Ensure we have all required columns and in the right order
        columns_to_keep = [
            'well_name', 'year', 'month', 'well_type', 
            'primary_type', 'secondary_type', 'has_dual_function', 'remarks'
        ]
        
        # Add oil_rate, water_rate, and water_inj_rate if they exist
        for col in ['oil_rate', 'water_rate', 'water_inj_rate']:
            if col in result.columns:
                columns_to_keep.append(col)
        
        # Filter to only keep the required columns
        result = result[columns_to_keep]
        
        # Sort the final result
        return result.sort_values(['well_name', 'year', 'month']).reset_index(drop=True)