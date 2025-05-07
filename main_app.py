import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QPushButton, QLineEdit, 
                             QSplitter, QMessageBox, QStatusBar, QCheckBox,
                             QMenu, QAction, QDialog, QMenuBar)
from PyQt5.QtCore import Qt, QSize, QThread, pyqtSignal
import json
import pandas as pd
from datetime import datetime

from database_manager import DatabaseManager
from models import Well, WellDataStore
from map_widget import WellMapWidget
from chart_widgets import ProductionProfileChart, InjectionProfileChart

# Import custom modules
from operations_database import OperationsDatabase
from well_type_calculator import WellTypeCalculator
from operation_dialogs import (OperationProgressDialog, OperationResultsDialog, 
                             WellTypeOperationDialog, OperationListDialog,
                             CompletionStateOperationDialog, CompletionStateResultsDialog)


# Class to handle operations in the background
class OperationWorker(QThread):
    """Worker thread to perform operations in the background"""
    
    # Signals
    progress_updated = pyqtSignal(int, str)
    operation_completed = pyqtSignal(bool, object)
    
    def __init__(self, operation_type, data_store, options=None):
        super().__init__()
        self.operation_type = operation_type
        self.data_store = data_store
        self.options = options or {}
        self.results = None
        self.error = None
        self.calculator = None
    

def run(self):
    """Run the operation"""
    try:
        # Initialize well type calculator
        self.progress_updated.emit(10, "Initializing well type calculator...")
        self.calculator = WellTypeCalculator(self.data_store)
        
        # Run operation based on type
        if self.operation_type == "well_monthly_type":
            self.calculate_well_monthly_types()
        elif self.operation_type == "completion_state":
            self.calculate_completion_states()
        else:
            raise ValueError(f"Unknown operation type: {self.operation_type}")
            
        # Operation completed successfully
        self.operation_completed.emit(True, self.results)
        
    except Exception as e:
        # Operation failed
        import traceback
        self.error = f"{str(e)}\n{traceback.format_exc()}"
        self.progress_updated.emit(0, f"Error: {str(e)}")
        self.operation_completed.emit(False, self.error)

    def calculate_completion_states(self):
        """Calculate completion states by reservoir with improved error handling"""
        try:
            self.progress_updated.emit(20, "Starting completion state calculation...")
            
            # Calculate completion states with progress updates forwarded
            # Add progress callback to the calculator
            self.calculator.progress_updated = self.progress_updated
            
            completion_status = self.calculator.calculate_completion_status()
            
            self.progress_updated.emit(80, "Processing results...")
            
            # Apply date filtering if specified in options
            if self.options.get('use_date_range', False):
                try:
                    start_date = pd.to_datetime(self.options.get('start_date'))
                    end_date = pd.to_datetime(self.options.get('end_date'))
                    
                    # Filter by date range
                    self.progress_updated.emit(85, f"Filtering data to date range {start_date.date()} to {end_date.date()}...")
                    
                    # Create a timestamp for filtering
                    completion_status['date'] = pd.to_datetime(
                        completion_status['year'].astype(str) + '-' + 
                        completion_status['month'].astype(str) + '-01'
                    )
                    
                    # Apply filter
                    completion_status = completion_status[
                        (completion_status['date'] >= start_date) & 
                        (completion_status['date'] <= end_date)
                    ]
                    
                    # Remove the temporary date column
                    completion_status = completion_status.drop('date', axis=1)
                except Exception as e:
                    self.progress_updated.emit(85, f"Warning: Could not apply date filter: {str(e)}")
            
            # Generate summary metrics
            record_count = len(completion_status) if completion_status is not None else 0
            completion_count = completion_status['completion_name'].nunique() if not completion_status.empty else 0
            
            self.progress_updated.emit(90, f"Successfully processed {record_count} records for {completion_count} completions...")
            
            self.results = {
                'completion_status': completion_status
            }
        except Exception as e:
            self.progress_updated.emit(20, f"Error calculating completion states: {str(e)}")
            raise

    def calculate_well_monthly_types(self):
        """Calculate well types by month"""
        self.progress_updated.emit(20, "Calculating monthly well types...")
        
        try:
            monthly_types = self.calculator.calculate_monthly_well_types()
            self.progress_updated.emit(90, f"Successfully processed {len(monthly_types)} monthly well records...")
            
            self.results = {
                'monthly_types': monthly_types
            }
        except Exception as e:
            self.progress_updated.emit(20, f"Error calculating monthly well types: {str(e)}")
            raise

class WellProductionApp(QMainWindow):
    """Main application window for Well Production App"""
    
    def __init__(self):
        super().__init__()
        
        # Initialize data managers
        self.db_manager = DatabaseManager()
        self.data_store = WellDataStore()
        
        # Initialize operations database
        self.operations_db = OperationsDatabase()
        
        # Set up UI
        self.setup_ui()
        
        # Set up menu
        self.setup_menu()
        
        # Connect to database and load data
        self.load_data()
        
        # Connect to operations database
        self.init_operations_db()
        
        # Set window title and size
        self.setWindowTitle("WellProductionApp")
        self.resize(1200, 800)
    
    def setup_ui(self):
        """Set up the user interface"""
        # Central widget
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Main layout - make it an instance variable so it's accessible everywhere
        self.main_layout = QVBoxLayout(self.central_widget)
        
        # Header with reservoir selection buttons
        header_layout = QHBoxLayout()
        
        # Reservoir buttons container
        self.reservoir_buttons_layout = QHBoxLayout()
        self.reservoir_buttons_layout.setSpacing(5)
        
        # Add label for reservoir selection
        reservoir_label = QLabel("Rerervoir:")
        reservoir_label.setStyleSheet("font-weight: bold;")
        self.reservoir_buttons_layout.addWidget(reservoir_label)
        
        # We'll populate these buttons after loading data
        self.reservoir_buttons = {}
        self.selected_reservoirs = set()  # Track selected reservoirs
        
        header_layout.addLayout(self.reservoir_buttons_layout)
        
        # Search field
        search_layout = QHBoxLayout()
        self.search_field = QLineEdit()
        self.search_field.setPlaceholderText("Search and select wells...")
        self.search_field.textChanged.connect(self.filter_wells)
        search_layout.addWidget(self.search_field)
        
        header_layout.addLayout(search_layout)
        header_layout.setStretchFactor(self.reservoir_buttons_layout, 2)
        header_layout.setStretchFactor(search_layout, 1)
        
        self.main_layout.addLayout(header_layout)
        
        # Main content splitter (map and charts)
        self.splitter = QSplitter(Qt.Horizontal)
        
        # Map widget on the left
        self.map_widget = WellMapWidget()
        self.map_widget.wellClicked.connect(self.well_selected)
        self.map_widget.wellsSelected.connect(self.wells_selected)
        
        # Charts container on the right
        charts_container = QWidget()
        charts_layout = QVBoxLayout(charts_container)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        
        # Production profile chart
        self.production_chart = ProductionProfileChart()
        charts_layout.addWidget(self.production_chart)
        
        # Injection profile chart
        self.injection_chart = InjectionProfileChart()
        charts_layout.addWidget(self.injection_chart)
        
        # Add widgets to splitter
        self.splitter.addWidget(self.map_widget)
        self.splitter.addWidget(charts_container)
        
        # Set initial sizes
        self.splitter.setSizes([400, 800])
        
        self.main_layout.addWidget(self.splitter)
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Action buttons
        button_layout = QHBoxLayout()
        
        # Selection mode checkbox
        self.multi_select_checkbox = QCheckBox("Multi-selection Mode")
        self.multi_select_checkbox.setToolTip("Enable to select multiple wells without holding Ctrl")
        self.multi_select_checkbox.stateChanged.connect(self.toggle_multi_selection_mode)
        button_layout.addWidget(self.multi_select_checkbox)
        
        # Select all visible wells button
        self.select_all_button = QPushButton("Select All Visible")
        self.select_all_button.clicked.connect(self.select_all_visible_wells)
        button_layout.addWidget(self.select_all_button)
        
        # Clear selection button
        self.clear_button = QPushButton("Clear Selection")
        self.clear_button.clicked.connect(self.clear_selection)
        button_layout.addWidget(self.clear_button)
        
        # Add button to reset reservoir filters
        self.reset_filters_button = QPushButton("Reset Filters")
        self.reset_filters_button.clicked.connect(self.reset_reservoir_filters)
        button_layout.addWidget(self.reset_filters_button)
        
        # Selection help label
        selection_help = QLabel("Tip: Hold Ctrl+click to select multiple wells. Shift+drag to select wells in a box.")
        selection_help.setStyleSheet("font-style: italic; color: #555;")
        
        # Add button layout to main layout
        self.main_layout.addLayout(button_layout)
        self.main_layout.addWidget(selection_help)
    
    def setup_menu(self):
        """Set up the application menu bar"""
        # Create menu bar
        menubar = self.menuBar()
        
        # Create menus
        file_menu = menubar.addMenu("&File")
        operations_menu = menubar.addMenu("&Operations")
        help_menu = menubar.addMenu("&Help")
        
        # File menu actions
        exit_action = QAction("E&xit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.setStatusTip("Exit the application")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Operations menu actions
        well_type_action = QAction("Calculate &Well Types", self)
        well_type_action.setStatusTip("Calculate well types (producer/injector) over time")
        well_type_action.triggered.connect(self.run_well_type_operation)
        operations_menu.addAction(well_type_action)
        
        # Add new action for completion state calculation
        completion_state_action = QAction("Calculate &Completion States", self)
        completion_state_action.setStatusTip("Calculate completion states by reservoir")
        completion_state_action.triggered.connect(self.run_completion_state_operation)
        operations_menu.addAction(completion_state_action)
        
        operations_menu.addSeparator()
        
        view_operations_action = QAction("View Previous &Operations", self)
        view_operations_action.setStatusTip("View and manage previous operations")
        view_operations_action.triggered.connect(self.view_operations)
        operations_menu.addAction(view_operations_action)
        
        # Help menu actions
        about_action = QAction("&About", self)
        about_action.setStatusTip("Show information about the application")
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def load_data(self):
        """
        Connect to database and load well data
        Modified to ensure only one point per well is added to the map
        """
        # Connect to database
        if not self.db_manager.connect():
            QMessageBox.critical(self, "Database Error", 
                            "Failed to connect to the database. Please check the database file location.")
            return
        
        # Load well locations
        wells_df = self.db_manager.get_well_locations()
        well_types_df = self.db_manager.get_well_types()
        
        if wells_df.empty:
            QMessageBox.warning(self, "Data Error", "No well data found in the database.")
            return
        
        # Load wells into data store
        self.data_store.load_wells(wells_df, well_types_df)
        
        # Load production and injection data
        prod_df = self.db_manager.get_production_data()
        inj_df = self.db_manager.get_injection_data()
        self.data_store.load_production_data(prod_df)
        self.data_store.load_injection_data(inj_df)
        
        # Add wells to map - one per well name, not per completion
        # Modified to ensure only one point per well is added to the map
        for well_name, well in self.data_store.wells.items():
            # Add well with proper type and active status
            self.map_widget.add_well(well_name, well.x_coordinate, well.y_coordinate, 
                                well.well_type, well.active)
        
        # Update well reservoir status for display in map
        self.update_well_reservoir_statuses()
        
        # Create reservoir buttons
        self.create_reservoir_buttons()
        
        # Disconnect from database
        self.db_manager.disconnect()
        
        # Update status
        well_count = len(self.data_store.wells)
        active_wells = sum(1 for well in self.data_store.wells.values() if well.active)
        inactive_wells = well_count - active_wells
        
        # Count injector wells vs producer wells
        inj_wells = sum(1 for well in self.data_store.wells.values() 
                    if well.well_type == "INJECTION")
        prod_wells = well_count - inj_wells
        
        self.status_bar.showMessage(
            f"Loaded {well_count} wells from database " +
            f"({active_wells} active, {inactive_wells} inactive), " +
            f"({prod_wells} producers, {inj_wells} injectors) as of Dec 2024"
        )
    
    def init_operations_db(self):
        """Initialize the operations database"""
        self.operations_db = OperationsDatabase()
        if not self.operations_db.connect():
            QMessageBox.warning(self, "Database Warning", 
                              "Failed to connect to the operations database. "
                              "Some features may not be available.")

    def update_well_reservoir_statuses(self):
        """
        Process well completion and activity data by reservoir
        Modified to update well status by reservoir for a single well point
        """
        # Initialize data for all wells and their reservoirs
        for well_name in self.data_store.wells:
            # Get all completions for this well
            if well_name in self.data_store.well_to_completions:
                completions = self.data_store.well_to_completions[well_name]
                
                # For each completion, check which reservoir it belongs to
                for completion in completions:
                    reservoir = self.data_store.completion_to_reservoir.get(completion)
                    
                    if reservoir:
                        # Determine activity status based on well type and data
                        is_production = self.data_store.wells[well_name].well_type == "PRODUCTION"
                        
                        if is_production:
                            # For production wells, check activity in this completion
                            is_active = self.data_store.production_data.is_well_active_in_december_2024(completion)
                            well_type = "PRODUCTION"
                        else:
                            # For injection wells, check activity in this completion
                            is_active = self.data_store.injection_data.is_well_active_in_december_2024(completion)
                            well_type = "INJECTION"
                        
                        # Update the map widget's tracking of well-reservoir status
                        # Including the well_type for this completion
                        self.map_widget.update_well_reservoir_status(
                            well_name=well_name,
                            reservoir=reservoir,
                            has_completion=True,
                            active=is_active,
                            well_type=well_type
                        )
    
    def create_reservoir_buttons(self):
        """Create buttons for each unique reservoir"""
        # Get unique reservoirs
        reservoirs = set()
        
        # Use the completion_to_reservoir mapping to get all unique reservoirs
        for reservoir in self.data_store.completion_to_reservoir.values():
            if reservoir and reservoir.strip():
                reservoirs.add(reservoir)
        
        # Add 'All' button first
        all_button = QPushButton("All")
        all_button.setCheckable(True)
        all_button.setChecked(True)
        all_button.clicked.connect(self.toggle_all_reservoirs)
        self.reservoir_buttons['all'] = all_button
        self.reservoir_buttons_layout.addWidget(all_button)
        
        # Add a button for each reservoir
        for reservoir in sorted(reservoirs):
            button = QPushButton(reservoir)
            button.setCheckable(True)
            # Important: Using lambda with default argument to avoid late binding issues
            button.clicked.connect(lambda checked, res=reservoir: self.toggle_reservoir(res, checked))
            self.reservoir_buttons[reservoir] = button
            self.reservoir_buttons_layout.addWidget(button)
    
    def toggle_reservoir(self, reservoir_name, checked):
        """Toggle selection of a reservoir"""
        # Handle the "All" button separately
        if reservoir_name == 'all':
            return
            
        # Update selected reservoirs set
        if checked:
            self.selected_reservoirs.add(reservoir_name)
            
            # If any reservoir is selected, uncheck the "All" button
            if self.reservoir_buttons['all'].isChecked():
                self.reservoir_buttons['all'].setChecked(False)
        else:
            if reservoir_name in self.selected_reservoirs:
                self.selected_reservoirs.remove(reservoir_name)
            
            # If no reservoirs are selected, check the "All" button
            if not self.selected_reservoirs:
                self.reservoir_buttons['all'].setChecked(True)
        
        # Update well visibility
        self.update_well_visibility()
        
        # Clear current well selection when reservoir filter changes
        self.clear_selection()
        
        # Update map widget with selected reservoirs for coloring
        self.map_widget.set_selected_reservoirs(self.selected_reservoirs)
        self.map_widget.set_all_reservoirs_button_state(self.reservoir_buttons['all'].isChecked())
    
    def toggle_all_reservoirs(self, checked):
        """Handle clicking the 'All' button"""
        if checked:
            # Uncheck all other reservoir buttons
            for res, button in self.reservoir_buttons.items():
                if res != 'all':
                    button.setChecked(False)
            
            # Clear selected reservoirs set
            self.selected_reservoirs.clear()
            
            # Show all wells
            for well_name in self.data_store.wells:
                self.map_widget.set_well_visibility(well_name, True)
        else:
            # If unchecking "All" but no specific reservoirs are selected,
            # keep "All" checked
            if not self.selected_reservoirs:
                self.reservoir_buttons['all'].setChecked(True)
        
        # Update well visibility
        self.update_well_visibility()
        
        # Update map widget with selected reservoirs for coloring
        self.map_widget.set_selected_reservoirs(self.selected_reservoirs)
        self.map_widget.set_all_reservoirs_button_state(self.reservoir_buttons['all'].isChecked())
        
        # Update status
        well_count = len(self.data_store.wells)
        prod_wells = sum(1 for well in self.data_store.wells.values() 
                        if well.well_type == "PRODUCTION")
        inj_wells = well_count - prod_wells
        self.status_bar.showMessage(f"Showing all {well_count} wells ({prod_wells} producers, {inj_wells} injectors)")
    
    def update_well_visibility(self):
        """
        Update visibility of wells based on selected reservoirs
        """
        # If "All" is selected, show all wells
        if self.reservoir_buttons['all'].isChecked():
            for well_name in self.data_store.wells:
                self.map_widget.set_well_visibility(well_name, True)
            return
        
        # If no reservoirs are selected, don't hide any wells
        if not self.selected_reservoirs:
            for well_name in self.data_store.wells:
                self.map_widget.set_well_visibility(well_name, True)
            return
        
        # Keep all wells visible, regardless of selected reservoirs
        for well_name in self.data_store.wells:
            self.map_widget.set_well_visibility(well_name, True)
        
        # Get wells that have completions in the selected reservoirs
        # to count them and display statistics in the status bar
        wells_with_completions = self.data_store.get_wells_for_reservoirs(self.selected_reservoirs)
        
        # Update the map
        self.map_widget.update()
        
        # Count producers and injectors in wells with completions in the selected reservoirs
        prod_count = sum(1 for well_name in wells_with_completions 
                       if self.data_store.wells[well_name].well_type == "PRODUCTION")
        inj_count = len(wells_with_completions) - prod_count
        
        # Update status bar        
        if len(self.selected_reservoirs) == 1:
            reservoir = next(iter(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing all wells. {len(wells_with_completions)} wells have completions in reservoir {reservoir} " +
                f"({prod_count} producers, {inj_count} injectors)"
            )
        else:
            reservoirs_str = ", ".join(sorted(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing all wells. {len(wells_with_completions)} wells have completions in reservoirs: {reservoirs_str} " +
                f"({prod_count} producers, {inj_count} injectors)"
            )
    
    def reset_reservoir_filters(self):
        """Reset all reservoir filters to show all wells"""
        # Reset selected reservoirs
        self.selected_reservoirs.clear()
        
        # Update button states
        for res, button in self.reservoir_buttons.items():
            if res == 'all':
                button.setChecked(True)
            else:
                button.setChecked(False)
        
        # Show all wells
        for well_name in self.data_store.wells:
            self.map_widget.set_well_visibility(well_name, True)
        
        # Update map widget selected reservoirs
        self.map_widget.set_selected_reservoirs(set())
        self.map_widget.set_all_reservoirs_button_state(True)
        
        # Update map
        self.map_widget.update()
        
        # Clear well selection
        self.clear_selection()
        
        # Update status
        well_count = len(self.data_store.wells)
        prod_wells = sum(1 for well in self.data_store.wells.values() 
                        if well.well_type == "PRODUCTION")
        inj_wells = well_count - prod_wells
        self.status_bar.showMessage(f"Showing all {well_count} wells ({prod_wells} producers, {inj_wells} injectors)")
    
    def well_selected(self, well_name):
        """Handle well selection event from map"""
        # Check if in multi-selection mode
        if self.multi_select_checkbox.isChecked():
            # Toggle selection in data store
            self.data_store.toggle_well_selection(well_name)
        else:
            # If not holding Ctrl, clear other selections first
            if not QApplication.keyboardModifiers() & Qt.ControlModifier:
                self.data_store.clear_selection()
            
            # Select the well
            if not self.data_store.is_well_selected(well_name):
                self.data_store.select_well(well_name)
            else:
                # If already selected and Ctrl is pressed, deselect it
                if QApplication.keyboardModifiers() & Qt.ControlModifier:
                    self.data_store.deselect_well(well_name)
        
        # Update charts with selected wells data
        self.update_charts()
    
    def wells_selected(self, well_names):
        """Handle multiple well selection"""
        # If not in multi-selection mode and not holding Ctrl, clear previous selection
        if not self.multi_select_checkbox.isChecked() and not QApplication.keyboardModifiers() & Qt.ControlModifier:
            self.data_store.clear_selection()
        
        # Update data store with selected wells
        for name in well_names:
            self.data_store.select_well(name)
        
        # Update charts
        self.update_charts()
    
    def update_charts(self):
        # Get selected well names for title
        selected_wells = self.data_store.get_selected_wells()
        well_names = [well.well_name for well in selected_wells]
        
        # Determine which reservoirs to filter by
        reservoirs_filter = None if self.reservoir_buttons['all'].isChecked() else self.selected_reservoirs
        
        # Get production and injection data for selected wells, filtered by reservoirs
        prod_data = self.data_store.get_production_for_selected(reservoirs_filter)
        inj_data = self.data_store.get_injection_for_selected(reservoirs_filter)
        
        # Update chart titles to include reservoir info if filtering
        if reservoirs_filter and selected_wells:
            if len(reservoirs_filter) == 1:
                reservoir_name = next(iter(reservoirs_filter))
                if len(selected_wells) == 1:
                    # For a single well, also show its completions for that reservoir
                    well = selected_wells[0]
                    completations = []
                    for comp in self.data_store.well_to_completions.get(well.well_name, []):
                        comp_reservoir = self.data_store.completion_to_reservoir.get(comp)
                        if comp_reservoir == reservoir_name:
                            completations.append(comp)
                    
                    if completations:
                        well_title = f"{well.well_name} [{', '.join(completations)}] ({reservoir_name})"
                    else:
                        well_title = f"{well.well_name} ({reservoir_name})"
                else:
                    well_title = f"{len(selected_wells)} Wells ({reservoir_name})"
            else:
                reservoirs_str = ", ".join(sorted(reservoirs_filter))
                if len(selected_wells) == 1:
                    well = selected_wells[0]
                    # For multiple reservoirs, show the well and its reservoirs
                    well_title = f"{well.well_name} ({reservoirs_str})"
                else:
                    well_title = f"{len(selected_wells)} Wells ({reservoirs_str})"
            
            # Update charts with reservoir-specific titles
            self.production_chart.update_chart(prod_data, [well_title])
            self.injection_chart.update_chart(inj_data, [well_title])
        else:
            # Normal update with well names
            self.production_chart.update_chart(prod_data, well_names)
            self.injection_chart.update_chart(inj_data, well_names)
        
        # Update status bar
        if selected_wells:
            if len(selected_wells) == 1:
                well = selected_wells[0]
                status = f"Selected: {well.well_name}"
                active_status = "Active" if well.active else "Inactive"
                # Show proper well type description
                well_type_display = "Producer" if well.well_type == "PRODUCTION" else "Injector"
                
                # Show completions and reservoirs information
                completations = self.data_store.well_to_completions.get(well.well_name, [])
                if completations:
                    # Get reservoirs for completions
                    compl_reservoirs = []
                    for comp in completations:
                        reservoir = self.data_store.completion_to_reservoir.get(comp)
                        if reservoir:
                            compl_reservoirs.append(f"{comp} ({reservoir})")
                        else:
                            compl_reservoirs.append(comp)
                    
                    if compl_reservoirs:
                        status += f" ({well_type_display}, {active_status}, Completaciones: {', '.join(compl_reservoirs)})"
                    else:
                        status += f" ({well_type_display}, {active_status})"
                else:
                    status += f" ({well_type_display}, {active_status})"
            else:
                status = f"Selected: {len(selected_wells)} wells"
                
                # Count active/inactive wells
                active_count = sum(1 for well in selected_wells if well.active)
                inactive_count = len(selected_wells) - active_count
                
                # Count producers and injectors
                prod_count = sum(1 for well in selected_wells if well.well_type == "PRODUCTION")
                inj_count = len(selected_wells) - prod_count
                
                status += f" ({active_count} active, {inactive_count} inactive, {prod_count} producers, {inj_count} injectors)"
                
                if reservoirs_filter:
                    reservoirs_str = ", ".join(sorted(reservoirs_filter))
                    status += f" (Arenas: {reservoirs_str})"
            self.status_bar.showMessage(status)
    
    def clear_selection(self):
        """Clear all well selections"""
        self.data_store.clear_selection()
        self.map_widget.clear_selection()
        
        # Clear charts
        self.production_chart.update_chart()
        self.injection_chart.update_chart()
        
        # Update status
        if self.reservoir_buttons['all'].isChecked():
            well_count = len(self.data_store.wells)
            active_wells = sum(1 for well in self.data_store.wells.values() if well.active)
            inactive_wells = well_count - active_wells
            prod_wells = sum(1 for well in self.data_store.wells.values() if well.well_type == "PRODUCTION")
            inj_wells = well_count - prod_wells
            
            self.status_bar.showMessage(
                f"Showing all {well_count} wells " +
                f"({active_wells} active, {inactive_wells} inactive, " +
                f"{prod_wells} producers, {inj_wells} injectors)"
            )
        elif self.selected_reservoirs:
            # Get wells that have completions in the selected reservoirs
            wells_with_completions = self.data_store.get_wells_for_reservoirs(self.selected_reservoirs)
            
            # Count active/inactive wells with completions
            active_wells = sum(1 for well_name in wells_with_completions if self.data_store.wells[well_name].active)
            inactive_wells = len(wells_with_completions) - active_wells
            
            # Count producers and injectors
            prod_wells = sum(1 for well_name in wells_with_completions 
                        if self.data_store.wells[well_name].well_type == "PRODUCTION")
            inj_wells = len(wells_with_completions) - prod_wells
            
            # Count total wells (now we show all)
            total_wells = len(self.data_store.wells)
            
            reservoirs_str = ", ".join(sorted(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing all {total_wells} wells. " +
                f"{len(wells_with_completions)} wells have completions in reservoirs: {reservoirs_str} " +
                f"({active_wells} active, {inactive_wells} inactive, " +
                f"{prod_wells} producers, {inj_wells} injectors)"
            )
        else:
            self.status_bar.showMessage("Selection cleared")
    
    def select_all_visible_wells(self):
        """Select all currently visible wells"""
        # Clear current selection first
        self.data_store.clear_selection()
        
        # Select all visible wells in the map
        for well_name, well_data in self.map_widget.wells.items():
            if well_data.get('visible', True):
                self.data_store.select_well(well_name)
                self.map_widget.select_well(well_name, True, False)
        
        # Update charts
        self.update_charts()
        
        # Update status
        selected_count = len(self.data_store.selected_wells)
        active_count = sum(1 for name in self.data_store.selected_wells 
                          if name in self.data_store.wells and self.data_store.wells[name].active)
        inactive_count = selected_count - active_count
        
        # Count producers and injectors
        prod_count = sum(1 for name in self.data_store.selected_wells 
                        if name in self.data_store.wells and 
                        self.data_store.wells[name].well_type == "PRODUCTION")
        inj_count = selected_count - prod_count
        
        self.status_bar.showMessage(
            f"Selected all {selected_count} visible wells " +
            f"({active_count} active, {inactive_count} inactive, " +
            f"{prod_count} producers, {inj_count} injectors)"
        )

    
    def toggle_multi_selection_mode(self, state):
        """Toggle between single and multi-selection modes"""
        # Just update the status bar to reflect the mode change
        if state:
            self.status_bar.showMessage("Multi-selection mode enabled - click wells to add to selection")
        else:
            self.status_bar.showMessage("Single-selection mode enabled - hold Ctrl to select multiple wells")
    
    def filter_wells(self, search_text):
        """Filter wells based on search text - selecting matching wells instead of hiding others"""
        if not search_text:
            # If search field is empty, just clear selection
            self.clear_selection()
            return
        
        search_text = search_text.lower()
        
        # Clear current selection
        self.data_store.clear_selection()
        self.map_widget.clear_selection()
        
        # Select wells that match the search text
        matching_wells = []
        for well_name, well in self.data_store.wells.items():
            # Check if well name contains search text
            if search_text in well_name.lower():
                # Select this well in data store and map
                self.data_store.select_well(well_name)
                self.map_widget.select_well(well_name, True, False)
                matching_wells.append(well_name)
        
        # Update charts
        self.update_charts()
        
        # Update status bar
        if matching_wells:
            active_count = sum(1 for name in matching_wells 
                              if name in self.data_store.wells and self.data_store.wells[name].active)
            inactive_count = len(matching_wells) - active_count
            
            # Count producers and injectors
            prod_count = sum(1 for name in matching_wells 
                            if name in self.data_store.wells and 
                            self.data_store.wells[name].well_type == "PRODUCTION")
            inj_count = len(matching_wells) - prod_count
            
            self.status_bar.showMessage(
                f"Selected {len(matching_wells)} wells matching '{search_text}' " +
                f"({active_count} active, {inactive_count} inactive, " +
                f"{prod_count} producers, {inj_count} injectors)"
            )
        else:
            self.status_bar.showMessage(f"No wells found matching '{search_text}'")

    def keyPressEvent(self, event):
        """Handle key press events for the main window"""
        # Handle keyboard shortcuts
        if event.key() == Qt.Key_A and event.modifiers() & Qt.ControlModifier:
            # Ctrl+A to select all visible wells
            self.select_all_visible_wells()
        elif event.key() == Qt.Key_Escape:
            # Escape to clear selection
            self.clear_selection()
        
        super().keyPressEvent(event)
    
    # Métodos para las operaciones
    def run_well_type_operation(self):
        """Run the well type calculation operation - simplified to only calculate at well level"""
        dialog = WellTypeOperationDialog(self)
        if dialog.exec_() != QDialog.Accepted:
            return
        
        # Get options from dialog
        options = dialog.get_options()
        
        # Always use well_monthly_type operation type now
        operation_type = "well_monthly_type"
        
        # Check if operation already exists
        if self.operations_db.operation_exists(operation_type):
            result = QMessageBox.question(
                self,
                "Operation Exists",
                f"A previous '{operation_type}' operation already exists. "
                f"Would you like to run it again?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if result != QMessageBox.Yes:
                return
        
        # Create progress dialog
        progress_dialog = OperationProgressDialog(
            self,
            title="Calculating Well Types",
            description="Calculating well types for all wells using the high-performance method"
        )
        
        # Create worker thread
        worker = OperationWorker(operation_type, self.data_store, options)
        
        # Connect signals
        worker.progress_updated.connect(progress_dialog.update_progress)
        worker.operation_completed.connect(lambda success, result: self.on_operation_completed(
            success, result, operation_type, progress_dialog, options
        ))
        
        # Start worker and show dialog
        worker.start()
        progress_dialog.exec_()
        
        # If dialog was rejected (Cancel button), terminate worker
        if not progress_dialog.result():
            worker.terminate()
            worker.wait()
    
    def on_operation_completed(self, success, result, operation_type, progress_dialog, options):
        """Handle operation completion"""
        if success:
            # Update progress dialog
            progress_dialog.operation_complete()
            
            # Save results to database
            save_success = self.save_operation_results(operation_type, result, options)
            
            # Show appropriate message
            if save_success:
                # Gather statistics for display
                stats_message = ""
                if 'monthly_types' in result:
                    monthly_count = len(result['monthly_types'])
                    well_count = result['monthly_types']['well_name'].nunique()
                    stats_message = f"\n\nProcessed {well_count} wells with {monthly_count} monthly records."
                
                QMessageBox.information(
                    self,
                    "Operation Completed",
                    f"The {operation_type} operation completed successfully.{stats_message}"
                )
            else:
                QMessageBox.warning(
                    self,
                    "Operation Partially Completed",
                    f"The {operation_type} calculation completed, but there was a problem saving all results to the database."
                )
        else:
            # If there's a traceback, show it in a detailed error message
            error_message = str(result)
            if '\n' in error_message:
                short_error = error_message.split('\n')[0]
            else:
                short_error = error_message
                
            # Update progress dialog with error
            progress_dialog.operation_failed(short_error)
            
            # Show detailed error message
            QMessageBox.critical(
                self,
                "Operation Failed",
                f"The {operation_type} operation failed:\n\n{short_error}\n\nSee the application log for more details."
            )
            
            # Print full error to console for debugging
            print(f"Operation error details:\n{error_message}")
    
    def save_operation_results(self, operation_type, results, options):
        """Save operation results to database, overwriting any previous operation of the same type"""
        try:
            # Create operation entry
            description = "Operation to classify wells by type and track completion status"
            parameters_json = json.dumps(options)
            
            # The create_operation method now deletes the previous operation if it exists
            operation_id = self.operations_db.create_operation(
                operation_name=operation_type,
                description=description,
                parameters=parameters_json
            )
            
            if not operation_id:
                QMessageBox.warning(
                    self,
                    "Database Warning",
                    f"Could not create operation record. Results were not saved."
                )
                return False
            
            # Save monthly well type data
            if 'monthly_types' in results and not results['monthly_types'].empty:
                success = self.operations_db.save_well_monthly_type(operation_id, results['monthly_types'])
                if not success:
                    QMessageBox.warning(
                        self,
                        "Database Warning",
                        f"Error saving monthly well type results."
                    )
                    return False
            else:
                QMessageBox.warning(
                    self,
                    "No Data",
                    f"No monthly well type results were generated."
                )
                return False
            
            # Save completion status data if available
            if 'completion_status' in results and not results['completion_status'].empty:
                success = self.operations_db.save_completion_status(operation_id, results['completion_status'])
                if not success:
                    QMessageBox.warning(
                        self,
                        "Database Warning",
                        f"Error saving completion status results."
                    )
                    # Continue despite this error, since we already saved the monthly data
            
            return True
                
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error in Saving Results",
                f"An error occurred while saving the operation results: {str(e)}"
            )
            return False
    
    def run_completion_state_operation(self):
        """Run the completion state calculation operation"""
        dialog = CompletionStateOperationDialog(self)
        if dialog.exec_() != QDialog.Accepted:
            return
        
        # Get options from dialog
        options = dialog.get_options()
        
        # Use a specific operation type for completion state
        operation_type = "completion_state"
        
        # Check if operation already exists
        if self.operations_db.operation_exists(operation_type):
            result = QMessageBox.question(
                self,
                "Operation Exists",
                f"A previous '{operation_type}' operation already exists. "
                f"Would you like to run it again?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if result != QMessageBox.Yes:
                return
        
        # Create progress dialog
        progress_dialog = OperationProgressDialog(
            self,
            title="Calculating Completion States",
            description="Calculating the state of each well completion by reservoir"
        )
        
        # Create worker thread
        worker = OperationWorker(operation_type, self.data_store, options)
        
        # Connect signals
        worker.progress_updated.connect(progress_dialog.update_progress)
        worker.operation_completed.connect(lambda success, result: self.on_operation_completed(
            success, result, operation_type, progress_dialog, options
        ))
        
        # Start worker and show dialog
        worker.start()
        progress_dialog.exec_()
        
        # If dialog was rejected (Cancel button), terminate worker
        if not progress_dialog.result():
            worker.terminate()
            worker.wait()
    
    def view_operations(self):
        """View and manage previous operations"""
        # Get list of operations
        operations_df = self.operations_db.get_operations()
        
        if operations_df.empty:
            QMessageBox.information(
                self,
                "No Operations",
                "No previous operations found."
            )
            return
        
        # Show operations list dialog
        dialog = OperationListDialog(self, operations_df)
        result = dialog.exec_()
        
        # Handle dialog result
        operation = dialog.get_selected_operation()
        if operation and result == 1:  # View operation
            self.view_operation_results(operation)
        elif operation and result == 2:  # Delete operation
            self.delete_operation(operation)

    def view_operation_results(self, operation):
        """View the results of an operation"""
        operation_id = operation['operation_id']
        operation_name = operation['operation_name']
        
        if operation_name == "well_monthly_type":
            # Get monthly well type data
            monthly_df = self.operations_db.get_well_monthly_type(operation_id)
            self.show_monthly_type_results(monthly_df, operation)
        elif operation_name == "completion_state":
            # Get completion state data
            completion_df = self.operations_db.get_completion_status(operation_id)
            self.show_completion_state_results(completion_df, operation)
        else:
            QMessageBox.warning(
                self,
                "Unknown Operation Type",
                f"The operation type '{operation_name}' is not recognized."
            )

    def show_completion_state_results(self, completion_df, operation):
        """Show results of completion state operation"""
        if completion_df.empty:
            QMessageBox.warning(
                self,
                "No Data",
                "No completion state data found for this operation."
            )
            return
        
        # Create results dialog
        dialog = CompletionStateResultsDialog(
            self,
            title=f"Completion State Results - {operation['creation_date']}",
            description="Well completion states by reservoir",
            data=completion_df
        )
        
        # Show dialog
        dialog.exec_()

    def show_monthly_type_results(self, monthly_df, operation):
        """Show results of monthly well type operation"""
        if monthly_df.empty:
            QMessageBox.warning(
                self,
                "No Data",
                "No data found for this operation."
            )
            return
        
        # Create results dialog
        dialog = OperationResultsDialog(
            self,
            title=f"Well Monthly Type Results - {operation['creation_date']}",
            description="Monthly well type classification by well"
        )
        
        # Add statistics to results list
        well_count = monthly_df['well_name'].nunique()
        month_count = monthly_df[['year', 'month']].drop_duplicates().shape[0]
        producer_months = monthly_df[monthly_df['well_type'] == 'PRODUCTION'].shape[0]
        injector_months = monthly_df[monthly_df['well_type'] == 'INJECTION'].shape[0]
        
        dialog.add_result_item(f"Total wells: {well_count}")
        dialog.add_result_item(f"Total months: {month_count}")
        dialog.add_result_item(f"Producer well-months: {producer_months}")
        dialog.add_result_item(f"Injector well-months: {injector_months}")
        
        # Show dialog
        dialog.exec_()

    def delete_operation(self, operation):
        """Delete an operation and its data"""
        operation_id = operation['operation_id']
        success = self.operations_db.delete_operation(operation_id)
        
        if success:
            QMessageBox.information(
                self,
                "Operation Deleted",
                f"Operation '{operation['operation_name']}' deleted successfully."
            )
        else:
            QMessageBox.critical(
                self,
                "Delete Failed",
                f"Failed to delete operation '{operation['operation_name']}'."
            )

    def show_about(self):
        """Show information about the application"""
        QMessageBox.about(
            self,
            "About Well Production App",
            "<h1>Well Production App</h1>"
            "<p>Version 1.1.0</p>"
            "<p>An application for visualizing and analyzing well production data.</p>"
            "<p>© 2025 Energy Company</p>"
        )


def main():
    app = QApplication(sys.argv)
    window = WellProductionApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
            