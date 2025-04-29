import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QPushButton, QLineEdit, 
                             QSplitter, QMessageBox, QStatusBar, QCheckBox)
from PyQt5.QtCore import Qt, QSize

from database_manager import DatabaseManager
from models import Well, WellDataStore
from map_widget import WellMapWidget
from chart_widgets import ProductionProfileChart, InjectionProfileChart


class WellProductionApp(QMainWindow):
    """Main application window for Well Production App"""
    
    def __init__(self):
        super().__init__()
        
        # Initialize data managers
        self.db_manager = DatabaseManager()
        self.data_store = WellDataStore()
        
        # Set up UI
        self.setup_ui()
        
        # Connect to database and load data
        self.load_data()
        
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
        reservoir_label = QLabel("Reservorio:")
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
    
    def load_data(self):
        """Connect to database and load well data"""
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
        
        # Add wells to map - using the dynamically determined well type
        for well_name, well in self.data_store.wells.items():
            # Add well with proper type and active status
            self.map_widget.add_well(well_name, well.x_coordinate, well.y_coordinate, 
                                     well.well_type, well.active)
        
        # Create reservoir buttons
        self.create_reservoir_buttons()
        
        # Disconnect from database
        self.db_manager.disconnect()
        
        # Update status
        well_count = len(self.data_store.wells)
        active_wells = sum(1 for well in self.data_store.wells.values() if well.active)
        inactive_wells = well_count - active_wells
        
        # Count injector wells vs producer wells
        inj_wells = sum(1 for well in self.data_store.wells.values() if well.well_type == "INJECTION")
        prod_wells = well_count - inj_wells
        
        self.status_bar.showMessage(
            f"Loaded {well_count} wells from database " +
            f"({active_wells} active, {inactive_wells} inactive), " +
            f"({prod_wells} producers, {inj_wells} injectors) as of Dec 2024"
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
        all_button = QPushButton("Todos")
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
        
        # Update status
        well_count = len(self.data_store.wells)
        prod_wells = sum(1 for well in self.data_store.wells.values() 
                        if well.well_type == "PRODUCTION")
        inj_wells = well_count - prod_wells
        self.status_bar.showMessage(f"Showing all {well_count} wells ({prod_wells} producers, {inj_wells} injectors)")
    
    def update_well_visibility(self):
        """Update visibility of wells based on selected reservoirs"""
        # If "All" is checked, show all wells
        if self.reservoir_buttons['all'].isChecked():
            for well_name in self.data_store.wells:
                self.map_widget.set_well_visibility(well_name, True)
            return
        
        # If no reservoirs selected, don't hide any wells
        if not self.selected_reservoirs:
            for well_name in self.data_store.wells:
                self.map_widget.set_well_visibility(well_name, True)
            return
        
        # Get wells that have completions in any of the selected reservoirs
        visible_wells = self.data_store.get_wells_for_reservoirs(self.selected_reservoirs)
        
        # Show wells from selected reservoirs, hide others
        for well_name in self.data_store.wells:
            if well_name in visible_wells:
                self.map_widget.set_well_visibility(well_name, True)
            else:
                self.map_widget.set_well_visibility(well_name, False)
        
        # Update the map
        self.map_widget.update()
        
        # Count producers and injectors in visible wells
        prod_count = sum(1 for well_name in visible_wells 
                        if self.data_store.wells[well_name].well_type == "PRODUCTION")
        inj_count = len(visible_wells) - prod_count
        
        # Update status bar
        if len(self.selected_reservoirs) == 1:
            reservoir = next(iter(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing {len(visible_wells)} wells in reservoir {reservoir} " +
                f"({prod_count} producers, {inj_count} injectors)"
            )
        else:
            reservoirs_str = ", ".join(sorted(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing {len(visible_wells)} wells in reservoirs: {reservoirs_str} " +
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
        """Update charts with current selection, filtered by selected reservoirs"""
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
                    well_title = f"{well_names[0]} ({reservoir_name})"
                else:
                    well_title = f"{len(selected_wells)} Wells ({reservoir_name})"
            else:
                reservoirs_str = ", ".join(sorted(reservoirs_filter))
                if len(selected_wells) == 1:
                    well_title = f"{well_names[0]} ({reservoirs_str})"
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
                
                if reservoirs_filter:
                    reservoirs_str = ", ".join(sorted(reservoirs_filter))
                    status += f" ({well_type_display}, {active_status}, Arenas: {reservoirs_str})"
                elif well.reservoir:
                    status += f" ({well_type_display}, {active_status}, {well.reservoir})"
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
            visible_wells = self.data_store.get_wells_for_reservoirs(self.selected_reservoirs)
            # Count active/inactive wells in the filter
            active_wells = sum(1 for well_name in visible_wells if self.data_store.wells[well_name].active)
            inactive_wells = len(visible_wells) - active_wells
            
            # Count producers and injectors
            prod_wells = sum(1 for well_name in visible_wells 
                            if self.data_store.wells[well_name].well_type == "PRODUCTION")
            inj_wells = len(visible_wells) - prod_wells
            
            reservoirs_str = ", ".join(sorted(self.selected_reservoirs))
            self.status_bar.showMessage(
                f"Showing {len(visible_wells)} wells " +
                f"({active_wells} active, {inactive_wells} inactive, " +
                f"{prod_wells} producers, {inj_wells} injectors) " +
                f"in reservoirs: {reservoirs_str}"
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


def main():
    app = QApplication(sys.argv)
    window = WellProductionApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()