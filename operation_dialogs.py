from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QProgressBar, QDialogButtonBox,
                             QMessageBox, QListWidget, QListWidgetItem, QFrame,
                             QComboBox, QGroupBox, QCheckBox, QDateEdit)
from PyQt5.QtCore import Qt, QDate


class OperationProgressDialog(QDialog):
    """Dialog to show progress of a calculation operation"""
    
    def __init__(self, parent=None, title="Operation in Progress", description=""):
        super().__init__(parent)
        
        self.setWindowTitle(title)
        self.setMinimumWidth(400)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Description label
        self.description_label = QLabel(description)
        self.description_label.setWordWrap(True)
        layout.addWidget(self.description_label)
        
        # Status label
        self.status_label = QLabel("Starting operation...")
        layout.addWidget(self.status_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)
        
        # Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)
        
        self.setLayout(layout)
    
    def update_progress(self, value, status_text=None):
        """Update progress bar and optionally status text"""
        self.progress_bar.setValue(value)
        if status_text:
            self.status_label.setText(status_text)
    
    def operation_complete(self):
        """Change dialog to show completion"""
        self.progress_bar.setValue(100)
        self.status_label.setText("Operation completed successfully")
        self.button_box.clear()
        self.button_box.addButton(QDialogButtonBox.Ok)
        self.button_box.accepted.connect(self.accept)
    
    def operation_failed(self, error_message):
        """Change dialog to show failure"""
        self.status_label.setText(f"Operation failed: {error_message}")
        self.button_box.clear()
        self.button_box.addButton(QDialogButtonBox.Ok)
        self.button_box.accepted.connect(self.accept)


class OperationResultsDialog(QDialog):
    """Dialog to display results of an operation"""
    
    def __init__(self, parent=None, title="Operation Results", description=""):
        super().__init__(parent)
        
        self.setWindowTitle(title)
        self.setMinimumSize(600, 400)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Description label
        self.description_label = QLabel(description)
        self.description_label.setWordWrap(True)
        layout.addWidget(self.description_label)
        
        # Results list
        self.results_list = QListWidget()
        layout.addWidget(self.results_list)
        
        # Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        self.button_box.accepted.connect(self.accept)
        layout.addWidget(self.button_box)
        
        self.setLayout(layout)
    
    def add_result_item(self, text, details=None):
        """Add an item to the results list"""
        item = QListWidgetItem(text)
        if details:
            item.setData(Qt.UserRole, details)
        self.results_list.addItem(item)


class WellTypeOperationDialog(QDialog):
    """Dialog to set up well type calculation operation"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.setWindowTitle("Well Type Classification")
        self.setMinimumWidth(450)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Description
        description = (
            "This operation will calculate the well type (producer, injector, or dual) "
            "for each well based on their production and injection data. The results will be "
            "stored in the operations database, overwriting any previous operation of the same type."
        )
        desc_label = QLabel(description)
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)
        
        # Options group
        options_group = QGroupBox("Options")
        options_layout = QVBoxLayout()
        
        # Add note about historical persistence
        note_label = QLabel("Note: A producer well will maintain its classification until injection data appears. "
                           "An injector well will maintain its classification until production data appears.")
        note_label.setStyleSheet("font-style: italic; color: #555;")
        note_label.setWordWrap(True)
        options_layout.addWidget(note_label)
        
        # Note about well-level only calculation (new)
        reservoir_note = QLabel("Well types will be calculated at the well level only, not at the reservoir level.")
        reservoir_note.setStyleSheet("font-style: italic; color: #555;")
        reservoir_note.setWordWrap(True)
        options_layout.addWidget(reservoir_note)
        
        # Note about overwriting previous results
        overwrite_note = QLabel("The results will overwrite any previous operation of the same type.")
        overwrite_note.setStyleSheet("font-weight: bold; color: #c00;")
        overwrite_note.setWordWrap(True)
        options_layout.addWidget(overwrite_note)
        
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)
        
        self.setLayout(layout)
    
    def get_options(self):
        """Return the selected options as a dictionary"""
        options = {
            'by_reservoir': False,  # Always set to False - no reservoir calculations
            'use_date_range': False,
            'start_date': None,
            'end_date': None
        }
        return options


class OperationListDialog(QDialog):
    """Dialog to display a list of previous operations and their results"""
    
    def __init__(self, parent=None, operations_df=None):
        super().__init__(parent)
        
        self.setWindowTitle("Previous Operations")
        self.setMinimumSize(700, 500)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Description
        description = "Select an operation to view details or perform actions"
        desc_label = QLabel(description)
        layout.addWidget(desc_label)
        
        # Operations list
        self.operations_list = QListWidget()
        layout.addWidget(self.operations_list)
        
        # Populate the list
        if operations_df is not None:
            self.add_operations(operations_df)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.view_button = QPushButton("View Results")
        self.view_button.clicked.connect(self.view_results)
        self.view_button.setEnabled(False)
        button_layout.addWidget(self.view_button)
        
        self.delete_button = QPushButton("Delete")
        self.delete_button.clicked.connect(self.delete_operation)
        self.delete_button.setEnabled(False)
        button_layout.addWidget(self.delete_button)
        
        button_layout.addStretch()
        
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        button_layout.addWidget(close_button)
        
        layout.addLayout(button_layout)
        
        # Connect selection change
        self.operations_list.itemSelectionChanged.connect(self.on_selection_changed)
        
        self.setLayout(layout)
    
    def add_operations(self, operations_df):
        """Add operations from DataFrame to the list"""
        for _, row in operations_df.iterrows():
            # Format item text
            item_text = f"{row['operation_name']} - {row['creation_date']}"
            
            # Create item
            item = QListWidgetItem(item_text)
            item.setData(Qt.UserRole, row.to_dict())
            
            # Add to list
            self.operations_list.addItem(item)
    
    def on_selection_changed(self):
        """Enable/disable buttons based on selection"""
        has_selection = len(self.operations_list.selectedItems()) > 0
        self.view_button.setEnabled(has_selection)
        self.delete_button.setEnabled(has_selection)
    
    def get_selected_operation(self):
        """Get the data for the selected operation"""
        items = self.operations_list.selectedItems()
        if not items:
            return None
        return items[0].data(Qt.UserRole)
    
    def view_results(self):
        """View results of the selected operation"""
        operation = self.get_selected_operation()
        if operation:
            self.accept()
            # Return code 1 means view operation
            self.done(1)
    
    def delete_operation(self):
        """Delete the selected operation"""
        operation = self.get_selected_operation()
        if operation:
            # Confirm deletion
            result = QMessageBox.question(
                self,
                "Confirm Deletion",
                f"Are you sure you want to delete the operation '{operation['operation_name']}'?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if result == QMessageBox.Yes:
                self.accept()
                # Return code 2 means delete operation
                self.done(2)