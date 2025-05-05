from PyQt5.QtWidgets import QWidget, QVBoxLayout, QMenu, QApplication
from PyQt5.QtCore import pyqtSignal, pyqtSlot, Qt, QPointF, QRectF
from PyQt5.QtGui import QPainter, QPen, QBrush, QColor, QPainterPath

class WellMapWidget(QWidget):
    """
    Widget for displaying and interacting with well locations on a map.
    Enhanced to support well visualization by unique well (not by completion).
    """
    # Signal emitted when a well is clicked
    wellClicked = pyqtSignal(str)
    # Signal emitted when multiple wells are selected
    wellsSelected = pyqtSignal(list)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Setup UI
        self.layout = QVBoxLayout(self)
        self.setLayout(self.layout)
        
        # Map data
        self.wells = {}  # Dict of well_name: (x, y, selected, well_type, active)
        self.map_bounds = QRectF(0, 0, 100, 100)  # Default map bounds
        self.scale_factor = 1.0
        self.offset_x = 0
        self.offset_y = 0
        self.drag_start = None
        self.ctrl_pressed = False
        self.selection_box_active = False
        self.selection_start = None
        self.selection_current = None
        
        # Add a reference to active reservoirs (set by main_app)
        self.selected_reservoirs = set()
        
        # Track whether "All" reservoirs button is checked
        self.reservoir_buttons_all_checked = True
        
        # Well display properties
        self.well_radius = 10
        self.selected_well_radius = 15
        
        # Color definitions for well states
        self.production_active_color = QColor(0, 150, 0)    # Green
        self.production_inactive_color = QColor(0, 150, 0)  # Green border only
        self.injection_active_color = QColor(0, 0, 200)     # Blue
        self.injection_inactive_color = QColor(0, 0, 200)   # Blue border only
        self.other_well_color = QColor(150, 150, 150)       # Grey
        self.no_completion_color = QColor(150, 150, 150)    # Grey for wells without completions in the reservoir
        self.selected_color = QColor(200, 0, 0)             # Red
        
        # Dictionary to store reservoir status for each well
        self.well_reservoir_status = {}  # Format: {well_name: {reservoir: {'has_completion': bool, 'active': bool, 'type': str}}}
        
        # Enable mouse tracking
        self.setMouseTracking(True)
        
        # For responsiveness
        from PyQt5.QtWidgets import QSizePolicy
        self.setSizePolicy(
            QSizePolicy.Expanding,
            QSizePolicy.Expanding
        )
        
        # Context menu
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
    
    def add_well(self, well_name, x, y, well_type="PRODUCTION", active=False):
        """Add a well to the map"""
        self.wells[well_name] = {
            'x': x,
            'y': y,
            'selected': False,
            'type': well_type.upper(),
            'visible': True,  # Add visibility property
            'active': active  # Add active state property
        }
        self.update_map_bounds()
        self.update()
    
    def update_well_reservoir_status(self, well_name, reservoir, has_completion, active, well_type="PRODUCTION"):
        """
        Update well completion status for a specific reservoir
        Modified to include well_type for reservoir-specific well type
        """
        if well_name not in self.well_reservoir_status:
            self.well_reservoir_status[well_name] = {}
            
        if reservoir not in self.well_reservoir_status[well_name]:
            self.well_reservoir_status[well_name][reservoir] = {}
            
        self.well_reservoir_status[well_name][reservoir] = {
            'has_completion': has_completion,
            'active': active,
            'type': well_type
        }
    
    def set_selected_reservoirs(self, reservoir_set):
        """Set the currently selected reservoirs for special rendering"""
        self.selected_reservoirs = reservoir_set
        self.update()
    
    def set_well_activity(self, well_name, active):
        """Set the activity state of a well"""
        if well_name in self.wells:
            self.wells[well_name]['active'] = active
            self.update()
    
    def set_well_visibility(self, well_name, visible):
        """Set visibility of a well"""
        if well_name in self.wells:
            self.wells[well_name]['visible'] = visible
    
    def set_wells(self, wells_dict):
        """Set multiple wells at once"""
        self.wells = wells_dict
        self.update_map_bounds()
        self.update()
    
    def update_map_bounds(self):
        """Calculate the bounding box of all wells"""
        if not self.wells:
            return
        
        x_coords = [well['x'] for well in self.wells.values()]
        y_coords = [well['y'] for well in self.wells.values()]
        
        min_x = min(x_coords)
        max_x = max(x_coords)
        min_y = min(y_coords)
        max_y = max(y_coords)
        
        # Add some padding
        width = max(max_x - min_x, 0.001) * 1.2
        height = max(max_y - min_y, 0.001) * 1.2
        
        self.map_bounds = QRectF(
            min_x - width * 0.1,
            min_y - height * 0.1,
            width,
            height
        )
        
        # Reset view
        self.scale_factor = 1.0
        self.offset_x = 0
        self.offset_y = 0
    
    def select_well(self, well_name, selected=True, emit_signal=True):
        """Select or deselect a well"""
        if well_name in self.wells:
            # Only update if changing selection state
            if self.wells[well_name]['selected'] != selected:
                self.wells[well_name]['selected'] = selected
                
                if emit_signal:
                    # Emit signal for all selected wells, not just this one
                    self.wellsSelected.emit(self.get_selected_wells())
                
                self.update()
    
    def toggle_well_selection(self, well_name, emit_signal=True):
        """Toggle selection state of a well"""
        if well_name in self.wells:
            self.wells[well_name]['selected'] = not self.wells[well_name]['selected']
            
            if emit_signal:
                # Emit signal for all selected wells, not just this one
                self.wellsSelected.emit(self.get_selected_wells())
            
            self.update()
    
    def clear_selection(self):
        """Clear all selections"""
        # Only update if there are selected wells
        if any(well['selected'] for well in self.wells.values()):
            for well_name in self.wells:
                self.wells[well_name]['selected'] = False
            self.update()
            self.wellsSelected.emit([])  # Emit empty list to clear selection
    
    def get_selected_wells(self):
        """Return list of selected well names"""
        return [name for name, data in self.wells.items() 
                if data['selected'] and data.get('visible', True)]
    
    def transform_point(self, x, y):
        """Transform point from map coordinates to widget coordinates"""
        if self.map_bounds.width() == 0 or self.map_bounds.height() == 0:
            return 0, 0
            
        widget_width = self.width()
        widget_height = self.height()
        
        # Calculate scale to fit map in widget
        scale_x = widget_width / self.map_bounds.width()
        scale_y = widget_height / self.map_bounds.height()
        scale = min(scale_x, scale_y) * self.scale_factor
        
        # Calculate center offset
        center_x = widget_width / 2
        center_y = widget_height / 2
        
        # Transform point with y-axis inverted (subtract from height to flip)
        tx = center_x + (x - self.map_bounds.center().x()) * scale + self.offset_x
        
        # Invert the y-coordinate to correct north-south orientation
        ty = center_y - (y - self.map_bounds.center().y()) * scale + self.offset_y
        
        return tx, ty
    
    def inverse_transform(self, x, y):
        """Transform point from widget coordinates to map coordinates"""
        if self.map_bounds.width() == 0 or self.map_bounds.height() == 0:
            return 0, 0
            
        widget_width = self.width()
        widget_height = self.height()
        
        # Calculate scale
        scale_x = widget_width / self.map_bounds.width()
        scale_y = widget_height / self.map_bounds.height()
        scale = min(scale_x, scale_y) * self.scale_factor
        
        # Calculate center
        center_x = widget_width / 2
        center_y = widget_height / 2
        
        # Inverse transform with y-axis correction
        mx = self.map_bounds.center().x() + (x - center_x - self.offset_x) / scale
        
        # Invert the y-coordinate to correct north-south orientation
        my = self.map_bounds.center().y() - (y - center_y - self.offset_y) / scale
        
        return mx, my
    
    def paintEvent(self, event):
        """Draw the map and wells"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # Draw background
        painter.fillRect(self.rect(), QColor(245, 245, 220))  # Light beige
        
        # Draw wells
        for well_name, well_data in self.wells.items():
            # Skip if not visible
            if not well_data.get('visible', True):
                continue
                
            x, y = self.transform_point(well_data['x'], well_data['y'])
            well_type = well_data['type']
            
            # Handle well rendering based on selection and reservoir status
            if well_data['selected']:
                # Selected wells always use selected color
                pen_color = self.selected_color.darker()
                brush_color = self.selected_color
                radius = self.selected_well_radius
                pen_width = 2
                
                # Draw the selected well
                painter.setPen(QPen(pen_color, pen_width))
                painter.setBrush(QBrush(brush_color))
                painter.drawEllipse(QPointF(x, y), radius, radius)
                
            else:
                radius = self.well_radius
                pen_width = 2
                
                # Check if we are filtering by reservoirs
                if self.selected_reservoirs and not self.reservoir_buttons_all_checked:
                    # We're filtering by specific reservoirs
                    has_completion_in_selected = False
                    is_active_in_selected = False
                    well_type_in_reservoir = "PRODUCTION"  # Default
                    
                    # Check if well has completions in any selected reservoir
                    for reservoir in self.selected_reservoirs:
                        if (well_name in self.well_reservoir_status and 
                            reservoir in self.well_reservoir_status[well_name]):
                            
                            status = self.well_reservoir_status[well_name][reservoir]
                            if status['has_completion']:
                                has_completion_in_selected = True
                                if status['active']:
                                    is_active_in_selected = True
                                    well_type_in_reservoir = status['type']
                                    break  # Found active completion, no need to check more
                    
                    # Set colors based on completion status in selected reservoirs
                    if has_completion_in_selected:
                        if well_type_in_reservoir == 'PRODUCTION':
                            pen_color = self.production_active_color.darker()
                            if is_active_in_selected:
                                brush_color = self.production_active_color
                            else:
                                brush_color = QColor(0, 0, 0, 0)  # Transparent
                        else:  # INJECTION
                            pen_color = self.injection_active_color.darker()
                            if is_active_in_selected:
                                brush_color = self.injection_active_color
                            else:
                                brush_color = QColor(0, 0, 0, 0)  # Transparent
                    else:
                        # No completions in selected reservoirs - grey outline and transparent fill
                        pen_color = self.no_completion_color
                        brush_color = QColor(0, 0, 0, 0)  # Transparent
                        
                else:
                    # Not filtering by reservoir or showing all reservoirs
                    # Use the default well coloring based on type and activity
                    if well_type == 'PRODUCTION':
                        pen_color = self.production_active_color.darker()
                        if well_data['active']:
                            brush_color = self.production_active_color
                        else:
                            brush_color = QColor(0, 0, 0, 0)  # Transparent
                    elif well_type == 'INJECTION':
                        pen_color = self.injection_active_color.darker()
                        if well_data['active']:
                            brush_color = self.injection_active_color
                        else:
                            brush_color = QColor(0, 0, 0, 0)  # Transparent
                    else:
                        pen_color = self.other_well_color.darker()
                        brush_color = self.other_well_color
                
                # Draw different icons based on well type
                if well_type == 'INJECTION':
                    # Draw circle for injection well
                    painter.setPen(QPen(pen_color, pen_width))
                    painter.setBrush(QBrush(brush_color))
                    painter.drawEllipse(QPointF(x, y), radius, radius)
                    
                    # Draw diagonal arrow through the circle
                    arrow_length = radius * 2.0
                    arrow_head_size = radius * 0.6
                    
                    # Calculate points for the diagonal main line (45 degree angle)
                    angle = 3.14159 / 4  # 45 degrees
                    
                    start_x = x - arrow_length * 0.707  # cos(45°) ≈ 0.707
                    start_y = y - arrow_length * 0.707  # sin(45°) ≈ 0.707
                    end_x = x + arrow_length * 0.707
                    end_y = y + arrow_length * 0.707
                    
                    # Draw the diagonal main line
                    arrow_pen = QPen(pen_color, pen_width * 1.5)  # Thicker line for the arrow
                    painter.setPen(arrow_pen)
                    painter.drawLine(QPointF(start_x, start_y), QPointF(end_x, end_y))
                    
                    # Calculate arrowhead points
                    p1_x = end_x - arrow_head_size * 0.707
                    p1_y = end_y
                    p2_x = end_x
                    p2_y = end_y - arrow_head_size * 0.707
                    
                    # Draw arrowhead lines
                    painter.drawLine(QPointF(end_x, end_y), QPointF(p1_x, p1_y))
                    painter.drawLine(QPointF(end_x, end_y), QPointF(p2_x, p2_y))
                    
                else:  # PRODUCTION
                    # Standard drawing for production wells
                    painter.setPen(QPen(pen_color, pen_width))
                    painter.setBrush(QBrush(brush_color))
                    painter.drawEllipse(QPointF(x, y), radius, radius)
                
            # Draw well name
            painter.setPen(QPen(Qt.black, 1))
            painter.drawText(int(x + radius + 2), int(y + 5), well_name)
        
        # Draw selection box if active
        if self.selection_box_active and self.selection_start and self.selection_current:
            painter.setPen(QPen(QColor(0, 0, 255, 150), 1, Qt.DashLine))
            painter.setBrush(QBrush(QColor(0, 0, 255, 30)))
            
            x1, y1 = self.selection_start.x(), self.selection_start.y()
            x2, y2 = self.selection_current.x(), self.selection_current.y()
            
            selection_rect = QRectF(min(x1, x2), min(y1, y2), abs(x2-x1), abs(y2-y1))
            painter.drawRect(selection_rect)
    
    # Tracking state of the "All" reservoir button
    def set_all_reservoirs_button_state(self, checked):
        """Track the state of the 'All' reservoirs button"""
        self.reservoir_buttons_all_checked = checked
        self.update()
    
    def mousePressEvent(self, event):
        """Handle mouse press events"""
        if event.button() == Qt.LeftButton:
            self.drag_start = event.pos()
            
            # Start selection box if shift is pressed
            if event.modifiers() & Qt.ShiftModifier:
                self.selection_box_active = True
                self.selection_start = event.pos()
                self.selection_current = event.pos()
                return
            
            # Check if clicking on a well
            clicked_on_well = False
            for well_name, well_data in self.wells.items():
                # Skip if not visible
                if not well_data.get('visible', True):
                    continue
                    
                x, y = self.transform_point(well_data['x'], well_data['y'])
                distance = ((event.pos().x() - x) ** 2 + (event.pos().y() - y) ** 2) ** 0.5
                
                if distance <= self.well_radius * 1.5:  # Increased hit area slightly
                    clicked_on_well = True
                    
                    # Check if Ctrl key is pressed for multi-selection
                    if event.modifiers() & Qt.ControlModifier:
                        # Toggle selection with Ctrl key
                        self.toggle_well_selection(well_name)
                    else:
                        # Clear previous selection and select this well
                        for w_name in self.wells:
                            if w_name != well_name:
                                self.wells[w_name]['selected'] = False
                        
                        # Select this well
                        self.wells[well_name]['selected'] = True
                        
                        # Emit selected wells
                        self.wellsSelected.emit([well_name])
                    
                    self.update()
                    break
            
            # If didn't click on a well and not holding ctrl, clear selection
            if not clicked_on_well and not (event.modifiers() & Qt.ControlModifier):
                self.clear_selection()
    
    def mouseMoveEvent(self, event):
        """Handle mouse move events for panning and selection box"""
        if self.selection_box_active:
            # Update selection box
            self.selection_current = event.pos()
            self.update()
            return
            
        if self.drag_start is not None and not self.selection_box_active:
            # Calculate drag distance
            drag_x = event.pos().x() - self.drag_start.x()
            drag_y = event.pos().y() - self.drag_start.y()
            
            # Update offset
            self.offset_x += drag_x
            self.offset_y += drag_y
            
            # Update drag start
            self.drag_start = event.pos()
            
            # Redraw
            self.update()
    
    def mouseReleaseEvent(self, event):
        """Handle mouse release events"""
        if event.button() == Qt.LeftButton:
            # Handle selection box if active
            if self.selection_box_active:
                self.select_wells_in_box(event.modifiers() & Qt.ControlModifier)
                self.selection_box_active = False
                self.selection_start = None
                self.selection_current = None
                self.update()
            
            self.drag_start = None
    
    def select_wells_in_box(self, keep_existing=False):
        """Select all wells within the selection box"""
        if not self.selection_start or not self.selection_current:
            return
            
        # Create selection rectangle
        x1, y1 = self.selection_start.x(), self.selection_start.y()
        x2, y2 = self.selection_current.x(), self.selection_current.y()
        
        selection_rect = QRectF(min(x1, x2), min(y1, y2), abs(x2-x1), abs(y2-y1))
        
        # Check which wells are in the rectangle
        selected_wells = []
        
        # If Ctrl is not pressed and we're not keeping existing selection, clear current selection
        if not keep_existing:
            for well_name in self.wells:
                self.wells[well_name]['selected'] = False
        
        # Select wells in the box
        for well_name, well_data in self.wells.items():
            # Skip if not visible
            if not well_data.get('visible', True):
                continue
                
            x, y = self.transform_point(well_data['x'], well_data['y'])
            if selection_rect.contains(QPointF(x, y)):
                self.wells[well_name]['selected'] = True
                selected_wells.append(well_name)
        
        # Emit signal if wells are selected
        self.wellsSelected.emit(self.get_selected_wells())
    
    def wheelEvent(self, event):
        """Handle mouse wheel events for zooming"""
        zoom_factor = 1.1
        
        if event.angleDelta().y() > 0:
            # Zoom in
            self.scale_factor *= zoom_factor
        else:
            # Zoom out
            self.scale_factor /= zoom_factor
        
        # Limit zoom range
        self.scale_factor = max(0.1, min(10.0, self.scale_factor))
        
        self.update()
    
    def keyPressEvent(self, event):
        """Handle key press events"""
        if event.key() == Qt.Key_Control:
            self.ctrl_pressed = True
        elif event.key() == Qt.Key_A and (event.modifiers() & Qt.ControlModifier):
            # Ctrl+A to select all visible wells
            self.select_all_visible_wells()
        super().keyPressEvent(event)
    
    def keyReleaseEvent(self, event):
        """Handle key release events"""
        if event.key() == Qt.Key_Control:
            self.ctrl_pressed = False
        super().keyReleaseEvent(event)
    
    def select_all_visible_wells(self):
        """Select all currently visible wells"""
        selected_wells = []
        
        for well_name, well_data in self.wells.items():
            if well_data.get('visible', True):
                self.wells[well_name]['selected'] = True
                selected_wells.append(well_name)
        
        self.update()
        
        # Emit signal with all selected wells
        if selected_wells:
            self.wellsSelected.emit(selected_wells)
    
    def show_context_menu(self, position):
        """Show context menu with selection options"""
        menu = QMenu(self)
        
        # Get selected wells under cursor for context
        well_under_cursor = None
        for well_name, well_data in self.wells.items():
            if not well_data.get('visible', True):
                continue
                
            x, y = self.transform_point(well_data['x'], well_data['y'])
            distance = ((position.x() - x) ** 2 + (position.y() - y) ** 2) ** 0.5
            
            if distance <= self.well_radius * 1.5:
                well_under_cursor = well_name
                break
                
        # Add actions
        select_all_action = menu.addAction("Select All Visible Wells")
        select_all_action.triggered.connect(self.select_all_visible_wells)
        
        clear_action = menu.addAction("Clear Selection")
        clear_action.triggered.connect(self.clear_selection)
        
        if well_under_cursor:
            menu.addSeparator()
            
            if self.wells[well_under_cursor]['selected']:
                deselect_action = menu.addAction(f"Deselect '{well_under_cursor}'")
                deselect_action.triggered.connect(lambda: self.select_well(well_under_cursor, False))
            else:
                select_action = menu.addAction(f"Select '{well_under_cursor}'")
                select_action.triggered.connect(lambda: self.select_well(well_under_cursor, True))
                
            select_only_action = menu.addAction(f"Select Only '{well_under_cursor}'")
            select_only_action.triggered.connect(lambda: self.select_only_well(well_under_cursor))
            
        menu.exec_(self.mapToGlobal(position))
    
    def select_only_well(self, well_name):
        """Clear selection and select only the specified well"""
        for w_name in self.wells:
            self.wells[w_name]['selected'] = (w_name == well_name)
        
        self.update()
        self.wellsSelected.emit([well_name])
        
    def resizeEvent(self, event):
        """Handle widget resize events"""
        super().resizeEvent(event)
        self.update()