from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt5.QtCore import Qt
import matplotlib
matplotlib.use('Qt5Agg')

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import pandas as pd
import numpy as np


class ProductionProfileChart(QWidget):
    """Widget for displaying oil production profile chart"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Setup UI
        self.layout = QVBoxLayout(self)
        
        # Title label
        self.title_label = QLabel("Production Profile")
        self.title_label.setAlignment(Qt.AlignCenter)
        self.title_label.setStyleSheet("font-size: 16px; font-weight: bold;")
        self.layout.addWidget(self.title_label)
        
        # Create matplotlib figure
        self.figure = Figure(figsize=(5, 4), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        self.layout.addWidget(self.canvas)
        
        self.ax = self.figure.add_subplot(111)
        self.ax2 = None  # Will be used for BSW on secondary y-axis
        self.setup_chart()
        
        self.setLayout(self.layout)
    
    def setup_chart(self):
        """Setup initial chart appearance"""
        self.ax.set_xlabel('Time (months)')
        self.ax.set_ylabel('Production Rate (bbl/d)')
        self.ax.grid(True)
        self.figure.tight_layout()
        self.canvas.draw()
    
    def update_chart(self, data=None, well_names=None):
        """
        Update chart with new data
        data: DataFrame with columns 'PROD_DT', 'OIL_RATE', 'WATER_RATE', 'LIQUID_RATE', 'BSW'
        well_names: List of selected well names for title
        """
        self.ax.clear()
        
        # Remove secondary axis if it exists
        if self.ax2 is not None:
            self.ax2.remove()
            self.ax2 = None
        
        if data is not None and not data.empty:
            # Create secondary axis for BSW
            self.ax2 = self.ax.twinx()
            
            # Plot oil, water and liquid rates
            self.ax.plot(data['PROD_DT'], data['OIL_RATE'], 'g-', linewidth=2, label='Oil Rate')
            self.ax.plot(data['PROD_DT'], data['WATER_RATE'], 'b-', linewidth=2, label='Water Rate')
            self.ax.plot(data['PROD_DT'], data['LIQUID_RATE'], 'k--', linewidth=2, label='Liquid Rate')
            
            # Plot BSW on secondary axis
            self.ax2.plot(data['PROD_DT'], data['BSW'], 'r-', linewidth=2, label='BSW %')
            self.ax2.set_ylabel('BSW (%)', color='r')
            self.ax2.tick_params(axis='y', labelcolor='r')
            
            # Format x-axis dates
            self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
            self.ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
            self.figure.autofmt_xdate()
            
            # Set y-axis scale
            max_rate = max(data['LIQUID_RATE'].max(), data['OIL_RATE'].max() * 1.2)
            self.ax.set_ylim(0, max_rate * 1.1)
            
            # Set BSW scale
            self.ax2.set_ylim(0, 100)  # BSW is a percentage
            
            # Add legend for both axes
            lines1, labels1 = self.ax.get_legend_handles_labels()
            lines2, labels2 = self.ax2.get_legend_handles_labels()
            self.ax.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
            
            # Update title if well names provided
            if well_names and len(well_names) > 0:
                if len(well_names) == 1:
                    title = f"Production Profile - {well_names[0]}"
                else:
                    title = f"Production Profile - {len(well_names)} Wells"
                self.title_label.setText(title)
        
        # Restore chart settings
        self.ax.set_xlabel('Time (months)')
        self.ax.set_ylabel('Production Rate (bbl/d)')
        self.ax.grid(True)
        
        # Redraw
        self.figure.tight_layout()
        self.canvas.draw()
    
    def plot_decline_curve(self, decline_data):
        """
        Plot decline curve fit on the production profile
        decline_data: Dictionary with decline curve parameters and data
        """
        if not decline_data:
            return
            
        # Get months and rates from decline data
        months = np.array(decline_data.get('months', []))
        fitted_rates = np.array(decline_data.get('fitted_rates', []))
        
        if len(months) > 0 and len(fitted_rates) > 0:
            # Convert months to dates (assuming first date is known)
            start_date = pd.Timestamp.now() - pd.DateOffset(months=int(months[-1]))
            dates = [start_date + pd.DateOffset(months=int(m)) for m in months]
            
            # Plot decline curve
            self.ax.plot(dates, fitted_rates, 'r--', linewidth=2, label='Decline Curve')
            self.ax.legend()
            
            # Redraw
            self.figure.tight_layout()
            self.canvas.draw()


class InjectionProfileChart(QWidget):
    """Widget for displaying water injection profile chart"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Setup UI
        self.layout = QVBoxLayout(self)
        
        # Title label
        self.title_label = QLabel("Injection Profile")
        self.title_label.setAlignment(Qt.AlignCenter)
        self.title_label.setStyleSheet("font-size: 16px; font-weight: bold;")
        self.layout.addWidget(self.title_label)
        
        # Create matplotlib figure
        self.figure = Figure(figsize=(5, 4), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        self.layout.addWidget(self.canvas)
        
        self.ax = self.figure.add_subplot(111)
        self.setup_chart()
        
        self.setLayout(self.layout)
    
    def setup_chart(self):
        """Setup initial chart appearance"""
        self.ax.set_xlabel('Time (months)')
        self.ax.set_ylabel('Water Rate (bbl/d)')
        self.ax.grid(True)
        self.figure.tight_layout()
        self.canvas.draw()
    
    def update_chart(self, data=None, well_names=None):
        """
        Update chart with new data
        data: DataFrame with columns 'Date' and 'WATER_INJ_RATE'
        well_names: List of selected well names for title
        """
        self.ax.clear()
        
        if data is not None and not data.empty:
            # Plot water injection rate vs time
            self.ax.plot(data['Date'], data['WATER_INJ_RATE'], 'b-', linewidth=2)
            
            # Format x-axis dates
            self.ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
            self.ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
            self.figure.autofmt_xdate()
            
            # Set y-axis scale
            max_rate = data['WATER_INJ_RATE'].max()
            self.ax.set_ylim(0, max_rate * 1.1)
            
            # Update title if well names provided
            if well_names and len(well_names) > 0:
                if len(well_names) == 1:
                    title = f"Injection Profile - {well_names[0]}"
                else:
                    title = f"Injection Profile - {len(well_names)} Wells"
                self.title_label.setText(title)
        
        # Restore chart settings
        self.ax.set_xlabel('Time (months)')
        self.ax.set_ylabel('Water Rate (bbl/d)')
        self.ax.grid(True)
        
        # Redraw
        self.figure.tight_layout()
        self.canvas.draw()
        
    def add_pressure_data(self, data):
        """
        Add injection pressure as a secondary y-axis
        data: DataFrame with 'Date' and 'press_iny' columns
        """
        if data is None or data.empty or 'press_iny' not in data.columns:
            return
            
        # Create secondary y-axis
        ax2 = self.ax.twinx()
        ax2.plot(data['Date'], data['press_iny'], 'r--', linewidth=2)
        ax2.set_ylabel('Injection Pressure (psi)', color='r')
        ax2.tick_params(axis='y', labelcolor='r')
        
        # Redraw
        self.figure.tight_layout()
        self.canvas.draw()