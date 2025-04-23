# Well Production Application

A professional Windows application for visualizing and analyzing oil well production and injection data.

## Features

- Interactive map displaying well locations
- Production decline curve visualization
- Water injection profile visualization
- Multi-well selection and aggregation
- Responsive UI that updates charts based on selection
- Search functionality for quickly finding wells

## Requirements

- Python 3.7+
- Microsoft Access Database Driver (for .mdb file access)
- Dependencies listed in `requirements.txt`

## Setup Instructions

1. Clone this repository or extract the source code to your preferred location.

2. Create a virtual environment (recommended):
   ```
   python -m venv venv
   venv\Scripts\activate  # On Windows
   source venv/bin/activate  # On macOS/Linux
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Make sure the database file is in the correct location:
   ```
   \Data\AC_SACHA_DIC_2024.mdb
   ```

5. Run the application:
   ```
   python main_app.py
   ```

## Database Structure

The application uses an Access database with the following structure:

- **MAESTRA**: Contains well location information
  - WELL_LEGAL_NAME
  - COMPLETION_LEGAL_NAME
  - COMPLETION_COORDINATE_X
  - COMPLETION_COORDINATE_Y

- **MENSUAL**: Contains production data
  - COMP_S_NAME
  - PROD_DT
  - VO_OIL_PROD
  - VO_GAS_PROD
  - VO_WAT_PROD
  - DIAS_ON

- **INY_CALDAY**: Contains injection data
  - COMPLETION_LEGAL_NAME
  - Date
  - Water_INJ_CALDAY
  - press_iny

- **SC**: Contains well type information
  - COMPLETION_LEGAL_NAME
  - TIPO_POZO
  - RESERVORIO

## Usage Guide

1. **Navigation**:
   - Left side shows the well map
   - Right side shows production and injection profiles
   - Click on wells to select them
   - Hold Ctrl while clicking to select multiple wells

2. **Search**:
   - Use the search box at the top to filter wells by name

3. **Data Visualization**:
   - When multiple wells are selected, charts show the aggregate data
   - Clear selection using the button at the bottom

## File Structure

- `main_app.py`: Main application entry point
- `database_manager.py`: Handles database connections and queries
- `models.py`: Data models for wells and production data
- `map_widget.py`: Interactive well map component
- `chart_widgets.py`: Production and injection chart components

## Troubleshooting

- If you encounter database connection issues, ensure you have the Microsoft Access ODBC driver installed
- For 64-bit Python, you'll need the 64-bit version of the Access Database Engine

## License

This software is proprietary and confidential.