import os
import logging
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import dask.dataframe as dd
import pandas as pd
from DaskEnhancedDataCleaner import DaskEnhancedDataCleaner

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global variables to store dataframes and cleaner instance
df_storage = {
    'original': None,
    'cleaned': None,
    'cleaner_instance': None,
    'filename': ''
}

@app.route('/')
def index():
    return "DataPro Agent Backend is running."

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file uploads, reads the data, and performs initial analysis."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        logger.info(f"File '{filename}' uploaded successfully.")
        
        # Initialize Dask cleaner and read data
        df_storage['cleaner_instance'] = DaskEnhancedDataCleaner(use_dask=True)
        ddf = df_storage['cleaner_instance'].read_data(filepath)
        df_storage['original'] = ddf.persist()
        df_storage['filename'] = filename
        
        # Perform initial analysis
        stats = df_storage['cleaner_instance'].analyze_data_quality(df_storage['original'])
        issues = df_storage['cleaner_instance'].get_data_quality_report(stats)

        # Get preview for UI
        preview_df = df_storage['original'].head(5).to_dict('records')
        
        # Calculate overall missing value count for UI
        total_rows = len(df_storage['original'])
        total_missing = int(df_storage['original'].isnull().values.sum().compute())
        
        return jsonify({
            "status": "success",
            "message": "File processed and analysis is ready.",
            "filename": filename,
            "total_rows": total_rows,
            "missing_values": total_missing,
            "columns": list(df_storage['original'].columns),
            "preview_data": preview_df,
            "issues": issues,
            "stats": stats
        })
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/clean', methods=['POST'])
def clean_data():
    """Applies cleaning options to the dataset and returns a summary."""
    if df_storage['original'] is None:
        return jsonify({"error": "No data has been uploaded yet."}), 400
        
    try:
        options = request.get_json()
        ddf = df_storage['original']

        # Apply cleaning based on UI options
        if options.get('removeDuplicates'):
            ddf = df_storage['cleaner_instance'].remove_duplicates(ddf)
            
        if options.get('dropMissing'):
            ddf = df_storage['cleaner_instance'].drop_missing_rows(ddf)
        
        if options.get('fillMissing'):
            fill_method = options.get('fillMethod', 'mean')
            ddf = df_storage['cleaner_instance'].fill_missing_values(ddf, fill_method)
            
        if options.get('autoConvert'):
            ddf = df_storage['cleaner_instance'].auto_convert_types(ddf)
            
        if options.get('standardizeText'):
            ddf = df_storage['cleaner_instance'].standardize_text_columns(ddf)
        
        cleaned_ddf = ddf.persist()
        
        # Store cleaned data and get summary
        df_storage['cleaned'] = cleaned_ddf
        original_rows = len(df_storage['original'])
        cleaned_rows = len(cleaned_ddf)
        removed_rows = original_rows - cleaned_rows

        return jsonify({
            "status": "success",
            "message": "Data cleaning complete.",
            "original_rows": original_rows,
            "cleaned_rows": cleaned_rows,
            "removed_rows": removed_rows,
        })
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<format>', methods=['GET'])
def download_data(format):
    """Downloads the cleaned data in the specified format."""
    if df_storage['cleaned'] is None:
        return jsonify({"error": "No cleaned data available for download."}), 400
    
    try:
        filename = f"cleaned_{os.path.splitext(df_storage['filename'])[0]}.{format}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        if format == 'csv':
            df_storage['cleaned'].to_csv(filepath, single_file=True, index=False)
            mime_type = 'text/csv'
        elif format == 'json':
            # Dask to_json requires a different approach for a single file
            df_storage['cleaned'].to_json(filepath, orient='records', lines=False)
            mime_type = 'application/json'
        else:
            return jsonify({"error": "Invalid format"}), 400
        
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True, mimetype=mime_type)
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)