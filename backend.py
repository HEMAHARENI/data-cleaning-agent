# Enhanced backend with Dask integration and error handling
from fastapi import FastAPI, File, UploadFile, HTTPException, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.websockets import WebSocket, WebSocketDisconnect
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder
from sklearn.ensemble import IsolationForest
from scipy import stats
import json
import uuid
import os
import shutil
from typing import Dict, List, Any, Optional, Union
import datetime
import asyncio
import logging
import psutil
from pathlib import Path

# Dask imports with error handling
try:
    import dask.dataframe as dd
    import dask.array as da
    from dask.distributed import Client, LocalCluster
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    print("⚠️ Dask not available. Install with: pip install dask[complete]")

# AI Integration
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("⚠️ Google Generative AI not available. Install with: pip install google-generativeai")

app = FastAPI(title="DataPro Agent API", version="1.0.0")

# Initialize Gemini AI
gemini_model = None
if GEMINI_AVAILABLE:
    try:
        # Replace 'YOUR_API_KEY_HERE' with your actual Gemini API key
        api_key = 'AIzaSyAlA0jaqYYbrwI4dOwoy28C38fNkhHjc8oRE'  # ← Add your key here
        # api_key = os.getenv('GEMINI_API_KEY')  # ← Comment out this line
        
        if api_key:
            genai.configure(api_key=api_key)
            gemini_model = genai.GenerativeModel('gemini-pro')
            print("✅ Gemini AI initialized successfully")
        else:
            print("⚠️ GEMINI_API_KEY not found. Set it as environment variable for AI features")
    except Exception as e:
        print(f"⚠️ Failed to initialize Gemini AI: {e}")
        gemini_model = None

# Enhanced CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Enhanced data structures
file_store = {}
results_store = {}
dask_cleaners = {}
processing_status = {}
manual_edits = {}  # Store manual edits for missing values

# Create directories
temp_dir = Path("temp_uploads")
processed_dir = Path("processed")
temp_dir.mkdir(exist_ok=True)
processed_dir.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datapro.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global Dask client
dask_client = None
dask_cluster = None

class DaskEnhancedDataCleaner:
    """Enhanced Dask-based data cleaner with proper error handling"""
    
    def __init__(self, use_dask=True, n_workers=None):
        self.use_dask = use_dask and DASK_AVAILABLE
        self.n_workers = n_workers or min(psutil.cpu_count(), 4)
        self.client = None
        
    def get_client(self):
        """Get or create Dask client"""
        global dask_client
        if self.use_dask and not dask_client:
            try:
                dask_client = get_dask_client()
            except Exception as e:
                logger.warning(f"Failed to get Dask client: {e}")
                self.use_dask = False
        return dask_client
    
    def load_data(self, file_path: str, **kwargs):
        """Load data with automatic format detection"""
        file_path = Path(file_path)
        
        try:
            if self.use_dask:
                client = self.get_client()
                if file_path.suffix.lower() == '.csv':
                    return dd.read_csv(
                        str(file_path),
                        assume_missing=True,
                        blocksize="64MB",
                        **kwargs
                    )
                elif file_path.suffix.lower() == '.parquet':
                    return dd.read_parquet(str(file_path), **kwargs)
                else:
                    # Fallback to pandas for unsupported formats
                    df = self._load_with_pandas(file_path, **kwargs)
                    if df is not None:
                        return dd.from_pandas(df, npartitions=self.n_workers)
            else:
                return self._load_with_pandas(file_path, **kwargs)
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            # Fallback to pandas
            return self._load_with_pandas(file_path, **kwargs)
    
    def _load_with_pandas(self, file_path: Path, **kwargs):
        """Load data using pandas"""
        try:
            if file_path.suffix.lower() == '.csv':
                return pd.read_csv(file_path, **kwargs)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                return pd.read_excel(file_path, **kwargs)
            elif file_path.suffix.lower() == '.json':
                return pd.read_json(file_path, **kwargs)
            elif file_path.suffix.lower() == '.parquet':
                return pd.read_parquet(file_path, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
        except Exception as e:
            logger.error(f"Error loading with pandas: {e}")
            raise
    
    def detect_issues_dask(self, ddf):
        """Enhanced issue detection for Dask DataFrames"""
        try:
            issues = {}
            
            # Basic info
            if hasattr(ddf, 'compute'):
                # It's a Dask DataFrame
                issues['shape'] = (len(ddf), len(ddf.columns))
                issues['columns'] = list(ddf.columns)
                issues['dtypes'] = dict(ddf.dtypes.astype(str))
                issues['partitions'] = ddf.npartitions
                
                # Missing values analysis
                missing_counts = ddf.isnull().sum().compute()
                issues['missing_values'] = dict(missing_counts)
                issues['missing_percentage'] = dict((missing_counts / len(ddf) * 100).compute())
                
                # Duplicates
                issues['duplicates'] = int(ddf.duplicated().sum().compute())
                
                # Memory usage estimation
                sample = ddf.head(1000)
                memory_per_row = sample.memory_usage(deep=True).sum() / len(sample)
                issues['memory_usage_mb'] = float((memory_per_row * len(ddf)) / 1024**2)
                
                # Outlier detection for numeric columns
                numeric_cols = ddf.select_dtypes(include=[np.number]).columns
                outliers = {}
                for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
                    try:
                        q1 = ddf[col].quantile(0.25).compute()
                        q3 = ddf[col].quantile(0.75).compute()
                        iqr = q3 - q1
                        lower = q1 - 1.5 * iqr
                        upper = q3 + 1.5 * iqr
                        outlier_count = ((ddf[col] < lower) | (ddf[col] > upper)).sum().compute()
                        outliers[col] = int(outlier_count)
                    except Exception as e:
                        logger.warning(f"Error detecting outliers in {col}: {e}")
                        outliers[col] = 0
                
                issues['outliers'] = outliers
                
            else:
                # It's a regular DataFrame, use pandas methods
                issues = self._detect_issues_pandas(ddf)
            
            return issues
            
        except Exception as e:
            logger.error(f"Error in detect_issues_dask: {e}")
            return self._detect_issues_pandas(ddf)
    
    def _detect_issues_pandas(self, df):
        """Fallback pandas issue detection"""
        issues = {}
        issues['shape'] = df.shape
        issues['columns'] = list(df.columns)
        issues['dtypes'] = dict(df.dtypes.astype(str))
        issues['partitions'] = 'N/A (Pandas DataFrame)'
        issues['missing_values'] = dict(df.isnull().sum())
        issues['missing_percentage'] = dict((df.isnull().sum() / len(df) * 100))
        issues['duplicates'] = int(df.duplicated().sum())
        issues['memory_usage_mb'] = float(df.memory_usage(deep=True).sum() / 1024**2)
        
        # Outliers
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outliers = {}
        for col in numeric_cols:
            if df[col].notna().sum() > 0:
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outlier_count = ((df[col] < lower) | (df[col] > upper)).sum()
                outliers[col] = int(outlier_count)
        
        issues['outliers'] = outliers
        return issues

def get_dask_client():
    """Enhanced Dask client initialization"""
    global dask_client, dask_cluster
    
    if not DASK_AVAILABLE:
        return None
        
    if dask_client is None:
        try:
            # System resources
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / 1024**3
            
            # Conservative resource allocation
            n_workers = min(cpu_count, 6)
            memory_per_worker = max(1, int(memory_gb * 0.5 / n_workers))
            
            # Create cluster
            dask_cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=2,
                memory_limit=f'{memory_per_worker}GB',
                dashboard_address=':8787',
                silence_logs=False,
                processes=True  # Use processes instead of threads for better isolation
            )
            
            dask_client = Client(dask_cluster)
            logger.info(f"Dask cluster initialized: {n_workers} workers, {memory_per_worker}GB each")
            
        except Exception as e:
            logger.error(f"Failed to initialize Dask cluster: {e}")
            dask_client = None
            dask_cluster = None
    
    return dask_client

def _to_jsonable(value: Any) -> Any:
    """Enhanced JSON serialization"""
    try:
        import numpy as np
        import pandas as pd
    except Exception:
        np = None
        pd = None

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    # Handle numpy types
    if np is not None and isinstance(value, np.generic):
        if np.isnan(value):
            return None
        return value.item()

    # Handle pandas types
    if pd is not None:
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, (pd.Series, pd.Index)):
            return [_to_jsonable(v) for v in value.tolist()]
        if isinstance(value, pd.DataFrame):
            return value.to_dict('records')

    # Handle Dask objects
    if hasattr(value, 'compute') and callable(getattr(value, 'compute')):
        try:
            return _to_jsonable(value.compute())
        except Exception as e:
            logger.warning(f"Failed to compute Dask object: {e}")
            return str(value)

    # Handle containers
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in list(value)]

    return str(value)

def check_file_size(file_path: str) -> Dict[str, Any]:
    """Enhanced file size checking with better Dask recommendations"""
    try:
        file_size_mb = os.path.getsize(file_path) / 1024**2
        memory_gb = psutil.virtual_memory().total / 1024**3
        available_memory_gb = psutil.virtual_memory().available / 1024**3
        
        # More intelligent Dask threshold
        use_dask_threshold = min(50, available_memory_gb * 0.1 * 1024)  # 50MB or 10% of available RAM
        
        recommendations = {
            "file_size_mb": round(file_size_mb, 2),
            "use_dask": file_size_mb > use_dask_threshold and DASK_AVAILABLE,
            "estimated_memory_usage": round(file_size_mb * 3, 2),  # More conservative estimate
            "memory_available": round(available_memory_gb, 2),
            "recommended_workers": min(psutil.cpu_count(), 6),
            "processing_method": "dask" if (file_size_mb > use_dask_threshold and DASK_AVAILABLE) else "pandas",
            "performance_tier": "large" if file_size_mb > 500 else "medium" if file_size_mb > 50 else "small"
        }
        
        # Add warnings
        if file_size_mb > 1000:
            recommendations["warning"] = "Very large file - processing may take significant time"
        elif file_size_mb > 200:
            recommendations["warning"] = "Large file - Dask processing recommended"
        elif not DASK_AVAILABLE and file_size_mb > 100:
            recommendations["warning"] = "Large file detected but Dask not available - install dask[complete]"
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error checking file size: {e}")
        return {
            "file_size_mb": 0,
            "use_dask": False,
            "error": str(e),
            "processing_method": "pandas"
        }

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Enhanced file upload with better error handling"""
    try:
        logger.info(f"Uploading file: {file.filename}")
        
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")
        
        allowed_extensions = {'.csv', '.xlsx', '.xls', '.json', '.parquet'}
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type. Supported: {', '.join(allowed_extensions)}"
            )
        
        # Generate file ID and path
        file_id = str(uuid.uuid4())
        file_path = temp_dir / f"{file_id}_{file.filename}"
        
        # Stream upload to disk
        try:
            with open(file_path, "wb") as f:
                while chunk := await file.read(1024 * 1024):  # 1MB chunks
                    f.write(chunk)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
        
        logger.info(f"File saved to: {file_path}")
        
        # Analyze file and determine processing method
        file_info = check_file_size(str(file_path))
        use_dask = file_info.get("use_dask", False)
        
        # Initialize cleaner
        cleaner = DaskEnhancedDataCleaner(use_dask=use_dask, n_workers=file_info.get("recommended_workers", 4))
        
        try:
            # Load data
            data = cleaner.load_data(str(file_path))
            
            if data is None:
                raise HTTPException(status_code=400, detail="Failed to load data from file")
            
            # Get basic information
            if hasattr(data, 'compute'):
                # Dask DataFrame
                total_rows = len(data)
                columns = list(data.columns)
                dtypes = dict(data.dtypes.astype(str))
                missing_values = int(data.isnull().sum().sum().compute())
                is_dask = True
                dask_cleaners[file_id] = cleaner
            else:
                # Pandas DataFrame
                total_rows = len(data)
                columns = list(data.columns)
                dtypes = dict(data.dtypes.astype(str))
                missing_values = int(data.isnull().sum().sum())
                is_dask = False
            
            # Store file information
            file_store[file_id] = {
                "filename": file.filename,
                "path": str(file_path),
                "columns": columns,
                "dtypes": dtypes,
                "total_rows": total_rows,
                "is_dask": is_dask,
                "processing_method": "dask" if is_dask else "pandas",
                "file_info": file_info,
                "dataframe" if not is_dask else "dask_dataframe": data
            }
            
            # Calculate completion rate
            total_cells = total_rows * len(columns)
            completion_rate = ((total_cells - missing_values) / total_cells * 100) if total_cells > 0 else 100
            
            response = {
                "file_id": file_id,
                "total_rows": total_rows,
                "missing_values": missing_values,
                "completion_rate": round(completion_rate, 2),
                "columns": columns,
                "dtypes": dtypes,
                "file_info": file_info,
                "processing_method": "dask" if is_dask else "pandas",
                "status": "success"
            }
            
            logger.info(f"Upload successful: {total_rows} rows, {missing_values} missing values")
            return _to_jsonable(response)
            
        except Exception as e:
            # Clean up file on processing error
            if file_path.exists():
                file_path.unlink()
            logger.error(f"Error processing uploaded file: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in upload: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/api/analyze")
async def analyze_data(data: Dict[str, Any] = Body(...)):
    """Enhanced data analysis with better error handling"""
    try:
        file_id = data.get("file_id")
        
        if not file_id or file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        is_dask = file_info.get("is_dask", False)
        
        try:
            if is_dask:
                cleaner = dask_cleaners.get(file_id)
                if not cleaner:
                    raise HTTPException(status_code=400, detail="Dask cleaner not available")
                
                ddf = file_info["dask_dataframe"]
                issues = cleaner.detect_issues_dask(ddf)
                
            else:
                df = file_info["dataframe"]
                cleaner = DaskEnhancedDataCleaner(use_dask=False)
                issues = cleaner._detect_issues_pandas(df)
            
            # Add data quality score
            issues["data_quality_score"] = calculate_data_quality_score(issues)
            
            # Add recommendations
            issues["recommendations"] = generate_data_recommendations(issues)
            
            return _to_jsonable({
                "file_id": file_id,
                "analysis": issues,
                "processing_method": "dask" if is_dask else "pandas",
                "status": "success"
            })
            
        except Exception as e:
            logger.error(f"Error analyzing data: {e}")
            raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")

def calculate_data_quality_score(issues: dict) -> dict:
    """Calculate comprehensive data quality score"""
    try:
        total_rows = issues.get('shape', [0, 0])[0]
        total_cols = issues.get('shape', [0, 0])[1]
        missing_values = sum(issues.get('missing_values', {}).values())
        duplicates = issues.get('duplicates', 0)
        
        if total_rows == 0 or total_cols == 0:
            return {"overall": 0, "completeness": 0, "uniqueness": 0, "consistency": 0}
        
        # Completeness score (0-10)
        total_cells = total_rows * total_cols
        completeness = ((total_cells - missing_values) / total_cells * 10) if total_cells > 0 else 10
        
        # Uniqueness score (0-10)
        uniqueness = ((total_rows - duplicates) / total_rows * 10) if total_rows > 0 else 10
        
        # Consistency score (simplified, based on data types)
        consistency = 8  # Placeholder - could be enhanced with more sophisticated checks
        
        # Overall score
        overall = (completeness + uniqueness + consistency) / 3
        
        return {
            "overall": round(overall, 1),
            "completeness": round(completeness, 1),
            "uniqueness": round(uniqueness, 1),
            "consistency": round(consistency, 1)
        }
        
    except Exception as e:
        logger.error(f"Error calculating quality score: {e}")
        return {"overall": 0, "completeness": 0, "uniqueness": 0, "consistency": 0}

def generate_data_recommendations(issues: dict) -> List[str]:
    """Generate specific recommendations based on analysis"""
    recommendations = []
    
    try:
        missing_values = sum(issues.get('missing_values', {}).values())
        duplicates = issues.get('duplicates', 0)
        total_rows = issues.get('shape', [0, 0])[0]
        
        if missing_values > 0:
            missing_percentage = (missing_values / (total_rows * len(issues.get('columns', [])))) * 100
            if missing_percentage > 10:
                recommendations.append("High missing data detected - consider data collection improvement")
            elif missing_percentage > 5:
                recommendations.append("Moderate missing data - use appropriate imputation strategies")
            else:
                recommendations.append("Low missing data - safe to fill or drop missing values")
        
        if duplicates > 0:
            dup_percentage = (duplicates / total_rows) * 100
            if dup_percentage > 5:
                recommendations.append("Significant duplicate data - investigate data collection process")
            else:
                recommendations.append("Remove duplicate rows to improve data quality")
        
        # Memory recommendations
        memory_mb = issues.get('memory_usage_mb', 0)
        if memory_mb > 500:
            recommendations.append("Large dataset - consider using Dask for better performance")
        elif memory_mb > 100:
            recommendations.append("Medium dataset - optimize data types to reduce memory usage")
        
        if not recommendations:
            recommendations.append("Data quality looks good - ready for processing")
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        recommendations.append("Unable to generate specific recommendations")
    
    return recommendations

@app.post("/api/manual-edit")
async def save_manual_edit(data: Dict[str, Any] = Body(...)):
    """Save manual edits for missing values"""
    try:
        file_id = data.get("file_id")
        row_index = data.get("row_index")
        column = data.get("column")
        value = data.get("value")
        
        if not all([file_id, row_index is not None, column, value is not None]):
            raise HTTPException(status_code=400, detail="Missing required fields")
        
        if file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Store manual edit
        if file_id not in manual_edits:
            manual_edits[file_id] = {}
        
        edit_key = f"{row_index}_{column}"
        manual_edits[file_id][edit_key] = value
        
        return {"status": "success", "message": "Manual edit saved"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving manual edit: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save edit: {str(e)}")

@app.get("/api/missing-values/{file_id}")
async def get_missing_values(file_id: str):
    """Get detailed missing values information for manual editing"""
    try:
        if file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        is_dask = file_info.get("is_dask", False)
        
        if is_dask:
            ddf = file_info["dask_dataframe"]
            # Sample data for missing value inspection
            sample_size = min(1000, len(ddf))
            sample_df = ddf.head(sample_size)
        else:
            sample_df = file_info["dataframe"].head(1000)  # Limit to first 1000 rows
        
        missing_info = []
        columns = sample_df.columns
        
        for idx, row in sample_df.iterrows():
            for col in columns:
                if pd.isna(row[col]) or row[col] == '' or row[col] is None:
                    # Get context (surrounding values)
                    context = []
                    for context_col in columns[:5]:  # First 5 columns as context
                        context.append(row[context_col] if pd.notna(row[context_col]) else None)
                    
                    missing_info.append({
                        "row_index": int(idx),
                        "column": col,
                        "context": context,
                        "existing_edit": manual_edits.get(file_id, {}).get(f"{idx}_{col}")
                    })
        
        return {
            "file_id": file_id,
            "missing_values": missing_info,
            "total_missing": len(missing_info),
            "columns": list(columns)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting missing values: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get missing values: {str(e)}")

@app.post("/api/process")
async def process_data(data: Dict[str, Any] = Body(...)):
    """Enhanced data processing with manual edits support"""
    try:
        file_id = data.get("file_id")
        options = data.get("options", {})
        
        if not file_id or file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        is_dask = file_info.get("is_dask", False)
        
        processing_log = []
        
        if is_dask:
            cleaner = dask_cleaners.get(file_id)
            if not cleaner:
                raise HTTPException(status_code=400, detail="Dask cleaner not available")
            
            ddf = file_info["dask_dataframe"]
            original_rows = len(ddf)
            
            # Apply manual edits first
            if file_id in manual_edits:
                # Convert to pandas for manual edits, then back to dask
                temp_df = ddf.compute()
                edit_count = 0
                
                for edit_key, value in manual_edits[file_id].items():
                    row_idx, col = edit_key.split('_', 1)
                    row_idx = int(row_idx)
                    if row_idx < len(temp_df) and col in temp_df.columns:
                        temp_df.iloc[row_idx, temp_df.columns.get_loc(col)] = value
                        edit_count += 1
                
                if edit_count > 0:
                    ddf = dd.from_pandas(temp_df, npartitions=ddf.npartitions)
                    processing_log.append(f"Applied {edit_count} manual edits")
            
            # Process with Dask
            processed_ddf = await process_with_dask(ddf, options, processing_log)
            cleaned_rows = len(processed_ddf)
            
            # Update stored dataframe
            file_info["dask_dataframe"] = processed_ddf
            
            # Get preview
            preview_data = processed_ddf.head(10).to_dict('records')
            
        else:
            df = file_info["dataframe"].copy()
            original_rows = len(df)
            
            # Apply manual edits
            if file_id in manual_edits:
                edit_count = 0
                for edit_key, value in manual_edits[file_id].items():
                    row_idx, col = edit_key.split('_', 1)
                    row_idx = int(row_idx)
                    if row_idx < len(df) and col in df.columns:
                        df.iloc[row_idx, df.columns.get_loc(col)] = value
                        edit_count += 1
                
                if edit_count > 0:
                    processing_log.append(f"Applied {edit_count} manual edits")
            
            # Process with pandas
            processed_df = process_with_pandas(df, options, processing_log)
            cleaned_rows = len(processed_df)
            
            # Store processed data
            file_info["dataframe"] = processed_df
            preview_data = processed_df.head(10).to_dict('records')
        
        # Generate processed file ID
        processed_file_id = str(uuid.uuid4())
        file_store[processed_file_id] = {
            "filename": f"processed_{file_info['filename']}",
            "original_file_id": file_id,
            "is_dask": is_dask,
            "processing_method": file_info["processing_method"],
            **file_info
        }
        
        # Calculate final statistics
        removed_rows = original_rows - cleaned_rows
        missing_values_final = 0
        
        if is_dask:
            missing_values_final = int(file_info["dask_dataframe"].isnull().sum().sum().compute())
        else:
            missing_values_final = int(file_info["dataframe"].isnull().sum().sum())
        
        return _to_jsonable({
            "file_id": processed_file_id,
            "original_rows": original_rows,
            "cleaned_rows": cleaned_rows,
            "removed_rows": removed_rows,
            "processed_columns": len(file_info["columns"]),
            "missing_values_final": missing_values_final,
            "processing_log": processing_log,
            "preview_data": preview_data,
            "success": True,
            "processing_method": "dask" if is_dask else "pandas"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

async def process_with_dask(ddf, options, processing_log):
    """Process data using Dask with comprehensive options"""
    try:
        # Missing values handling
        missing_opts = options.get("missing_values", {})
        if missing_opts.get("fill"):
            method = missing_opts.get("method", "mean")
            numeric_cols = ddf.select_dtypes(include=[np.number]).columns
            
            if method == "mean":
                ddf[numeric_cols] = ddf[numeric_cols].fillna(ddf[numeric_cols].mean())
            elif method == "median":
                ddf[numeric_cols] = ddf[numeric_cols].fillna(ddf[numeric_cols].median())
            elif method == "mode":
                for col in numeric_cols:
                    mode_val = ddf[col].mode().iloc[0] if len(ddf[col].mode()) > 0 else 0
                    ddf[col] = ddf[col].fillna(mode_val)
            
            # Fill non-numeric columns
            non_numeric_cols = ddf.select_dtypes(exclude=[np.number]).columns
            for col in non_numeric_cols:
                mode_val = ddf[col].mode().iloc[0] if len(ddf[col].mode()) > 0 else "Unknown"
                ddf[col] = ddf[col].fillna(mode_val)
                
            processing_log.append(f"Filled missing values using {method} method")
        
        if missing_opts.get("drop"):
            ddf = ddf.dropna()
            processing_log.append("Dropped rows with remaining missing values")
        
        # Duplicates handling
        dup_opts = options.get("duplicates", {})
        if dup_opts.get("remove"):
            keep = "first" if dup_opts.get("keep_first", True) else "last"
            original_count = len(ddf)
            ddf = ddf.drop_duplicates(keep=keep)
            removed_count = original_count - len(ddf)
            processing_log.append(f"Removed {removed_count} duplicate rows")
        
        # Data type optimization
        dtype_opts = options.get("data_types", {})
        if dtype_opts.get("optimize"):
            # Optimize numeric types
            numeric_cols = ddf.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if ddf[col].dtype == 'float64':
                    if ddf[col].min() >= np.iinfo(np.int32).min and ddf[col].max() <= np.iinfo(np.int32).max:
                        if (ddf[col] % 1 == 0).all().compute():
                            ddf[col] = ddf[col].astype('int32')
                    else:
                        ddf[col] = ddf[col].astype('float32')
            
            processing_log.append("Optimized data types for memory efficiency")
        
        if dtype_opts.get("standardize_text"):
            text_cols = ddf.select_dtypes(include=['object']).columns
            for col in text_cols:
                ddf[col] = ddf[col].astype(str).str.lower().str.strip()
            processing_log.append(f"Standardized text in {len(text_cols)} columns")
        
        # Advanced preprocessing
        prep_opts = options.get("preprocessing", {})
        if prep_opts.get("normalize"):
            numeric_cols = ddf.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                # Use Dask-compatible scaling
                means = ddf[numeric_cols].mean()
                stds = ddf[numeric_cols].std()
                ddf[numeric_cols] = (ddf[numeric_cols] - means) / stds
                processing_log.append(f"Normalized {len(numeric_cols)} numeric columns")
        
        return ddf
        
    except Exception as e:
        logger.error(f"Error in Dask processing: {e}")
        raise

def process_with_pandas(df, options, processing_log):
    """Process data using Pandas with comprehensive options"""
    try:
        # Missing values handling
        missing_opts = options.get("missing_values", {})
        if missing_opts.get("fill"):
            method = missing_opts.get("method", "mean")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if df[col].isnull().any():
                    if method == "mean":
                        df[col].fillna(df[col].mean(), inplace=True)
                    elif method == "median":
                        df[col].fillna(df[col].median(), inplace=True)
                    elif method == "mode":
                        mode_val = df[col].mode().iloc[0] if not df[col].mode().empty else 0
                        df[col].fillna(mode_val, inplace=True)
                    elif method == "forward":
                        df[col].fillna(method='ffill', inplace=True)
                    elif method == "backward":
                        df[col].fillna(method='bfill', inplace=True)
            
            # Fill non-numeric columns
            non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
            for col in non_numeric_cols:
                if df[col].isnull().any():
                    mode_val = df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown"
                    df[col].fillna(mode_val, inplace=True)
                    
            processing_log.append(f"Filled missing values using {method} method")
        
        if missing_opts.get("drop"):
            df.dropna(inplace=True)
            processing_log.append("Dropped rows with remaining missing values")
        
        # Duplicates handling
        dup_opts = options.get("duplicates", {})
        if dup_opts.get("remove"):
            keep = "first" if dup_opts.get("keep_first", True) else "last"
            duplicates_count = df.duplicated().sum()
            df.drop_duplicates(keep=keep, inplace=True)
            processing_log.append(f"Removed {duplicates_count} duplicate rows")
        
        # Data type optimization
        dtype_opts = options.get("data_types", {})
        if dtype_opts.get("optimize"):
            df = df.infer_objects()
            processing_log.append("Optimized data types")
        
        if dtype_opts.get("standardize_text"):
            text_cols = df.select_dtypes(include=['object']).columns
            for col in text_cols:
                df[col] = df[col].astype(str).str.lower().str.strip()
            processing_log.append(f"Standardized text in {len(text_cols)} columns")
        
        # Outlier handling
        outlier_opts = options.get("outliers", {})
        if outlier_opts.get("detect") or outlier_opts.get("remove"):
            method = outlier_opts.get("method", "iqr")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            outliers_removed = 0
            
            for col in numeric_cols:
                if method == "iqr":
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                elif method == "zscore":
                    z_scores = np.abs(stats.zscore(df[col].dropna()))
                    outlier_mask = pd.Series([False] * len(df), index=df.index)
                    outlier_mask[df[col].notna()] = z_scores > 3
                elif method == "isolation":
                    iso_forest = IsolationForest(contamination=0.1, random_state=42)
                    outlier_predictions = iso_forest.fit_predict(df[[col]].dropna())
                    outlier_mask = pd.Series([False] * len(df), index=df.index)
                    outlier_mask[df[col].notna()] = outlier_predictions == -1
                
                if outlier_opts.get("remove"):
                    outliers_count = outlier_mask.sum()
                    outliers_removed += outliers_count
                    df = df[~outlier_mask]
            
            if outliers_removed > 0:
                processing_log.append(f"Removed {outliers_removed} outliers using {method} method")
        
        # Preprocessing
        prep_opts = options.get("preprocessing", {})
        if prep_opts.get("normalize"):
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            scaler = StandardScaler()
            df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
            processing_log.append(f"Normalized {len(numeric_cols)} numeric columns")
        
        if prep_opts.get("one_hot_encode"):
            categorical_cols = df.select_dtypes(include=['object']).columns[:5]
            for col in categorical_cols:
                if df[col].nunique() <= 10:
                    dummies = pd.get_dummies(df[col], prefix=col, drop_first=True)
                    df = pd.concat([df, dummies], axis=1)
                    df.drop(col, axis=1, inplace=True)
            processing_log.append(f"One-hot encoded categorical columns")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in pandas processing: {e}")
        raise

@app.get("/api/preview/{file_id}")
async def get_preview(file_id: str, rows: int = 10):
    """Enhanced preview with error handling"""
    try:
        if file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        is_dask = file_info.get("is_dask", False)
        
        if is_dask:
            ddf = file_info["dask_dataframe"]
            preview_data = ddf.head(rows).to_dict('records')
        else:
            df = file_info["dataframe"]
            preview_data = df.head(rows).to_dict('records')
        
        return _to_jsonable({
            "file_id": file_id,
            "preview_data": preview_data,
            "total_rows": file_info["total_rows"],
            "processing_method": "dask" if is_dask else "pandas"
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting preview: {e}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

@app.get("/api/dask/status")
async def get_dask_status():
    """Enhanced Dask cluster status"""
    try:
        if not DASK_AVAILABLE:
            return {
                "status": "unavailable",
                "message": "Dask not installed. Install with: pip install dask[complete]",
                "workers": 0,
                "dashboard_link": None,
                "system_resources": get_system_resources()
            }
        
        client = get_dask_client()
        if not client:
            return {
                "status": "inactive",
                "message": "Dask cluster not initialized",
                "workers": 0,
                "dashboard_link": "http://localhost:8787",
                "system_resources": get_system_resources()
            }
        
        try:
            cluster_info = client.scheduler_info()
            workers = cluster_info.get('workers', {})
            
            worker_details = []
            for worker_id, worker_info in workers.items():
                worker_details.append({
                    "worker_id": worker_id.split('-')[-1][:8],
                    "memory_limit": format_bytes(worker_info.get('memory_limit', 0)),
                    "memory_used": format_bytes(worker_info.get('memory', 0)),
                    "nthreads": worker_info.get('nthreads', 0),
                    "status": worker_info.get('status', 'unknown')
                })
        
        except Exception as e:
            logger.warning(f"Error getting cluster info: {e}")
            workers = {}
            worker_details = []
        
        return {
            "status": "active",
            "dashboard_link": getattr(client, 'dashboard_link', 'http://localhost:8787'),
            "workers": len(workers),
            "worker_details": worker_details,
            "system_resources": get_system_resources(),
            "cluster_type": "LocalCluster"
        }
        
    except Exception as e:
        logger.error(f"Error getting Dask status: {e}")
        return {
            "status": "error",
            "message": str(e),
            "workers": 0,
            "dashboard_link": None,
            "system_resources": get_system_resources()
        }

def get_system_resources():
    """Get system resource information"""
    try:
        memory = psutil.virtual_memory()
        return {
            "total_memory_gb": round(memory.total / 1024**3, 2),
            "available_memory_gb": round(memory.available / 1024**3, 2),
            "used_memory_gb": round(memory.used / 1024**3, 2),
            "memory_percent": round(memory.percent, 1),
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": round(psutil.cpu_percent(interval=1), 1)
        }
    except Exception as e:
        logger.error(f"Error getting system resources: {e}")
        return {
            "total_memory_gb": 0,
            "available_memory_gb": 0,
            "used_memory_gb": 0,
            "memory_percent": 0,
            "cpu_count": 0,
            "cpu_percent": 0
        }

def format_bytes(bytes_value):
    """Format bytes to human readable format"""
    if bytes_value == 0:
        return "0 B"
    
    try:
        sizes = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while bytes_value >= 1024 and i < len(sizes) - 1:
            bytes_value /= 1024
            i += 1
        return f"{bytes_value:.1f} {sizes[i]}"
    except:
        return "0 B"

@app.get("/api/download/{file_id}")
async def download_file(file_id: str, format: str = "csv"):
    """Enhanced download with better error handling"""
    try:
        if file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        is_dask = file_info.get("is_dask", False)
        filename = file_info["filename"]
        
        base_name = Path(filename).stem
        
        if is_dask:
            ddf = file_info["dask_dataframe"]
            
            if format == "csv":
                file_path = processed_dir / f"{base_name}.csv"
                ddf.to_csv(str(file_path), index=False, single_file=True)
                
            elif format == "parquet":
                file_path = processed_dir / f"{base_name}.parquet"
                ddf.to_parquet(str(file_path))
                
            else:
                # Convert to pandas for other formats
                df = ddf.compute()
                file_path = await _save_pandas_file(df, base_name, format)
        else:
            df = file_info["dataframe"]
            file_path = await _save_pandas_file(df, base_name, format)
        
        if not file_path.exists():
            raise HTTPException(status_code=500, detail="Failed to create download file")
        
        return FileResponse(
            str(file_path),
            filename=f"{base_name}.{format}",
            media_type=get_media_type(format)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

async def _save_pandas_file(df: pd.DataFrame, base_name: str, format: str):
    """Save pandas DataFrame in specified format"""
    try:
        if format == "csv":
            file_path = processed_dir / f"{base_name}.csv"
            df.to_csv(file_path, index=False)
        elif format == "json":
            file_path = processed_dir / f"{base_name}.json"
            df.to_json(file_path, orient='records', indent=2)
        elif format == "excel":
            file_path = processed_dir / f"{base_name}.xlsx"
            df.to_excel(file_path, index=False)
        elif format == "parquet":
            file_path = processed_dir / f"{base_name}.parquet"
            df.to_parquet(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return file_path
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise

def get_media_type(format: str) -> str:
    """Get media type for file format"""
    media_types = {
        "csv": "text/csv",
        "json": "application/json",
        "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "parquet": "application/octet-stream"
    }
    return media_types.get(format, "application/octet-stream")

@app.post("/api/chat")
async def chat_endpoint(data: Dict[str, Any] = Body(...)):
    """Enhanced AI chatbot with better context awareness"""
    try:
        message = data.get("message", "").strip()
        context = data.get("context", {})
        file_id = context.get("file_id")
        
        # Get data context
        data_context = {}
        if file_id and file_id in file_store:
            file_info = file_store[file_id]
            is_dask = file_info.get("is_dask", False)
            
            if is_dask:
                cleaner = dask_cleaners.get(file_id)
                if cleaner:
                    try:
                        ddf = file_info["dask_dataframe"]
                        issues = cleaner.detect_issues_dask(ddf)
                        data_context = {
                            **issues,
                            "processing_method": "Dask",
                            "file_size_mb": file_info.get("file_info", {}).get("file_size_mb", 0)
                        }
                    except Exception as e:
                        logger.warning(f"Error getting Dask context: {e}")
            else:
                df = file_info["dataframe"]
                cleaner = DaskEnhancedDataCleaner(use_dask=False)
                issues = cleaner._detect_issues_pandas(df)
                data_context = {
                    **issues,
                    "processing_method": "Pandas",
                    "file_size_mb": file_info.get("file_info", {}).get("file_size_mb", 0)
                }
        
        # Generate response
        response = await generate_intelligent_response(message, data_context, context)
        suggestions = generate_actionable_suggestions(data_context, message)
        
        return _to_jsonable({
            "response": response,
            "context": data_context,
            "suggestions": suggestions,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return {
            "response": "I'm experiencing some technical difficulties. Please try again in a moment.",
            "error": str(e),
            "suggestions": []
        }
@app.post("/api/force-dask")
async def force_dask_processing(data: Dict[str, Any] = Body(...)):
    """Force switch to DASK processing for any dataset"""
    try:
        file_id = data.get("file_id")
        
        if not file_id or file_id not in file_store:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_info = file_store[file_id]
        file_path = file_info["path"]
        
        # Initialize DASK cleaner regardless of file size
        cleaner = DaskEnhancedDataCleaner(use_dask=True, n_workers=4)
        
        # Force load with DASK
        ddf = cleaner.load_data(file_path)
        
        if hasattr(ddf, 'compute'):
            # Successfully loaded as DASK DataFrame
            file_info["is_dask"] = True
            file_info["processing_method"] = "dask" 
            file_info["dask_dataframe"] = ddf
            dask_cleaners[file_id] = cleaner
            
            # Update current data structure
            file_info.update({
                "total_rows": len(ddf),
                "columns": list(ddf.columns),
                "dtypes": dict(ddf.dtypes.astype(str))
            })
            
            return {
                "message": "Successfully switched to DASK processing",
                "partitions": ddf.npartitions,
                "processing_method": "dask",
                "status": "success"
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to initialize DASK processing")
            
    except Exception as e:
        logger.error(f"Force DASK error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch to DASK: {str(e)}")
async def generate_intelligent_response(message: str, data_context: dict, user_context: dict) -> str:
    """Generate intelligent AI response"""
    try:
        if gemini_model and data_context:
            return await get_gemini_response(message, data_context, user_context)
        else:
            return get_rule_based_response(message, data_context)
    except Exception as e:
        logger.error(f"AI response error: {e}")
        return get_rule_based_response(message, data_context)

async def get_gemini_response(message: str, data_context: dict, user_context: dict) -> str:
    """Get enhanced response from Gemini AI"""
    try:
        missing_values = sum(data_context.get('missing_values', {}).values())
        duplicates = data_context.get('duplicates', 0)
        total_rows = data_context.get('shape', [0, 0])[0]
        quality_score = data_context.get('data_quality_score', {})
        
        prompt = f"""
        You are an expert data scientist assistant. Provide specific, actionable advice.

        User Question: {message}

        Dataset Analysis:
        - Rows: {total_rows:,}, Columns: {len(data_context.get('columns', []))}
        - Missing Values: {missing_values:,} ({((missing_values/(total_rows*len(data_context.get('columns', [])))) * 100):.1f}% of total cells)
        - Duplicates: {duplicates:,} ({(duplicates/total_rows*100) if total_rows > 0 else 0:.1f}%)
        - Data Quality Score: {quality_score.get('overall', 0)}/10
        - Processing Method: {data_context.get('processing_method', 'Unknown')}
        - Memory Usage: {data_context.get('memory_usage_mb', 0):.1f} MB

        Provide a helpful response that:
        1. Directly addresses the user's question
        2. References specific data characteristics
        3. Suggests concrete next steps
        4. Explains the reasoning behind recommendations
        5. Is concise but comprehensive (max 200 words)

        Use a professional but friendly tone. Include specific numbers from the analysis.
        """
        
        response = gemini_model.generate_content(prompt)
        return response.text
        
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return get_rule_based_response(message, data_context)

def get_rule_based_response(message: str, data_context: dict) -> str:
    """Enhanced rule-based response system"""
    msg = message.lower()
    
    if not data_context:
        return "Please upload a dataset first so I can provide specific insights about your data!"
    
    missing_count = sum(data_context.get('missing_values', {}).values())
    duplicates = data_context.get('duplicates', 0)
    total_rows = data_context.get('shape', [0, 0])[0]
    
    if any(word in msg for word in ['analyze', 'analysis', 'summary']):
        return f"""Data Analysis Summary:
        
Your dataset has {total_rows:,} rows with {missing_count:,} missing values and {duplicates:,} duplicates.
Data quality score: {data_context.get('data_quality_score', {}).get('overall', 0)}/10

Key issues found:
{format_key_issues(data_context)}

Recommended actions:
{format_recommendations(data_context)}
"""
    
    if any(word in msg for word in ['missing', 'null', 'empty']):
        if missing_count == 0:
            return "Excellent! Your dataset has no missing values. This indicates high data quality and completeness."
        else:
            missing_pct = (missing_count / (total_rows * len(data_context.get('columns', []))))  * 100
            severity = "high" if missing_pct > 10 else "moderate" if missing_pct > 5 else "low"
            return f"""Missing Values Analysis:
            
Found {missing_count:,} missing values ({missing_pct:.1f}% of total data) - {severity} severity.

Column breakdown:
{format_missing_by_column(data_context)}

Recommended approach: {'Consider data collection improvement' if missing_pct > 10 else 'Use appropriate imputation strategies'}
"""
    
    return "I can help you analyze your data, handle missing values, remove duplicates, and optimize data quality. What specific aspect would you like to focus on?"

def format_key_issues(data_context: dict) -> str:
    """Format key data issues"""
    issues = []
    missing_count = sum(data_context.get('missing_values', {}).values())
    duplicates = data_context.get('duplicates', 0)
    
    if missing_count > 0:
        issues.append(f"• {missing_count:,} missing values need attention")
    if duplicates > 0:
        issues.append(f"• {duplicates:,} duplicate rows found")
    
    return '\n'.join(issues) if issues else "• No major data quality issues detected"

def format_missing_by_column(data_context: dict) -> str:
    """Format missing values by column"""
    missing_vals = data_context.get('missing_values', {})
    return '\n'.join([f"• {col}: {count:,}" for col, count in missing_vals.items() if count > 0][:5])

def format_recommendations(data_context: dict) -> str:
    """Format recommendations"""
    recommendations = data_context.get('recommendations', [])
    return '\n'.join([f"• {rec}" for rec in recommendations[:3]])

def generate_actionable_suggestions(data_context: dict, message: str) -> List[dict]:
    """Generate context-aware actionable suggestions"""
    suggestions = []
    
    if not data_context:
        return suggestions
    
    missing_count = sum(data_context.get('missing_values', {}).values())
    duplicates = data_context.get('duplicates', 0)
    memory_mb = data_context.get('memory_usage_mb', 0)
    
    if missing_count > 0:
        suggestions.append({
            "action": "Handle Missing Values",
            "description": f"Fill or remove {missing_count:,} missing values",
            "priority": "high" if missing_count > 100 else "medium",
            "icon": "🔧",
            "category": "Data Quality"
        })
    
    if duplicates > 0:
        suggestions.append({
            "action": "Remove Duplicates",
            "description": f"Clean {duplicates:,} duplicate rows",
            "priority": "high" if duplicates > 50 else "medium",
            "icon": "🔄",
            "category": "Data Quality"
        })
    
    if memory_mb > 100:
        suggestions.append({
            "action": "Optimize Performance",
            "description": f"Reduce {memory_mb:.0f}MB memory usage with Dask",
            "priority": "medium",
            "icon": "⚡",
            "category": "Performance"
        })
    
    return suggestions

@app.delete("/api/cleanup")
async def cleanup_files():
    """Enhanced cleanup with better error handling"""
    try:
        cleanup_count = 0
        
        # Clean up temp files
        if temp_dir.exists():
            for file_path in temp_dir.glob("*"):
                try:
                    file_path.unlink()
                    cleanup_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete {file_path}: {e}")
        
        # Clean up processed files
        if processed_dir.exists():
            for file_path in processed_dir.glob("*"):
                try:
                    file_path.unlink()
                    cleanup_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete {file_path}: {e}")
        
        # Clear in-memory stores
        file_store.clear()
        results_store.clear()
        dask_cleaners.clear()
        processing_status.clear()
        manual_edits.clear()
        
        # Close Dask client if exists
        global dask_client, dask_cluster
        if dask_client:
            try:
                dask_client.close()
                dask_client = None
            except Exception as e:
                logger.warning(f"Error closing Dask client: {e}")
        
        if dask_cluster:
            try:
                dask_cluster.close()
                dask_cluster = None
            except Exception as e:
                logger.warning(f"Error closing Dask cluster: {e}")
        
        return {
            "message": f"Cleanup completed successfully - {cleanup_count} files removed",
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.post("/api/dask/initialize")
async def initialize_dask_cluster():
    """Initialize Dask cluster with enhanced error handling"""
    try:
        if not DASK_AVAILABLE:
            raise HTTPException(
                status_code=400, 
                detail="Dask not available. Install with: pip install dask[complete]"
            )
        
        client = get_dask_client()
        if client:
            cluster_info = client.scheduler_info()
            return {
                "status": "success",
                "message": "Dask cluster initialized successfully",
                "dashboard_link": getattr(client, 'dashboard_link', 'http://localhost:8787'),
                "workers": len(cluster_info.get('workers', {})),
                "cluster_type": "LocalCluster"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to initialize Dask cluster")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cluster initialization error: {e}")
        raise HTTPException(status_code=500, detail=f"Initialization failed: {str(e)}")

@app.post("/api/dask/optimize")
async def optimize_dask_settings(data: Dict[str, Any] = Body(...)):
    """Get Dask optimization recommendations"""
    try:
        file_size_mb = data.get("file_size_mb", 0)
        system_resources = get_system_resources()
        
        memory_gb = system_resources["total_memory_gb"]
        cpu_count = system_resources["cpu_count"]
        
        # Calculate optimal settings based on file size and system resources
        if file_size_mb > 1000:  # Very large files
            recommended_workers = min(cpu_count, 8)
            memory_per_worker = max(2, int(memory_gb * 0.4 / recommended_workers))
            blocksize = "100MB"
            use_dask = True
            estimated_time = f"{file_size_mb / 200:.1f} minutes"
        elif file_size_mb > 200:  # Large files
            recommended_workers = min(cpu_count, 6)
            memory_per_worker = max(1, int(memory_gb * 0.5 / recommended_workers))
            blocksize = "50MB"
            use_dask = True
            estimated_time = f"{file_size_mb / 300:.1f} minutes"
        elif file_size_mb > 50:  # Medium files
            recommended_workers = min(cpu_count, 4)
            memory_per_worker = max(1, int(memory_gb * 0.6 / recommended_workers))
            blocksize = "25MB"
            use_dask = DASK_AVAILABLE
            estimated_time = "Under 2 minutes"
        else:  # Small files
            recommended_workers = 2
            memory_per_worker = 1
            blocksize = "10MB"
            use_dask = False
            estimated_time = "Under 1 minute"
        
        return _to_jsonable({
            "file_size_mb": file_size_mb,
            "recommended_workers": recommended_workers,
            "memory_per_worker_gb": memory_per_worker,
            "blocksize": blocksize,
            "use_dask": use_dask,
            "estimated_processing_time": estimated_time,
            "system_resources": system_resources,
            "recommendations": generate_performance_recommendations(file_size_mb, system_resources)
        })
        
    except Exception as e:
        logger.error(f"Optimization error: {e}")
        raise HTTPException(status_code=500, detail=f"Optimization failed: {str(e)}")

def generate_performance_recommendations(file_size_mb: float, system_resources: dict) -> List[str]:
    """Generate performance optimization recommendations"""
    recommendations = []
    
    memory_gb = system_resources["total_memory_gb"]
    available_memory = system_resources["available_memory_gb"]
    
    if file_size_mb > memory_gb * 1024 * 0.5:  # File larger than 50% of total RAM
        recommendations.append("File is large relative to available memory - Dask highly recommended")
    
    if available_memory < 2:
        recommendations.append("Low available memory - consider closing other applications")
    
    if system_resources["cpu_count"] < 4:
        recommendations.append("Limited CPU cores - processing may be slower")
    
    if file_size_mb > 500 and not DASK_AVAILABLE:
        recommendations.append("Large file detected - install Dask for better performance: pip install dask[complete]")
    
    if not recommendations:
        recommendations.append("System resources are adequate for processing")
    
    return recommendations

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting DataPro Agent API Server")
    logger.info(f"Dask Available: {DASK_AVAILABLE}")
    logger.info(f"Gemini AI Available: {GEMINI_AVAILABLE}")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,  # Changed from 8000 to 8001
        log_level="info",
        access_log=True
    );