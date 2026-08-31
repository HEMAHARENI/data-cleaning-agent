import pandas as pd
import numpy as np
import json
import os
import sys
from typing import Dict, List, Optional, Any, Union
import warnings
import google.generativeai as genai
from sklearn.preprocessing import MinMaxScaler, StandardScaler, LabelEncoder
import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, LocalCluster
from tqdm import tqdm
import logging
import psutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

class DaskEnhancedDataCleaner:
    def __init__(self, gemini_api_key: str = None, use_dask: bool = True, n_workers: int = None):
        """
        Initialize Enhanced Data Cleaner with full Dask integration
        
        Args:
            gemini_api_key: Google Gemini API key for AI suggestions
            use_dask: Whether to use Dask for large data processing
            n_workers: Number of Dask workers (default: CPU count)
        """
        self.use_dask = use_dask
        self.ai_enabled = False
        self.client = None
        
        # Initialize Dask cluster for large data processing
        if self.use_dask:
            self._setup_dask_cluster(n_workers)
        
        # Initialize Gemini AI if API key provided
        if gemini_api_key:
            try:
                genai.configure(api_key=gemini_api_key)
                self.model = genai.GenerativeModel('gemini-pro')
                self.ai_enabled = True
                logger.info("✅ Gemini AI initialized successfully")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize Gemini AI: {e}")
                self.ai_enabled = False
        
        self.cleaning_history = []
        self.suggestions_cache = {}

    def _setup_dask_cluster(self, n_workers: int = None):
        """Setup Dask cluster for parallel processing"""
        try:
            # Determine optimal number of workers
            if n_workers is None:
                n_workers = min(psutil.cpu_count(), 8)  # Cap at 8 to avoid overwhelming
            
            # Calculate memory per worker (leave some for system)
            total_memory = psutil.virtual_memory().total / 1024**3  # GB
            memory_per_worker = max(1, int(total_memory * 0.7 / n_workers))  # 70% of total memory
            
            # Create local cluster
            cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=2,
                memory_limit=f'{memory_per_worker}GB',
                silence_logs=False
            )
            
            self.client = Client(cluster)
            logger.info(f"✅ Dask cluster initialized with {n_workers} workers")
            logger.info(f"📊 Dashboard available at: {self.client.dashboard_link}")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Dask cluster: {e}")
            self.use_dask = False

    def load_data(self, file_path: str, force_pandas: bool = False) -> Union[pd.DataFrame, dd.DataFrame]:
        """Load data with intelligent Dask/Pandas selection"""
        try:
            ext = os.path.splitext(file_path)[-1].lower()
            file_size_mb = os.path.getsize(file_path) / 1024**2
            
            # Decide whether to use Dask based on file size and user preference
            use_dask_for_file = self.use_dask and file_size_mb > 100 and not force_pandas
            
            logger.info(f"Loading {file_path} ({file_size_mb:.1f} MB)...")
            
            if use_dask_for_file:
                logger.info("🚀 Using Dask for large dataset processing...")
                
                if ext == '.csv':
                    # Dask CSV reading with optimizations
                    ddf = dd.read_csv(
                        file_path,
                        blocksize=25e6,  # 25MB blocks
                        assume_missing=True,
                        dtype_backend='numpy_nullable'  # Better handling of mixed types
                    )
                    logger.info(f"✅ Dask DataFrame loaded. Partitions: {ddf.npartitions}")
                    return ddf
                    
                elif ext == '.parquet':
                    ddf = dd.read_parquet(file_path)
                    logger.info(f"✅ Dask DataFrame loaded. Partitions: {ddf.npartitions}")
                    return ddf
                    
                else:
                    logger.info("⚠️ File format not supported by Dask, using Pandas...")
                    use_dask_for_file = False
            
            # Pandas loading for smaller files or unsupported formats
            if ext == '.csv':
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='latin-1')
            elif ext in ['.xls', '.xlsx']:
                df = pd.read_excel(file_path)
            elif ext == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
            elif ext == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {ext}")
                
            logger.info(f"✅ Pandas DataFrame loaded. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            raise

    def detect_issues_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Dict[str, Any]:
        """Enhanced issue detection optimized for Dask DataFrames"""
        logger.info("🔍 Analyzing dataset for issues...")
        
        is_dask = isinstance(ddf, dd.DataFrame)
        
        if is_dask:
            # Dask operations
            issues = {
                "shape": (len(ddf), len(ddf.columns)),
                "columns": list(ddf.columns),
                "dtypes": ddf.dtypes.to_dict(),
                "memory_usage_mb": ddf.memory_usage(deep=True).sum().compute() / 1024**2,
                "missing_values": ddf.isnull().sum().compute().to_dict(),
                "duplicates": ddf.duplicated().sum().compute(),
                "partitions": ddf.npartitions,
            }
            
            # Calculate missing percentages
            total_rows = len(ddf)
            issues["missing_percentage"] = {
                col: (count / total_rows * 100) 
                for col, count in issues["missing_values"].items()
            }
            
            # Get unique counts (this can be expensive for large datasets)
            print("📊 Computing unique counts (this may take a moment for large datasets)...")
            unique_counts = {}
            for col in tqdm(ddf.columns, desc="Analyzing columns"):
                try:
                    unique_counts[col] = ddf[col].nunique().compute()
                except Exception as e:
                    logger.warning(f"Could not compute unique count for {col}: {e}")
                    unique_counts[col] = "Unknown"
            
            issues["unique_counts"] = unique_counts
            
            # Detect constant columns
            issues["constant_columns"] = [
                col for col, count in unique_counts.items() 
                if isinstance(count, int) and count <= 1
            ]
            
            # Sample data for AI analysis
            sample_df = ddf.head(1000)
            
        else:
            # Pandas operations (original logic)
            issues = {
                "shape": ddf.shape,
                "memory_usage_mb": ddf.memory_usage(deep=True).sum() / 1024**2,
                "missing_values": ddf.isnull().sum().to_dict(),
                "missing_percentage": (ddf.isnull().sum() / len(ddf) * 100).to_dict(),
                "duplicates": ddf.duplicated().sum(),
                "dtypes": ddf.dtypes.apply(str).to_dict(),
                "outliers": self._detect_outliers_dask(ddf),
                "unique_counts": {col: ddf[col].nunique() for col in ddf.columns},
                "constant_columns": [col for col in ddf.columns if ddf[col].nunique() <= 1],
                "partitions": "N/A (Pandas DataFrame)"
            }
            sample_df = ddf.head(100)
        
        # Get AI suggestions if enabled
        if self.ai_enabled:
            issues["ai_suggestions"] = self._get_ai_suggestions(issues, sample_df)
        
        return issues

    def _detect_outliers_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Dict[str, int]:
        """Detect outliers with Dask support"""
        outliers = {}
        
        if isinstance(ddf, dd.DataFrame):
            numeric_cols = ddf.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                try:
                    # Use Dask for quantile computation
                    q1 = ddf[col].quantile(0.25).compute()
                    q3 = ddf[col].quantile(0.75).compute()
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr
                    
                    # Count outliers
                    outlier_mask = (ddf[col] < lower) | (ddf[col] > upper)
                    outlier_count = outlier_mask.sum().compute()
                    outliers[col] = int(outlier_count)
                    
                except Exception as e:
                    logger.warning(f"Could not compute outliers for {col}: {e}")
                    outliers[col] = 0
        else:
            # Original pandas logic
            numeric_cols = ddf.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if ddf[col].notna().sum() > 0:
                    q1 = ddf[col].quantile(0.25)
                    q3 = ddf[col].quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr
                    outlier_count = ((ddf[col] < lower) | (ddf[col] > upper)).sum()
                    outliers[col] = int(outlier_count)
        
        return outliers

    def _get_ai_suggestions(self, issues: Dict, sample_data: pd.DataFrame) -> str:
        """Get AI-powered cleaning suggestions"""
        try:
            prompt = f"""
            Analyze this dataset and provide intelligent data cleaning suggestions:
            
            Dataset Info:
            - Shape: {issues['shape']}
            - Memory Usage: {issues.get('memory_usage_mb', 0):.2f} MB
            - Missing Values: {dict(list(issues['missing_values'].items())[:5])}
            - Duplicates: {issues['duplicates']}
            - Data Types: {dict(list(issues['dtypes'].items())[:5])}
            - Partitions: {issues.get('partitions', 'N/A')}
            
            Sample Data:
            {sample_data.to_string()}
            
            Provide concise, actionable suggestions for:
            1. Handling missing values
            2. Data type conversions
            3. Feature engineering opportunities
            4. Large data optimization tips
            """
            
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.warning(f"Failed to get AI suggestions: {e}")
            return "AI suggestions unavailable"

    def interactive_missing_handler_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
        """Interactive missing value handling optimized for Dask"""
        is_dask = isinstance(ddf, dd.DataFrame)
        
        if is_dask:
            missing_info = ddf.isnull().sum().compute()
            missing_cols = missing_info[missing_info > 0].index.tolist()
        else:
            missing_cols = ddf.columns[ddf.isnull().any()].tolist()
        
        if not missing_cols:
            logger.info("✅ No missing values found!")
            return ddf
        
        print("\n" + "="*60)
        print("🔧 INTERACTIVE MISSING VALUE HANDLER (Dask Optimized)")
        print("="*60)
        
        # For Dask DataFrames, convert to pandas for interactive processing if reasonable size
        if is_dask and len(ddf) > 1000000:  # > 1M rows
            print("⚠️ Large dataset detected. Processing missing values in chunks...")
            return self._handle_missing_dask_chunks(ddf, missing_cols)
        elif is_dask:
            print("🔄 Converting Dask DataFrame to Pandas for interactive processing...")
            df = ddf.compute()
            return self._interactive_missing_pandas(df, missing_cols)
        else:
            return self._interactive_missing_pandas(ddf, missing_cols)

    def _handle_missing_dask_chunks(self, ddf: dd.DataFrame, missing_cols: List[str]) -> dd.DataFrame:
        """Handle missing values for large Dask DataFrames using chunk processing"""
        print(f"Processing {len(missing_cols)} columns with missing values...")
        
        strategies = {}
        
        # Get user strategy for each column
        for col in missing_cols:
            missing_count = ddf[col].isnull().sum().compute()
            missing_pct = (missing_count / len(ddf)) * 100
            col_dtype = str(ddf[col].dtype)
            
            # Get sample values (first partition)
            sample_values = ddf[col].dropna().head(5).compute().tolist()
            
            print(f"\n📊 Column: {col}")
            print(f"   Missing: {missing_count} ({missing_pct:.2f}%)")
            print(f"   Type: {col_dtype}")
            print(f"   Sample values: {sample_values}")
            
            # Get AI suggestion
            if self.ai_enabled:
                ai_suggestion = self._get_column_ai_suggestion_dask(ddf, col)
                print(f"   🤖 AI Suggestion: {ai_suggestion}")
            
            print("\nChoose handling strategy:")
            print("1. Fill with mean (numeric only)")
            print("2. Fill with median (numeric only)")
            print("3. Fill with mode (most frequent)")
            print("4. Forward fill")
            print("5. Backward fill")
            print("6. Custom value")
            print("7. Drop this column")
            print("0. Skip this column")
            
            while True:
                try:
                    choice = input(f"\nEnter choice for '{col}' (0-7): ").strip()
                    
                    if choice == '0':
                        strategies[col] = 'skip'
                        break
                    elif choice == '1' and 'int' in col_dtype or 'float' in col_dtype:
                        strategies[col] = ('mean', None)
                        break
                    elif choice == '2' and 'int' in col_dtype or 'float' in col_dtype:
                        strategies[col] = ('median', None)
                        break
                    elif choice == '3':
                        strategies[col] = ('mode', None)
                        break
                    elif choice == '4':
                        strategies[col] = ('ffill', None)
                        break
                    elif choice == '5':
                        strategies[col] = ('bfill', None)
                        break
                    elif choice == '6':
                        custom_value = input("Enter custom value: ")
                        try:
                            if 'int' in col_dtype or 'float' in col_dtype:
                                custom_value = float(custom_value)
                        except ValueError:
                            pass
                        strategies[col] = ('custom', custom_value)
                        break
                    elif choice == '7':
                        strategies[col] = 'drop'
                        break
                    else:
                        print("❌ Invalid choice or incompatible with column type. Please try again.")
                        
                except KeyboardInterrupt:
                    print("\n⚠️ Operation cancelled by user")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
        
        # Apply strategies using Dask operations
        return self._apply_missing_strategies_dask(ddf, strategies)

    def _apply_missing_strategies_dask(self, ddf: dd.DataFrame, strategies: Dict) -> dd.DataFrame:
        """Apply missing value strategies using Dask operations"""
        logger.info("🔧 Applying missing value strategies with Dask...")
        
        # Columns to drop
        cols_to_drop = [col for col, strategy in strategies.items() if strategy == 'drop']
        if cols_to_drop:
            ddf = ddf.drop(columns=cols_to_drop)
            print(f"✅ Dropped columns: {cols_to_drop}")
        
        # Apply fill strategies
        for col, strategy in strategies.items():
            if strategy == 'skip' or strategy == 'drop':
                continue
                
            strategy_type, value = strategy
            
            try:
                if strategy_type == 'mean':
                    fill_value = ddf[col].mean().compute()
                    ddf[col] = ddf[col].fillna(fill_value)
                    print(f"✅ Filled '{col}' with mean: {fill_value:.2f}")
                    
                elif strategy_type == 'median':
                    fill_value = ddf[col].quantile(0.5).compute()
                    ddf[col] = ddf[col].fillna(fill_value)
                    print(f"✅ Filled '{col}' with median: {fill_value:.2f}")
                    
                elif strategy_type == 'mode':
                    # Mode computation for Dask (take most frequent from sample)
                    mode_value = ddf[col].value_counts().idxmax().compute()
                    ddf[col] = ddf[col].fillna(mode_value)
                    print(f"✅ Filled '{col}' with mode: {mode_value}")
                    
                elif strategy_type == 'ffill':
                    ddf[col] = ddf[col].fillna(method='ffill')
                    print(f"✅ Forward filled '{col}'")
                    
                elif strategy_type == 'bfill':
                    ddf[col] = ddf[col].fillna(method='bfill')
                    print(f"✅ Backward filled '{col}'")
                    
                elif strategy_type == 'custom':
                    ddf[col] = ddf[col].fillna(value)
                    print(f"✅ Filled '{col}' with custom value: {value}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing column {col}: {e}")
        
        return ddf

    def _interactive_missing_pandas(self, df: pd.DataFrame, missing_cols: List[str]) -> pd.DataFrame:
        """Interactive missing handling for Pandas DataFrames (original logic)"""
        for col in missing_cols:
            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / len(df)) * 100
            col_dtype = df[col].dtype
            
            print(f"\n📊 Column: {col}")
            print(f"   Missing: {missing_count} ({missing_pct:.2f}%)")
            print(f"   Type: {col_dtype}")
            
            sample_values = df[col].dropna().head(3).tolist()
            print(f"   Sample values: {sample_values}")
            
            if self.ai_enabled:
                ai_suggestion = self._get_column_ai_suggestion_pandas(df, col)
                print(f"   🤖 AI Suggestion: {ai_suggestion}")
            
            print("\nChoose handling strategy:")
            print("1. Fill with mean (numeric only)")
            print("2. Fill with median (numeric only)")
            print("3. Fill with mode (most frequent)")
            print("4. Forward fill")
            print("5. Backward fill")
            print("6. Interpolate (numeric only)")
            print("7. Custom value")
            print("8. Drop rows with missing values")
            print("9. Drop this column")
            print("0. Skip this column")
            
            while True:
                try:
                    choice = input(f"\nEnter choice for '{col}' (0-9): ").strip()
                    
                    if choice == '0':
                        break
                    elif choice == '1' and col_dtype in ['int64', 'float64']:
                        df[col].fillna(df[col].mean(), inplace=True)
                        print(f"✅ Filled with mean: {df[col].mean():.2f}")
                        break
                    elif choice == '2' and col_dtype in ['int64', 'float64']:
                        df[col].fillna(df[col].median(), inplace=True)
                        print(f"✅ Filled with median: {df[col].median():.2f}")
                        break
                    elif choice == '3':
                        mode_val = df[col].mode()[0] if not df[col].mode().empty else 'Unknown'
                        df[col].fillna(mode_val, inplace=True)
                        print(f"✅ Filled with mode: {mode_val}")
                        break
                    elif choice == '4':
                        df[col].fillna(method='ffill', inplace=True)
                        print("✅ Forward filled")
                        break
                    elif choice == '5':
                        df[col].fillna(method='bfill', inplace=True)
                        print("✅ Backward filled")
                        break
                    elif choice == '6' and col_dtype in ['int64', 'float64']:
                        df[col].interpolate(inplace=True)
                        print("✅ Interpolated")
                        break
                    elif choice == '7':
                        custom_value = input("Enter custom value: ")
                        try:
                            if col_dtype in ['int64', 'float64']:
                                custom_value = float(custom_value)
                        except ValueError:
                            pass
                        df[col].fillna(custom_value, inplace=True)
                        print(f"✅ Filled with custom value: {custom_value}")
                        break
                    elif choice == '8':
                        initial_rows = len(df)
                        df.dropna(subset=[col], inplace=True)
                        dropped_rows = initial_rows - len(df)
                        print(f"✅ Dropped {dropped_rows} rows")
                        break
                    elif choice == '9':
                        df.drop(columns=[col], inplace=True)
                        print(f"✅ Dropped column '{col}'")
                        break
                    else:
                        print("❌ Invalid choice or incompatible with column type. Please try again.")
                        
                except KeyboardInterrupt:
                    print("\n⚠️ Operation cancelled by user")
                    break
                except Exception as e:
                    print(f"❌ Error: {e}")
        
        return df

    def _get_column_ai_suggestion_dask(self, ddf: dd.DataFrame, column: str) -> str:
        """Get AI suggestion for specific column in Dask DataFrame"""
        try:
            missing_count = ddf[column].isnull().sum().compute()
            unique_count = ddf[column].nunique().compute()
            sample_values = ddf[column].dropna().head(5).compute().tolist()
            
            col_info = {
                'name': column,
                'type': str(ddf[column].dtype),
                'missing_count': missing_count,
                'unique_count': unique_count,
                'sample_values': sample_values
            }
            
            prompt = f"""
            For column '{column}' with the following characteristics:
            - Type: {col_info['type']}
            - Missing: {col_info['missing_count']} values
            - Unique values: {col_info['unique_count']}
            - Sample: {col_info['sample_values']}
            
            Suggest the BEST single strategy for handling missing values. Be concise (max 20 words).
            """
            
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except:
            return "Fill with median/mode based on data type"

    def _get_column_ai_suggestion_pandas(self, df: pd.DataFrame, column: str) -> str:
        """Get AI suggestion for specific column in Pandas DataFrame"""
        try:
            col_info = {
                'name': column,
                'type': str(df[column].dtype),
                'missing_count': df[column].isnull().sum(),
                'unique_count': df[column].nunique(),
                'sample_values': df[column].dropna().head(5).tolist()
            }
            
            prompt = f"""
            For column '{column}' with the following characteristics:
            - Type: {col_info['type']}
            - Missing: {col_info['missing_count']} values
            - Unique values: {col_info['unique_count']}
            - Sample: {col_info['sample_values']}
            
            Suggest the BEST single strategy for handling missing values. Be concise (max 20 words).
            """
            
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except:
            return "Fill with median/mode based on data type"

    def remove_duplicates_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
        """Remove duplicates with Dask optimization"""
        if isinstance(ddf, dd.DataFrame):
            logger.info("🔄 Removing duplicates with Dask (this may take time for large datasets)...")
            return ddf.drop_duplicates()
        else:
            return ddf.drop_duplicates()

    def smart_encoding_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
        """Smart encoding with Dask support"""
        is_dask = isinstance(ddf, dd.DataFrame)
        
        if is_dask:
            categorical_cols = ddf.select_dtypes(include=['object']).columns.tolist()
        else:
            categorical_cols = ddf.select_dtypes(include=['object', 'category']).columns.tolist()
        
        if len(categorical_cols) == 0:
            return ddf
        
        print("\n🔄 SMART CATEGORICAL ENCODING (Dask Optimized)")
        print("="*50)
        
        for col in categorical_cols:
            if is_dask:
                unique_count = ddf[col].nunique().compute()
            else:
                unique_count = ddf[col].nunique()
                
            print(f"\nColumn: {col}")
            print(f"Unique values: {unique_count}")
            
            if unique_count <= 10:
                print("✅ Using One-Hot Encoding (low cardinality)")
                if is_dask:
                    ddf = dd.get_dummies(ddf, columns=[col], prefix=col)
                else:
                    ddf = pd.get_dummies(ddf, columns=[col], prefix=col)
            else:
                print("✅ Using Label Encoding (high cardinality)")
                if is_dask:
                    # For Dask, we need to compute unique values first
                    unique_vals = ddf[col].unique().compute()
                    mapping = {val: idx for idx, val in enumerate(unique_vals)}
                    ddf[col] = ddf[col].map(mapping)
                else:
                    le = LabelEncoder()
                    ddf[col] = le.fit_transform(ddf[col].astype(str))
        
        return ddf

    def optimize_dtypes_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
        """Optimize data types with Dask support"""
        logger.info("🔧 Optimizing data types...")
        
        if isinstance(ddf, dd.DataFrame):
            # For Dask, optimize by converting to more efficient types
            optimizations = {}
            
            for col in ddf.columns:
                dtype = str(ddf[col].dtype)
                
                if 'int64' in dtype:
                    # Check if we can downcast to smaller int types
                    col_min = ddf[col].min().compute()
                    col_max = ddf[col].max().compute()
                    
                    if col_min >= 0:
                        if col_max < 255:
                            optimizations[col] = 'uint8'
                        elif col_max < 65535:
                            optimizations[col] = 'uint16'
                        elif col_max < 4294967295:
                            optimizations[col] = 'uint32'
                    else:
                        if col_min >= -128 and col_max <= 127:
                            optimizations[col] = 'int8'
                        elif col_min >= -32768 and col_max <= 32767:
                            optimizations[col] = 'int16'
                        elif col_min >= -2147483648 and col_max <= 2147483647:
                            optimizations[col] = 'int32'
            
            # Apply optimizations
            for col, new_dtype in optimizations.items():
                try:
                    ddf[col] = ddf[col].astype(new_dtype)
                    print(f"✅ Optimized '{col}': {dtype} → {new_dtype}")
                except Exception as e:
                    logger.warning(f"Could not optimize {col}: {e}")
        else:
            # Use pandas convert_dtypes for smaller datasets
            ddf = ddf.convert_dtypes()
        
        return ddf

    def generate_comprehensive_report(self, original_issues: Dict, cleaned_ddf: Union[pd.DataFrame, dd.DataFrame]):
        """Generate detailed cleaning report with Dask support"""
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE DATA CLEANING REPORT")
        print("="*80)
        
        is_dask = isinstance(cleaned_ddf, dd.DataFrame)
        
        if is_dask:
            final_shape = (len(cleaned_ddf), len(cleaned_ddf.columns))
            final_memory = cleaned_ddf.memory_usage(deep=True).sum().compute() / 1024**2
            final_missing = cleaned_ddf.isnull().sum().sum().compute()
        else:
            final_shape = cleaned_ddf.shape
            final_memory = cleaned_ddf.memory_usage(deep=True).sum() / 1024**2
            final_missing = cleaned_ddf.isnull().sum().sum()
        
        # Original vs cleaned comparison
        print(f"\n📈 DATASET TRANSFORMATION:")
        print(f"   Original shape: {original_issues['shape']}")
        print(f"   Final shape: {final_shape}")
        print(f"   Memory usage: {original_issues['memory_usage_mb']:.2f} MB → {final_memory:.2f} MB")
        
        if is_dask:
            print(f"   Processing: Dask ({original_issues.get('partitions', 'N/A')} partitions)")
        else:
            print(f"   Processing: Pandas")
        
        # Missing values summary
        original_missing = sum(original_issues['missing_values'].values())
        print(f"\n🔍 MISSING VALUES:")
        print(f"   Original: {original_missing}")
        print(f"   Final: {final_missing}")
        print(f"   Reduction: {original_missing - final_missing}")
        
        # Data types summary
        print(f"\n📊 DATA TYPES:")
        if is_dask:
            dtype_counts = pd.Series(cleaned_ddf.dtypes.values).value_counts()
        else:
            dtype_counts = cleaned_ddf.dtypes.value_counts()
            
        for dtype, count in dtype_counts.items():
            print(f"   {dtype}: {count} columns")
        
        # AI insights if available
        if self.ai_enabled and 'ai_suggestions' in original_issues:
            print(f"\n🤖 AI INSIGHTS:")
            print(f"   {original_issues['ai_suggestions'][:200]}...")
        
        print("\n✅ Data cleaning completed successfully!")

    def save_data_dask(self, ddf: Union[pd.DataFrame, dd.DataFrame], output_path: str, compression: str = None):
        """Save cleaned data with Dask optimization"""
        try:
            ext = os.path.splitext(output_path)[-1].lower()
            is_dask = isinstance(ddf, dd.DataFrame)
            
            logger.info(f"💾 Saving cleaned data to {output_path}...")
            
            if is_dask:
                # Dask saving operations
                if ext == '.csv':
                    # Save as multiple CSV files (Dask default behavior)
                    base_path = output_path.replace('.csv', '')
                    ddf.to_csv(f"{base_path}_*.csv", index=False, compression=compression)
                    logger.info(f"✅ Dask DataFrame saved as multiple CSV files: {base_path}_*.csv")
                    
                elif ext == '.parquet':
                    ddf.to_parquet(output_path, compression=compression or 'snappy')
                    logger.info(f"✅ Dask DataFrame saved as Parquet")
                    
                else:
                    # Convert to pandas for unsupported formats
                    logger.info("🔄 Converting to Pandas for saving...")
                    df = ddf.compute()
                    self._save_pandas(df, output_path, compression)
            else:
                # Pandas saving
                self._save_pandas(ddf, output_path, compression)
                
        except Exception as e:
            logger.error(f"❌ Error saving file: {e}")
            raise

    def _save_pandas(self, df: pd.DataFrame, output_path: str, compression: str = None):
        """Save Pandas DataFrame"""
        ext = os.path.splitext(output_path)[-1].lower()
        
        if ext == '.csv':
            df.to_csv(output_path, index=False, compression=compression)
        elif ext in ['.xls', '.xlsx']:
            df.to_excel(output_path, index=False)
        elif ext == '.json':
            df.to_json(output_path, orient='records', indent=2)
        elif ext == '.parquet':
            df.to_parquet(output_path, compression=compression or 'snappy')
        else:
            raise ValueError(f"Unsupported output format: {ext}")
        
        file_size = os.path.getsize(output_path) / 1024**2  # MB
        logger.info(f"✅ File saved successfully! Size: {file_size:.2f} MB")

    def close_dask_client(self):
        """Close Dask client and cluster"""
        if self.client:
            self.client.close()
            logger.info("🔄 Dask client closed")

def check_system_resources():
    """Check system resources and recommend Dask settings"""
    cpu_count = psutil.cpu_count()
    memory_gb = psutil.virtual_memory().total / 1024**3
    
    print(f"\n💻 SYSTEM RESOURCES:")
    print(f"   CPU cores: {cpu_count}")
    print(f"   Total RAM: {memory_gb:.1f} GB")
    print(f"   Available RAM: {psutil.virtual_memory().available / 1024**3:.1f} GB")
    
    # Recommendations
    if memory_gb < 4:
        print("⚠️ Low memory detected. Consider using smaller chunk sizes.")
        return False, 2
    elif memory_gb < 8:
        print("📊 Moderate memory. Dask recommended for files > 100MB.")
        return True, min(cpu_count, 4)
    else:
        print("🚀 High memory. Dask recommended for files > 500MB.")
        return True, min(cpu_count, 8)

def main():
    """Main execution function with enhanced Dask integration"""
    print("🚀 ENHANCED DATA CLEANING AGENT WITH FULL DASK INTEGRATION")
    print("="*65)
    
    # Check system resources
    should_use_dask, recommended_workers = check_system_resources()
    
    # Get user preferences
    gemini_key = input("\nEnter Gemini API key (optional, press Enter to skip): ").strip()
    if not gemini_key:
        gemini_key = None
    
    use_dask = input(f"\nUse Dask for large data processing? Recommended: {'Yes' if should_use_dask else 'No'} (y/n): ").strip().lower() == 'y'
    
    if use_dask:
        n_workers = input(f"Number of Dask workers (recommended: {recommended_workers}, press Enter for default): ").strip()
        n_workers = int(n_workers) if n_workers else recommended_workers
    else:
        n_workers = None
    
    # Initialize cleaner
    cleaner = DaskEnhancedDataCleaner(
        gemini_api_key=gemini_key, 
        use_dask=use_dask,
        n_workers=n_workers
    )
    
    try:
        # Load data
        file_path = input("\nEnter dataset path (.csv/.xlsx/.json/.parquet): ").strip()
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Check file size and recommend processing method
        file_size_mb = os.path.getsize(file_path) / 1024**2
        print(f"\n📁 File size: {file_size_mb:.1f} MB")
        
        if file_size_mb > 1000:
            print("🚨 Very large file detected. Strongly recommend using Dask.")
        elif file_size_mb > 100:
            print("📊 Large file detected. Dask recommended for better performance.")
        
        # Load data
        ddf = cleaner.load_data(file_path)
        
        # Analyze issues
        original_issues = cleaner.detect_issues_dask(ddf)
        cleaner.generate_comprehensive_report(original_issues, ddf)
        
        # Interactive cleaning
        print(f"\n🛠️ Starting interactive data cleaning...")
        
        # Handle missing values interactively
        ddf = cleaner.interactive_missing_handler_dask(ddf)
        
        # Remove duplicates
        if original_issues['duplicates'] > 0:
            confirm = input(f"\nFound {original_issues['duplicates']} duplicates. Remove them? (y/n): ")
            if confirm.lower() == 'y':
                ddf = cleaner.remove_duplicates_dask(ddf)
                print("✅ Duplicates removed")
        
        # Smart encoding
        ddf = cleaner.smart_encoding_dask(ddf)
        
        # Data type optimization
        ddf = cleaner.optimize_dtypes_dask(ddf)
        
        # Generate final report
        cleaner.generate_comprehensive_report(original_issues, ddf)
        
        # Save cleaned data
        output_path = input("\nEnter output path (.csv/.xlsx/.json/.parquet): ").strip()
        
        # Recommend output format based on data size
        if isinstance(ddf, dd.DataFrame):
            print("\n💡 For large Dask DataFrames:")
            print("   - .parquet: Best for large data (recommended)")
            print("   - .csv: Will create multiple files (filename_*.csv)")
            print("   - .xlsx/.json: Will convert to Pandas first (memory intensive)")
        
        compression = input("Enter compression (gzip/bz2/xz for CSV, snappy/gzip for parquet, or press Enter): ").strip()
        if not compression:
            compression = None
            
        cleaner.save_data_dask(ddf, output_path, compression)
        
        # Performance summary
        if isinstance(ddf, dd.DataFrame):
            print(f"\n⚡ PERFORMANCE SUMMARY:")
            print(f"   Processing method: Dask ({ddf.npartitions} partitions)")
            print(f"   Workers used: {len(cleaner.client.scheduler_info()['workers']) if cleaner.client else 'N/A'}")
            print(f"   Dashboard: {cleaner.client.dashboard_link if cleaner.client else 'N/A'}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error in main process: {e}")
        raise
    finally:
        # Clean up Dask resources
        cleaner.close_dask_client()

if __name__ == "__main__":
    # Check and install required packages
    required_packages = [
        "pandas", "numpy", "scikit-learn", "dask[complete]", 
        "tqdm", "google-generativeai", "openpyxl", "pyarrow", "psutil"
    ]
    
    print("📦 REQUIRED PACKAGES:")
    print("Run this command to install all dependencies:")
    print(f"pip install {' '.join(required_packages)}")
    print("\n" + "="*65)
    
    # Check if packages are installed
    missing_packages = []
    for package in ['pandas', 'dask', 'tqdm', 'psutil']:
        try:
            __import__(package)
            
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing packages: {missing_packages}")
        print("Please install them before running the script!")
        sys.exit(1)
    
    main()