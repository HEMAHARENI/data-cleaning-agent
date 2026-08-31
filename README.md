# DataPro Agent - Enhanced with Dask Integration

A powerful data cleaning and preprocessing tool that automatically scales to handle large datasets using Dask distributed computing.

## 🚀 Features

### Core Functionality
- **Intelligent Data Processing**: Automatically chooses between Pandas and Dask based on file size
- **Large Data Support**: Handle datasets that exceed available memory using Dask
- **Real-time Analysis**: Comprehensive data quality analysis with missing values, outliers, and duplicates detection
- **Interactive Cleaning**: User-friendly interface for data cleaning operations
- **Multiple Export Formats**: CSV, JSON, Excel, and Parquet support

### Dask Integration
- **Automatic Scaling**: Files > 100MB automatically use Dask for processing
- **Cluster Management**: Real-time monitoring of Dask cluster status and performance
- **Memory Optimization**: Intelligent memory management and worker allocation
- **Dashboard Integration**: Direct access to Dask dashboard for advanced monitoring
- **Performance Recommendations**: AI-powered suggestions for optimal cluster configuration

## 📋 Prerequisites

- Python 3.8 or higher
- At least 4GB RAM (8GB+ recommended for large datasets)
- Modern web browser

## 🛠️ Installation

1. **Clone or download the project files**

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the backend server:**
   ```bash
   python backend.py
   ```

4. **Open the frontend:**
   - Open `data-cleaning-agent-frontend/frontend.html` in your web browser
   - Or serve it using a local web server

## 🎯 Usage

### Basic Workflow

1. **Upload Data**: Drag and drop or select your data file (CSV, Excel, JSON, Parquet)
2. **Automatic Analysis**: The system automatically analyzes your data and chooses the best processing method
3. **Review Results**: Check the analysis results and data quality metrics
4. **Configure Cleaning**: Select cleaning options (missing values, duplicates, outliers, etc.)
5. **Process Data**: Apply cleaning operations using either Pandas or Dask
6. **Download Results**: Export cleaned data in your preferred format

### Large Data Processing

For files larger than 100MB:
- The system automatically uses Dask for distributed processing
- Monitor cluster performance in the "Dask Cluster" tab
- View real-time processing status and memory usage
- Access the Dask dashboard for detailed monitoring

### Dask Cluster Management

- **Cluster Status**: Monitor active workers and system resources
- **Performance Metrics**: View memory usage, CPU utilization, and worker performance
- **Optimization**: Get AI-powered recommendations for optimal cluster settings
- **Dashboard**: Access the Dask dashboard for advanced monitoring

## 🔧 Configuration

### Backend Configuration

The backend automatically configures Dask based on your system:
- **Workers**: Automatically set to optimal number based on CPU cores
- **Memory**: Allocates 60% of available RAM to workers
- **Block Size**: Optimized based on file size and available memory

### Manual Configuration

You can modify the Dask configuration in `backend.py`:
```python
# Adjust worker count
n_workers = min(cpu_count, 8)  # Cap at 8 workers

# Adjust memory allocation
memory_per_worker = max(1, int(memory_gb * 0.6 / n_workers))
```

## 📊 Supported File Formats

### Input Formats
- **CSV**: Comma-separated values
- **Excel**: .xlsx and .xls files
- **JSON**: JSON data files
- **Parquet**: Columnar storage format (recommended for large datasets)

### Output Formats
- **CSV**: Standard CSV format
- **JSON**: JSON records format
- **Excel**: .xlsx format
- **Parquet**: Optimized for large datasets (Dask only)

## 🎨 Features by File Size

### Small Files (< 100MB)
- Uses Pandas for fast processing
- Full interactive analysis
- All cleaning operations available
- Quick processing times

### Large Files (> 100MB)
- Automatically switches to Dask
- Distributed processing across multiple workers
- Memory-efficient operations
- Progress monitoring and status updates
- Parquet export support

### Very Large Files (> 1GB)
- Optimized Dask configuration
- Chunked processing
- Memory usage warnings
- Extended processing times
- Dashboard monitoring recommended

## 🔍 Data Cleaning Operations

### Missing Values
- **Fill Methods**: Mean, median, mode, forward fill, backward fill
- **Custom Values**: Specify custom fill values
- **Drop Rows**: Remove rows with missing values

### Duplicates
- **Detection**: Identify duplicate rows
- **Removal**: Keep first or last occurrence
- **Dask Optimized**: Efficient duplicate removal for large datasets

### Outliers
- **IQR Method**: Interquartile range detection
- **Z-Score**: Statistical outlier detection
- **Isolation Forest**: Machine learning-based detection

### Data Types
- **Auto-conversion**: Intelligent type inference
- **Optimization**: Memory-efficient data types
- **Text Standardization**: Clean and normalize text data

### Preprocessing
- **Smart Encoding**: Automatic categorical encoding
- **Normalization**: StandardScaler, MinMaxScaler, RobustScaler
- **Feature Scaling**: Advanced scaling options

## 📈 Performance Monitoring

### Real-time Metrics
- **Memory Usage**: Current and available memory
- **CPU Utilization**: Worker performance monitoring
- **Processing Speed**: Operations per second
- **Cluster Health**: Worker status and connectivity

### Dask Dashboard
- Access at `http://localhost:8787` when cluster is active
- Real-time task monitoring
- Memory and CPU usage graphs
- Worker performance metrics

## 🚨 Troubleshooting

### Common Issues

1. **Memory Errors**
   - Reduce worker count in backend configuration
   - Use Parquet format for large datasets
   - Process data in smaller chunks

2. **Slow Processing**
   - Check cluster status in Dask tab
   - Verify worker allocation
   - Consider using SSD storage

3. **Upload Failures**
   - Check file format compatibility
   - Verify file size limits
   - Ensure backend server is running

### Performance Tips

1. **For Large Datasets**
   - Use Parquet format when possible
   - Monitor memory usage in Dask dashboard
   - Consider processing during off-peak hours

2. **For Better Performance**
   - Ensure adequate RAM (8GB+ recommended)
   - Use SSD storage for faster I/O
   - Close unnecessary applications

## 🔮 Future Enhancements

- **GPU Support**: CUDA integration for massive datasets
- **Cloud Integration**: AWS/Azure cluster support
- **Advanced Analytics**: Machine learning integration
- **Real-time Streaming**: Live data processing capabilities
- **Collaborative Features**: Multi-user data cleaning sessions

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📞 Support

For support and questions:
- Check the troubleshooting section
- Review the Dask documentation
- Open an issue in the project repository

---

**DataPro Agent** - Making large-scale data cleaning accessible and efficient! 🚀
