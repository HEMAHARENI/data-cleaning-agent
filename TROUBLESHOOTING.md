# Troubleshooting Guide - DataPro Agent

## 🚨 Installation Issues

### Windows Pandas Installation Error

If you're getting the pandas installation error you encountered, try these solutions in order:

#### Solution 1: Use the Alternative Installation Script
```bash
python install_alternative.py
```

#### Solution 2: Install with Pre-compiled Binaries
```bash
pip install --only-binary=all pandas numpy scipy scikit-learn
```

#### Solution 3: Install Microsoft Visual C++ Build Tools
1. Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/
2. Install "C++ build tools" workload
3. Restart your command prompt
4. Try installing again: `pip install -r requirements.txt`

#### Solution 4: Use Conda Instead of Pip
```bash
# Install Anaconda or Miniconda first
conda install pandas numpy scipy scikit-learn
pip install fastapi uvicorn dask tqdm openpyxl psutil python-multipart
```

#### Solution 5: Use Minimal Requirements
```bash
pip install -r requirements_minimal.txt
```

### Common Error Messages and Solutions

#### "metadata-generation-failed"
- **Cause**: Missing build tools or incompatible versions
- **Solution**: Use Solution 1 or 3 above

#### "Microsoft Visual C++ 14.0 is required"
- **Cause**: Missing Visual C++ Build Tools
- **Solution**: Install Visual C++ Build Tools (Solution 3)

#### "No module named 'pandas'"
- **Cause**: Pandas not installed or wrong Python environment
- **Solution**: 
  ```bash
  pip install pandas
  # or
  python -m pip install pandas
  ```

#### "Dask installation failed"
- **Cause**: Dask has many dependencies
- **Solution**: Install without Dask first, then add it later:
  ```bash
  pip install -r requirements_minimal.txt
  pip install dask[complete]
  ```

## 🔧 System Requirements

### Minimum Requirements
- Python 3.8 or higher
- 4GB RAM
- 2GB free disk space

### Recommended for Large Data
- Python 3.9+
- 8GB+ RAM
- SSD storage
- 4+ CPU cores

### Windows-Specific Requirements
- Microsoft Visual C++ Build Tools
- Windows 10 or later
- PowerShell or Command Prompt

## 🐛 Runtime Issues

### Backend Won't Start

#### Port Already in Use
```bash
# Kill process using port 8000
netstat -ano | findstr :8000
taskkill /PID <PID_NUMBER> /F

# Or use a different port
python backend.py --port 8001
```

#### Module Import Errors
```bash
# Check if all packages are installed
python -c "import pandas, numpy, fastapi, uvicorn; print('All packages installed')"

# If not, install missing packages
pip install <missing_package>
```

### Frontend Issues

#### CORS Errors
- Make sure backend is running on http://localhost:8000
- Check browser console for specific error messages
- Try opening frontend in incognito/private mode

#### File Upload Fails
- Check file size (should be under 100MB for basic processing)
- Verify file format (CSV, Excel, JSON supported)
- Check browser console for error messages

### Dask-Specific Issues

#### Cluster Won't Start
```python
# Check if Dask is installed
python -c "import dask; print('Dask installed')"

# If not installed
pip install dask[complete]
```

#### Memory Errors
- Reduce worker count in backend.py
- Use smaller chunk sizes
- Process data in batches

#### Dashboard Not Accessible
- Check if port 8787 is available
- Try accessing http://localhost:8787
- Restart the backend server

## 🔍 Debugging Steps

### 1. Check Python Version
```bash
python --version
# Should be 3.8 or higher
```

### 2. Check Installed Packages
```bash
pip list | findstr pandas
pip list | findstr dask
pip list | findstr fastapi
```

### 3. Test Individual Components
```python
# Test pandas
import pandas as pd
df = pd.DataFrame({'test': [1, 2, 3]})
print("Pandas working")

# Test Dask
import dask.dataframe as dd
print("Dask working")

# Test FastAPI
import fastapi
print("FastAPI working")
```

### 4. Check System Resources
```python
import psutil
print(f"CPU cores: {psutil.cpu_count()}")
print(f"Memory: {psutil.virtual_memory().total / 1024**3:.1f} GB")
```

## 📞 Getting Help

### Before Asking for Help
1. Check this troubleshooting guide
2. Try the alternative installation methods
3. Check the error messages carefully
4. Verify your system meets requirements

### When Reporting Issues
Include:
- Operating system and version
- Python version
- Error message (full traceback)
- Steps you tried
- System specifications (RAM, CPU)

### Quick Fixes
- **Restart everything**: Close all terminals, restart computer
- **Clean install**: Uninstall and reinstall Python packages
- **Use virtual environment**: Create isolated Python environment
- **Try different Python version**: Use Python 3.9 or 3.10

## 🚀 Performance Optimization

### For Large Datasets
- Use SSD storage
- Increase available RAM
- Close unnecessary applications
- Use Parquet format for data storage

### For Better Performance
- Install packages with `--no-cache-dir` flag
- Use `pip install --upgrade pip` regularly
- Consider using conda for scientific packages

---

**Remember**: Most installation issues on Windows are related to missing build tools. Installing Microsoft Visual C++ Build Tools usually solves the problem!
