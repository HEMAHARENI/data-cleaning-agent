@echo off
echo Installing DataPro Agent Dependencies for Windows...
echo.

echo Step 1: Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Step 2: Installing core dependencies...
pip install wheel setuptools

echo.
echo Step 3: Installing numpy first (required for pandas)...
pip install numpy

echo.
echo Step 4: Installing pandas...
pip install pandas

echo.
echo Step 5: Installing scipy...
pip install scipy

echo.
echo Step 6: Installing scikit-learn...
pip install scikit-learn

echo.
echo Step 7: Installing Dask...
pip install "dask[complete]"

echo.
echo Step 8: Installing remaining dependencies...
pip install fastapi uvicorn tqdm google-generativeai openpyxl pyarrow psutil python-multipart

echo.
echo Installation complete!
echo.
echo To start the server, run:
echo python backend.py
echo.
pause
