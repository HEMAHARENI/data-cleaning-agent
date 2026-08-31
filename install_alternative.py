#!/usr/bin/env python3
"""
Alternative installation script for DataPro Agent
Handles Windows compatibility issues with pandas and other dependencies
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during {description}:")
        print(f"Command: {command}")
        print(f"Error: {e.stderr}")
        return False

def install_package(package, description=None):
    """Install a single package"""
    if description is None:
        description = f"Installing {package}"
    return run_command(f"pip install {package}", description)

def main():
    print("🚀 DataPro Agent - Alternative Installation Script")
    print("=" * 50)
    
    # Check if we're on Windows
    is_windows = os.name == 'nt'
    print(f"Platform: {'Windows' if is_windows else 'Unix/Linux'}")
    
    # Step 1: Upgrade pip
    if not run_command("python -m pip install --upgrade pip", "Upgrading pip"):
        print("⚠️ Warning: Could not upgrade pip, continuing...")
    
    # Step 2: Install build tools
    if is_windows:
        if not install_package("wheel setuptools", "Installing build tools"):
            print("⚠️ Warning: Could not install build tools")
    
    # Step 3: Install numpy first (required for pandas)
    if not install_package("numpy", "Installing numpy (required for pandas)"):
        print("❌ Failed to install numpy. Trying alternative method...")
        if not install_package("numpy --only-binary=all", "Installing numpy (binary only)"):
            print("❌ Critical error: Cannot install numpy. Please check your Python installation.")
            return False
    
    # Step 4: Install pandas with Windows-specific options
    if is_windows:
        pandas_commands = [
            "pip install pandas",
            "pip install pandas --only-binary=all",
            "pip install pandas --no-build-isolation",
            "pip install pandas --prefer-binary"
        ]
        
        pandas_installed = False
        for cmd in pandas_commands:
            if run_command(cmd, f"Installing pandas (trying: {cmd})"):
                pandas_installed = True
                break
        
        if not pandas_installed:
            print("❌ Failed to install pandas with all methods.")
            print("💡 Try installing Microsoft Visual C++ Build Tools:")
            print("   https://visualstudio.microsoft.com/visual-cpp-build-tools/")
            return False
    else:
        if not install_package("pandas", "Installing pandas"):
            return False
    
    # Step 5: Install remaining packages
    packages = [
        ("scipy", "Installing scipy"),
        ("scikit-learn", "Installing scikit-learn"),
        ("fastapi", "Installing FastAPI"),
        ("uvicorn", "Installing Uvicorn"),
        ("tqdm", "Installing tqdm"),
        ("openpyxl", "Installing openpyxl"),
        ("psutil", "Installing psutil"),
        ("python-multipart", "Installing python-multipart")
    ]
    
    for package, description in packages:
        if not install_package(package, description):
            print(f"⚠️ Warning: Could not install {package}")
    
    # Step 6: Install Dask (optional, can be large)
    print("\n🔄 Installing Dask (this may take a while)...")
    dask_commands = [
        "pip install dask[complete]",
        "pip install dask[complete] --no-deps",
        "pip install dask distributed"
    ]
    
    dask_installed = False
    for cmd in dask_commands:
        if run_command(cmd, f"Installing Dask (trying: {cmd})"):
            dask_installed = True
            break
    
    if not dask_installed:
        print("⚠️ Warning: Could not install Dask. Large data processing will not be available.")
        print("💡 You can try installing Dask manually later: pip install dask[complete]")
    
    # Step 7: Install optional packages
    optional_packages = [
        ("pyarrow", "Installing PyArrow (for Parquet support)"),
        ("google-generativeai", "Installing Google Generative AI (for AI features)")
    ]
    
    for package, description in optional_packages:
        if not install_package(package, description):
            print(f"ℹ️ Info: {package} is optional, continuing without it")
    
    print("\n" + "=" * 50)
    print("🎉 Installation completed!")
    print("\nTo start the DataPro Agent:")
    print("1. Run: python backend.py")
    print("2. Open: data-cleaning-agent-frontend/frontend.html")
    print("\nIf you encounter any issues:")
    print("- Make sure you have Python 3.8+ installed")
    print("- On Windows, ensure you have Visual C++ Build Tools")
    print("- Try running: pip install --upgrade pip setuptools wheel")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
