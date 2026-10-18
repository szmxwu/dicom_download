# DICOM Download - Offline Package Installer
# For Python 3.8 + Windows x64

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host " DICOM Download - Offline Package Installer" -ForegroundColor Cyan
Write-Host " For Python 3.8 + Windows x64" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Check Python version
$pythonVersion = python --version 2>&1
if ($pythonVersion -notmatch "3\.8") {
    Write-Host "[ERROR] Python 3.8 is required! Found: $pythonVersion" -ForegroundColor Red
    Write-Host "Please use: conda activate your_env_name"
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "[1/3] Uninstalling existing numpy..."
pip uninstall numpy -y 2>$null

Write-Host "[2/3] Installing packages from wheels..."
$wheels = Get-ChildItem -Filter "*.whl"
$total = $wheels.Count
$count = 0

foreach ($wheel in $wheels) {
    $count++
    Write-Host "[$count/$total] Installing $($wheel.Name)..."
    pip install $wheel.FullName --no-index --find-links .
}

Write-Host "[3/3] Verifying installation..."
python -c "import numpy; print(f'NumPy {numpy.__version__} installed successfully')"
python -c "import pandas; print(f'Pandas {pandas.__version__} installed successfully')"
python -c "import pydicom; print(f'pydicom {pydicom.__version__} installed successfully')"
python -c "import pynetdicom; print(f'pynetdicom {pynetdicom.__version__} installed successfully')"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host " Installation Complete!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Read-Host "Press Enter to exit"
