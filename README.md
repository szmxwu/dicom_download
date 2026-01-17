# DICOM Download & Processing Client

[中文说明请见 README_CN.md](README_CN.md)

A unified client for downloading DICOM files from PACS servers and processing them. This tool provides a web interface for managing downloads, extracting metadata, and converting images.

## Features

- **Direct PACS Integration**: Communicate directly with PACS servers using DICOM protocols (C-FIND, C-MOVE).
- **Metadata Extraction**: Extract DICOM tags to Excel files. Supports customizable templates for different modalities (MR, CT, DX, MG).
- **MR Metadata Normalization (MR_clean)**: When MR series are present, the exported Excel will include an additional sheet `MR_Cleaned` with standardized features (e.g. `sequenceClass`, `standardOrientation`, `isFatSuppressed`, `dynamicGroup`, `dynamicPhase`).
- **Image Conversion**: Convert DICOM series to NIfTI format.
- **Web Interface**: User-friendly web UI for searching patients and managing tasks.
- **Modality Support**: Specialized metadata extraction for MRI, CT, Digital Radiography (DX/DR), and Mammography (MG).

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### PACS Connection
Create a `.env` file in the root directory with your PACS server details:

```ini
# DICOM Server Configuration
PACS_IP=127.0.0.1
PACS_PORT=1024
CALLING_AET=cllient
CALLED_AET=pacsServer
CALLING_PORT=1023
```

### Metadata Templates
You can customize the DICOM tags extracted for each modality by editing the JSON files in the `dicom_tags/` directory:
- `mr.json`: MRI
- `ct.json`: CT
- `dx.json`: Digital Radiography (DR/DX/CR)
- `mg.json`: Mammography

Note for MRI normalization:
- `mr.json` must include `ImageType` (used by `MR_clean.py` for refined image type and subtype detection).

### MR_clean Rules Configuration
MRI normalization rules (keywords/thresholds/regex) are externalized in `mr_clean_config.json`.

- Default behavior: `MR_clean.process_mri_dataframe(df)` loads `mr_clean_config.json` automatically.
- Advanced usage: pass a config dict via `cfg=...` or override path via `config_path=...`.
- Typical edits:
   - `thresholds.field_strength.*`: TR/TE/TI thresholds by field strength
   - `classification.ruleA`: name-based special sequence recognition
   - `classification.sequence_family`: GRE/SE/TSE family heuristics
   - `dynamic.*`: dynamic grouping / contrast heuristics

## Usage

1. Start the web application:
   ```bash
   python app.py
   ```
2. Open your browser and navigate to `http://localhost:5005`.
3. Use the interface to search for patients and start download/processing tasks.

### Output
- The metadata Excel contains at least `DICOM_Metadata` and `Series_Summary` sheets.
- If MR records are present, an additional `MR_Cleaned` sheet is generated.

## Project Structure

- `app.py`: Main Flask web application.
- `dicom_client_unified.py`: Core DICOM handling logic.
- `MR_clean.py`: MR metadata normalization and sequence classification.
- `dicom_tags/`: Configuration files for metadata extraction.
- `templates/`: HTML templates for the web UI.
- `static/`: Static assets (CSS, JS).
- `uploads/`: Directory for uploaded files.
- `results/`: Directory for processed results.

## Output formats (NIfTI / NPZ)

This project now supports exporting image data as either NIfTI (`.nii` / `.nii.gz`) or a normalized NPZ (`.npz`) file.

- Front-end: there is an "Output Format" option in the Process Options panel where you can choose `NIfTI` (default) or `NPZ`.
- Programmatic / CLI: when calling the processing workflow from Python, pass the `output_format` parameter to `process_complete_workflow`:

```python
from dicom_client_unified import DICOMDownloadClient

client = DICOMDownloadClient()
client.process_complete_workflow(
      accession_number='M25053000056',
      base_output_dir='./dicom_processed',
      output_format='npz'  # 'nifti' or 'npz'
)
```

- NPZ format details:
   - The tool generates a compressed `.npz` file containing a single array named `data` (dtype: float32).
   - The saved array shape is `[Z, Y, X]` where Z=depth (slices), Y=height (rows), X=width (cols). The file is normalized to a consistent patient-oriented coordinate system (based on DICOM ImageOrientationPatient / ImagePositionPatient):
      - Z axis is ordered from head -> foot (Superior → Inferior).
      - In-plane orientation is normalized to clinical axial (supine) view so that an extracted slice `arr[z]` is ready for display with common Python imaging tools.
   - If your downstream pipeline expects a different layout (for example `[Z, X, Y]`), you can transpose the saved array easily after loading:

```python
import numpy as np

arr = np.load('series.npz')['data']  # arr.shape == (Z, Y, X)
arr_zxy = np.transpose(arr, (0, 2, 1))  # now shape == (Z, X, Y)
```

Notes:
   - NPZ generation uses a temporary NIfTI intermediate (dcm2niix or Python libs) to obtain robust orientation information from DICOM tags.
   - The `.npz` files are compressed with `np.savez_compressed` and use float32 to balance precision and size.

Recent improvements (2026-01-17):

- Quality control during conversion:
   - The tool now runs a lightweight QC (`_assess_image_quality`) on images during NPZ conversion. It detects low dynamic range, low contrast, grayscale inversion (MONOCHROME1-like), and exposure anomalies (under/over-exposed) and marks problematic images.
   - For series (CT/MR) a sequence-level QC (`_assess_series_quality`) aggregates per-slice results. Behavior:
      - If slice_count <= 200: full per-slice QC.
      - If slice_count > 200: sampling QC using middle ±3 slices (7 samples).
   - NPZ conversion returns QC metadata in its result dict: `low_quality`, `low_quality_ratio`, `qc_mode`, and `qc_sample_indices`.

- Photometric & rescale handling:
   - DICOM `RescaleSlope/RescaleIntercept` are applied before saving.
   - `PhotometricInterpretation` MONOCHROME1 images are auto-inverted to match display expectation (consistent with MONOCHROME2).

These changes improve robustness for downstream pipelines and provide early detection of problematic series.
