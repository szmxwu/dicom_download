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
PACS_IP=172.17.250.192
PACS_PORT=2104
CALLING_AET=WMX01
CALLED_AET=pacsFIR
CALLING_PORT=1103
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
2. Open your browser and navigate to `http://localhost:5000`.
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
