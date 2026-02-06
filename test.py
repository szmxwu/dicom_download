# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""Upload workflow test runner.

Iterates all ZIP files in uploads/ and runs both NIfTI and NPZ workflows.
Result ZIPs are written to download/ for manual verification.
"""

import os
import time
from typing import List, Tuple

from dicom_client_unified import DICOMDownloadClient
from result_packaging import create_result_zip


def list_upload_zips(uploads_dir: str) -> List[str]:
    if not os.path.isdir(uploads_dir):
        return []
    return sorted(
        os.path.join(uploads_dir, name)
        for name in os.listdir(uploads_dir)
        if name.lower().endswith('.zip')
    )


def run_upload_test(zip_path: str, output_format: str, results_root: str, download_dir: str) -> Tuple[bool, str]:
    client = DICOMDownloadClient()

    zip_name = os.path.splitext(os.path.basename(zip_path))[0]
    base_output_dir = os.path.join(results_root, zip_name, output_format)
    os.makedirs(base_output_dir, exist_ok=True)

    options = {
        'auto_organize': True,
        'auto_metadata': True,
        'output_format': output_format
    }

    result = client.process_upload_workflow(zip_path, base_output_dir, options)
    if not result.get('success'):
        return False, result.get('error') or 'Unknown error'

    task_id = f"{zip_name}_{output_format}_{int(time.time())}"
    zip_result = create_result_zip(base_output_dir, task_id, download_dir)
    return True, zip_result


def main():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    uploads_dir = os.path.join(base_dir, 'uploads')
    run_id = str(int(time.time()))
    results_root = os.path.join(base_dir, 'results', 'upload_tests', run_id)
    download_dir = os.path.join(base_dir, 'download')

    os.makedirs(results_root, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)

    zip_files = list_upload_zips(uploads_dir)
    if not zip_files:
        print(f"No zip files found in: {uploads_dir}")
        return 1

    formats = ['nifti', 'npz']
    failures = []

    for zip_path in zip_files:
        print(f"\n== Testing ZIP: {os.path.basename(zip_path)} ==")
        for fmt in formats:
            print(f"-- Format: {fmt}")
            ok, info = run_upload_test(zip_path, fmt, results_root, download_dir)
            if ok:
                print(f"   OK -> {info}")
            else:
                print(f"   FAIL -> {info}")
                failures.append((zip_path, fmt, info))

    if failures:
        print("\nFailures:")
        for zip_path, fmt, info in failures:
            print(f"- {os.path.basename(zip_path)} [{fmt}]: {info}")
        return 2

    print("\nAll upload tests completed successfully.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
