# -*- coding: utf-8 -*-
"""Packaging helpers for results."""

import os
import zipfile
from typing import Iterable, Optional


def create_result_zip(source_dir: str, task_id: str, result_dir: str, extra_files: Optional[Iterable[str]] = None) -> str:
    """Create result ZIP file.

    extra_files: additional files to include at ZIP root.
    """
    os.makedirs(result_dir, exist_ok=True)
    zip_path = os.path.join(result_dir, f"result_{task_id}.zip")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arc_name)

        if extra_files:
            for extra_path in extra_files:
                if not extra_path or not os.path.exists(extra_path):
                    continue
                try:
                    base_dir = os.path.abspath(source_dir)
                    extra_abs = os.path.abspath(extra_path)
                    if os.path.commonpath([base_dir, extra_abs]) == base_dir:
                        continue
                except Exception:
                    pass
                arc_name = os.path.basename(extra_path)
                zipf.write(extra_path, arc_name)

    return zip_path
