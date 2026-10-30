"""Semantic counterexamples use synthetic headers, never PACS or patient data."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence
from pydicom.uid import ExplicitVRLittleEndian, EnhancedMRImageStorage, generate_uid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.core.mr_clean import (process_mri_dataframe, load_mr_clean_config, detect_fat_suppression,
                               get_subtype_suffix, build_v2_dataframe, generate_standardized_name)
from src.core.mr_semantics import (parse_multivalue, contrast_attributes, fat_attributes,
                                  build_semantics, acquisition_time)
from src.core.mr_index import (build_mr_frame_index, representative_metadata, attach_mr_index)


@pytest.fixture
def cfg():
    return load_mr_clean_config()


def row(**values):
    result = dict(ProtocolName='t1_vibe', SeriesDescription='t1_vibe', ImageType=['ORIGINAL', 'PRIMARY'],
                  ContrastBolusAgent='', MRAcquisitionType='3D', ScanningSequence='GR',
                  RepetitionTime=5, EchoTime=2, FlipAngle=12, SliceThickness=1,
                  MagneticFieldStrength=3, StudyInstanceUID='1.2.3', SeriesInstanceUID='1.2.3.4',
                  ImageOrientationPatient=[1, 0, 0, 0, 1, 0], ImagePositionPatient=[0, 0, 0],
                  FrameOfReferenceUID='1.2.5', AcquisitionDate='20261030', AcquisitionTime='120000')
    result.update(values)
    return result


@pytest.mark.parametrize('encoding', [lambda x: x, lambda x: MultiValue(str, x), json.dumps, str,
                                            lambda x: '\\'.join(x)])
def test_multivalue_encodings_equivalent(encoding, cfg):
    tokens = ['ORIGINAL', 'PRIMARY', 'WATER']
    r = row(ProtocolName='t1_dixon', SeriesDescription='t1_dixon', ImageType=encoding(tokens))
    assert parse_multivalue(r['ImageType']) == tokens
    assert detect_fat_suppression(r, cfg)
    assert get_subtype_suffix(r, cfg) == '_WATER'
    assert build_semantics(r, cfg)['orientation']['status'] == 'known'


@pytest.mark.parametrize('token,expected,fs', [('WATER', 'WATER_ONLY', True), ('FAT', 'FAT_ONLY', False),
                                            ('INPHASE', 'IN_PHASE', False), ('OUTPHASE', 'OPPOSED_PHASE', False)])
def test_dixon_four_outputs(cfg, token, expected, fs):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='t1_dixon', SeriesDescription='t1_dixon',
                                                ImageType=['ORIGINAL', 'PRIMARY', token])]), cfg).iloc[0]
    attrs = json.loads(r.mr_semantics_v2)['attributes']
    assert attrs['water_fat_component']['value'] == expected
    assert bool(r.isFatSuppressed) is fs
    assert attrs['fat_suppression_method']['value'] == 'UNKNOWN'
    assert attrs['fat_suppression_effect_observed']['status'] == 'unknown'
    assert ('_FAT' in r.sequenceClass) == (token == 'FAT')
    assert 'fat_suppressed' not in r.sequenceFamily if token == 'FAT' else True


@pytest.mark.parametrize('name', ['t1_fatsat', 'T1 fat sat', 't1 fat suppression', 't1_fs'])
def test_fatsat_never_fat_image(cfg, name):
    r = row(ProtocolName=name, SeriesDescription=name)
    assert detect_fat_suppression(r, cfg)
    assert get_subtype_suffix(r, cfg) == ''


def test_plain_dixon_unknown_and_water_excitation_separate(cfg):
    assert not detect_fat_suppression(row(ProtocolName='t1_dixon', SeriesDescription=''), cfg)
    attrs = fat_attributes(row(ProtocolName='water excitation', SeriesDescription=''), cfg)
    assert attrs['fat_suppression_method']['value'] == 'WATER_EXCITATION'
    assert attrs['water_fat_component']['status'] == 'unknown'


def test_structured_name_conflict_explicit(cfg):
    r = row(ProtocolName='t1_water', SeriesDescription='', ImageType='ORIGINAL\\PRIMARY\\FAT')
    attrs = fat_attributes(r, cfg)
    assert attrs['water_fat_component']['status'] == 'conflict'
    assert len(attrs['water_fat_component']['evidence']) == 2
    assert get_subtype_suffix(r, cfg) == ''
    assert not detect_fat_suppression(r, cfg)


@pytest.mark.parametrize('agent', ['', ' ', None, np.nan, 'none', 'null', '<NA>', 'no'])
def test_empty_agent_never_enhanced(agent, cfg):
    # Even an old user config cannot reenable the unsafe T1 override.
    cfg['post_processing']['contrast_enhancement_override']['enabled'] = True
    r = process_mri_dataframe(pd.DataFrame([row(ContrastBolusAgent=agent)]), cfg).iloc[0]
    assert not r.isContrastEnhanced
    assert r.dynamicPhase == 'UNKNOWN'


@pytest.mark.parametrize('name', ['precontrast', 'noncontrast', 'pre contrast', 'unenhanced', '增强前', '平扫', 'dyn'])
def test_pre_names_and_dyn_not_post(cfg, name):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='t1_'+name, SeriesDescription='',
                                               ContrastBolusAgent='Gadovist')]), cfg).iloc[0]
    assert not r.isContrastEnhanced
    assert r.dynamicPhase == ('UNKNOWN' if name == 'dyn' else 'PRE')


def test_agent_record_does_not_establish_administration(cfg):
    attrs = contrast_attributes(row(ContrastBolusAgent='Gadovist'))
    assert attrs['contrast_agent_recorded']['value'] is True
    assert attrs['contrast_administration_state']['status'] == 'unknown'
    assert attrs['enhancement_observed']['status'] == 'unknown'


def test_pre_never_silently_overwritten(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(dynamicPhase='PRE', ContrastBolusAgent='Gadovist')]), cfg).iloc[0]
    assert r.dynamicPhase == 'PRE'
    assert not r.isContrastEnhanced
    r = process_mri_dataframe(pd.DataFrame([row(dynamicPhase='PRE', ProtocolName='t1_post',
                                               ContrastBolusAgent='Gadovist')]), cfg).iloc[0]
    assert r.dynamicPhase == 'CONFLICT'
    assert not r.isContrastEnhanced


def test_frame_administration_and_actual_enhancement_separate(cfg):
    attrs = contrast_attributes(row(ContrastBolusAgentAdministered='YES'))
    assert attrs['contrast_administration_state']['value'] == 'POST'
    assert attrs['contrast_administration_state']['status'] == 'known'
    assert attrs['enhancement_observed']['status'] == 'unknown'
    attrs = contrast_attributes(row(ProtocolName='precontrast', ContrastBolusAgentAdministered='YES'))
    assert attrs['contrast_administration_state']['status'] == 'conflict'


def test_repeats_not_pre_post_and_times_cross_midnight(cfg):
    rows = [row(AcquisitionDate='20261030', AcquisitionTime='235900'),
            row(SeriesInstanceUID='1.2.3.5', AcquisitionDate='20261031', AcquisitionTime='000100')]
    r = process_mri_dataframe(pd.DataFrame(rows), cfg)
    assert r.temporal_group.nunique() == 1
    assert r.temporal_index.tolist() == [0, 1]
    assert not r.isContrastEnhanced.any()
    assert r.dynamicPhase.tolist() == ['UNKNOWN', 'UNKNOWN']
    assert json.loads(r.iloc[0].temporal_evidence)['registration'] == 'unverified'


@pytest.mark.parametrize('changes', [dict(ImagePositionPatient=''), dict(RepetitionTime=None),
                                     dict(ImageOrientationPatient=[0, 0, 0, 0, 0, 0])])
def test_missing_geometry_never_matches_missing(cfg, changes):
    rows = [row(**changes), row(SeriesInstanceUID='1.2.3.5', **changes)]
    r = process_mri_dataframe(pd.DataFrame(rows), cfg)
    assert r.temporal_group.isna().all()


def test_frame_uid_and_duplicate_series_protect_groups(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(), row(FrameOfReferenceUID='other', SeriesInstanceUID='other')]), cfg)
    assert r.temporal_group.isna().all()
    r = process_mri_dataframe(pd.DataFrame([row(), row()]), cfg)
    assert r.temporal_group.isna().all()


def test_time_ties_do_not_get_invented_indices(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(), row(SeriesInstanceUID='other')]), cfg)
    assert r.temporal_group.notna().all()
    assert r.temporal_index.isna().all()


def test_stir_threshold_shared_and_ir_required(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='anonymous', SeriesDescription='anonymous',
                                               ScanningSequence=['SE', 'IR'], InversionTime=280)]), cfg).iloc[0]
    assert r.sequenceClass == 'T2_STIR'
    assert r.isFatSuppressed
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='anonymous', SeriesDescription='anonymous',
                                               ScanningSequence='GR', InversionTime=280)]), cfg).iloc[0]
    assert r.sequenceClass != 'T2_STIR'
    assert not r.isFatSuppressed


def test_structured_weighting_precedes_tr_te(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='anonymous', SeriesDescription='',
                                               EchoPulseSequence='SPIN', ScanningSequence='SE',
                                               AcquisitionContrast='T2')]), cfg).iloc[0]
    assert r.sequenceClass == 'T2_SE'
    assert json.loads(r.mr_semantics_v2)['attributes']['contrast_weighting']['status'] == 'known'


def test_localizer_and_empty_description_do_not_erase_v2(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='scout', SeriesDescription='')]), cfg)
    v2 = build_v2_dataframe(r).iloc[0]
    assert v2.image_role == 'LOCALIZER'
    assert v2.eligible_for_diagnostic_training == False
    assert v2.eligible_for_anatomical_localization == 'UNKNOWN'
    assert v2.orientation_status == 'known'


def test_secondary_is_not_derived_and_phase_not_opposed(cfg):
    attrs = build_semantics(row(ImageType=['ORIGINAL', 'SECONDARY', 'P', 'OUTPHASE']), cfg)
    assert attrs['is_secondary']['value'] is True
    assert attrs['is_derived']['value'] is False
    assert attrs['complex_signal_component']['value'] == 'PHASE'
    assert attrs['water_fat_component']['value'] == 'OPPOSED_PHASE'


def test_evidence_and_mask_roundtrip(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(ImageType=['ORIGINAL', 'PRIMARY', 'FAT'])]), cfg).iloc[0]
    obj = json.loads(r.mr_semantics_v2)
    assert obj['supervision_mask']['water_fat_component'] is True
    assert obj['supervision_mask']['enhancement_observed'] is False
    for f in obj['attributes'].values():
        assert {'value', 'status', 'evidence', 'rule_id', 'rule_version'} <= f.keys()
    assert '_C+' not in generate_standardized_name(r)


def dicom_file(tmp_path, name, series_uid=None):
    meta = FileMetaDataset()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(None, {}, file_meta=meta, preamble=b'\0'*128)
    ds.SOPClassUID = EnhancedMRImageStorage
    ds.SOPInstanceUID = generate_uid()
    ds.SeriesInstanceUID = series_uid or generate_uid()
    ds.StudyInstanceUID = generate_uid()
    ds.Modality = 'MR'
    ds.ProtocolName = ds.SeriesDescription = 'diffusion'
    ds.EchoTime = 10
    ds.DiffusionBValue = 0
    return ds, tmp_path / name


def test_mixed_classic_echo_b_preserves_membership(tmp_path):
    uid = generate_uid()
    paths = []
    for i, (te, b) in enumerate([(10, 0), (10, 1000), (20, 1000)]):
        ds, path = dicom_file(tmp_path, str(i)+'.dcm', uid)
        ds.EchoTime, ds.DiffusionBValue = te, b
        ds.save_as(path)
        paths.append(str(path))
    result = build_mr_frame_index(paths)
    assert result['status'] == 'complete'
    assert result['echo_times'] == [10, 20]
    assert result['b_values'] == [0, 1000]
    assert result['heterogeneous']
    assert len(result['candidate_volumes']) == 3
    assert {g['members'][0]['file'] for g in result['candidate_volumes']} == {'0.dcm', '1.dcm', '2.dcm'}
    assert result['output_volume_mapping_status'] == 'UNVERIFIED'


def test_enhanced_shared_perframe_precedence(tmp_path):
    ds, path = dicom_file(tmp_path, 'enhanced.dcm')
    ds.NumberOfFrames = 2
    shared = Dataset()
    echo = Dataset(); echo.EffectiveEchoTime = 40
    shared.MREchoSequence = Sequence([echo])
    ds.SharedFunctionalGroupsSequence = Sequence([shared])
    frames = []
    for i, b in enumerate([0, 800]):
        frame = Dataset()
        diff = Dataset(); diff.DiffusionBValue = b
        gradient = Dataset(); gradient.DiffusionGradientOrientation = [1, 0, 0]
        diff.DiffusionGradientDirectionSequence = Sequence([gradient])
        frame.MRDiffusionSequence = Sequence([diff])
        content = Dataset(); content.TemporalPositionIndex = i + 1
        frame.FrameContentSequence = Sequence([content])
        frames.append(frame)
    ds.PerFrameFunctionalGroupsSequence = Sequence(frames)
    ds.save_as(path)
    rep = representative_metadata(ds)
    assert rep['EchoTime'] == 40
    assert rep['b_value'] == 0
    assert 'PerFrame' in json.loads(rep['mr_metadata_sources'])['b_value']
    result = build_mr_frame_index([str(path)])
    assert result['echo_times'] == [40]
    assert result['b_values'] == [0, 800]
    assert [g['members'][0]['frame'] for g in result['candidate_volumes']] == [1, 2]
    assert result['candidate_volumes'][0]['attributes']['DiffusionGradientOrientation'] == [1, 0, 0]


def test_index_incomplete_is_not_silently_complete(tmp_path):
    ds, path = dicom_file(tmp_path, 'ok.dcm')
    ds.save_as(path)
    result = build_mr_frame_index([str(path), str(tmp_path/'missing')])
    assert result['status'] == 'incomplete'
    assert len(result['errors']) == 1


def test_index_opt_in_and_heterogeneous_mask(tmp_path, monkeypatch, cfg):
    records = [row()]
    monkeypatch.delenv('MR_FRAME_INDEX_ENABLED', raising=False)
    attach_mr_index(records, ['missing'], str(tmp_path))
    assert 'mr_index_status' not in records[0]
    monkeypatch.setenv('MR_FRAME_INDEX_ENABLED', '1')
    attach_mr_index(records, ['missing'], str(tmp_path))
    assert records[0]['mr_index_status'] == 'incomplete'
    assert (tmp_path/'mr_frame_index.json').exists()
    r = process_mri_dataframe(pd.DataFrame([row(mr_heterogeneous=True)]), cfg).iloc[0]
    assert not any(json.loads(r.mr_semantics_v2)['supervision_mask'].values())


def test_result_cache_includes_semantics_and_index_setting(monkeypatch):
    from src.utils.cache import _compute_param_hash
    from src.core import mr_semantics
    monkeypatch.delenv('MR_FRAME_INDEX_ENABLED', raising=False)
    before = _compute_param_hash('synthetic', {})
    monkeypatch.setenv('MR_FRAME_INDEX_ENABLED', '1')
    assert _compute_param_hash('synthetic', {}) != before
    monkeypatch.delenv('MR_FRAME_INDEX_ENABLED')
    monkeypatch.setattr(mr_semantics, 'RULE_VERSION', 'future-rule-version')
    assert _compute_param_hash('synthetic', {}) != before


def test_rule_config_changes_fingerprint(tmp_path):
    from src.core.mr_semantics import rules_fingerprint
    config = tmp_path/'rules.json'
    config.write_text('{}')
    first = rules_fingerprint(config)
    config.write_text('{"changed":true}')
    assert rules_fingerprint(config) != first


def test_excel_export_writes_parallel_v2_sheet(tmp_path):
    from src.client.unified import DICOMDownloadClient
    client = DICOMDownloadClient.__new__(DICOMDownloadClient)
    client.progress_callback = None
    path = tmp_path/'mr.xlsx'
    df = pd.DataFrame([dict(row(), Modality='MR')])
    df.to_excel(path, sheet_name='DICOM_Metadata', index=False)
    client._append_mr_cleaned_sheet(df, str(path))
    with pd.ExcelFile(path) as book:
        assert {'DICOM_Metadata', 'MR_Cleaned', 'MR_Cleaned_v2'} <= set(book.sheet_names)
        legacy = pd.read_excel(book, 'MR_Cleaned')
        v2 = pd.read_excel(book, 'MR_Cleaned_v2')
    assert 'mr_semantics_v2' not in legacy
    attrs = json.loads(v2.iloc[0].attribute_evidence_json)
    assert attrs['enhancement_observed']['status'] == 'unknown'
    assert v2.iloc[0].contrast_administration_state == 'UNKNOWN'


def test_cache_retains_index_after_dicom_deletion(tmp_path, monkeypatch):
    from src.client.unified import DICOMDownloadClient
    from src.core.metadata import extract_dicom_metadata
    client = DICOMDownloadClient.__new__(DICOMDownloadClient)
    client.progress_callback = None
    client.modality_keywords = {'MR': json.loads((Path(__file__).parents[1]/'dicom_tags/mr.json').read_text())}
    folder = tmp_path/'series'; folder.mkdir()
    paths = []
    series_uid = generate_uid()
    for i, b in enumerate([0, 1000]):
        ds, path = dicom_file(folder, str(i)+'.dcm', series_uid)
        ds.DiffusionBValue = b
        ds.save_as(path)
        paths.append(str(path))
    monkeypatch.setenv('MR_FRAME_INDEX_ENABLED', '1')
    client._cache_metadata_for_series(str(folder), 'series', paths, 'MR')
    cache_path = folder/'dicom_metadata_cache.json'
    cache = json.loads(cache_path.read_text())
    assert cache['records'][0]['b_values'] == '[0.0, 1000.0]'
    for path in paths:
        Path(path).unlink()
    out = tmp_path/'cached.xlsx'
    result = extract_dicom_metadata(str(tmp_path), str(out), client.get_keywords,
        lambda _: ([], None), lambda *a, **k: 0, lambda *a: {'low_quality': 0},
        client._append_mr_cleaned_sheet)
    assert result == str(out)
    v2 = pd.read_excel(out, 'MR_Cleaned_v2')
    assert v2.iloc[0].b_values == '[0.0, 1000.0]'
    assert v2.iloc[0].mr_heterogeneous
    assert not any(json.loads(v2.iloc[0].supervision_mask_json).values())
    assert json.loads((folder/'mr_frame_index.json').read_text())['frame_count'] == 2


def test_direct_metadata_extracts_standard_b_and_sources(tmp_path, monkeypatch):
    from src.client.unified import DICOMDownloadClient
    from src.core.metadata import extract_dicom_metadata
    client = DICOMDownloadClient.__new__(DICOMDownloadClient)
    client.progress_callback = None
    client.modality_keywords = {'MR': json.loads((Path(__file__).parents[1]/'dicom_tags/mr.json').read_text())}
    folder = tmp_path/'series'; folder.mkdir()
    ds, path = dicom_file(folder, 'single.dcm')
    ds.DiffusionBValue = 800
    ds.ImageType = ['ORIGINAL', 'PRIMARY', 'WATER']
    ds.save_as(path)
    monkeypatch.delenv('MR_FRAME_INDEX_ENABLED', raising=False)
    out = tmp_path/'direct.xlsx'
    result = extract_dicom_metadata(str(tmp_path), str(out), client.get_keywords,
        lambda _: ([], None), lambda *a, **k: 0, lambda *a: {'low_quality': 0}, client._append_mr_cleaned_sheet)
    assert result == str(out)
    raw = pd.read_excel(out, 'DICOM_Metadata').iloc[0]
    assert raw.b_value == 800
    assert json.loads(raw.mr_metadata_sources)['b_value'] == 'DiffusionBValue'
    v2 = pd.read_excel(out, 'MR_Cleaned_v2').iloc[0]
    assert v2.water_fat_component == 'WATER_ONLY'
    assert not (folder/'mr_frame_index.json').exists()


def test_empty_dataframe_supported(cfg):
    assert process_mri_dataframe(pd.DataFrame(), cfg).empty


def test_partial_datetime_not_treated_as_full_time():
    assert acquisition_time({'AcquisitionDateTime': '202610'})[0] is None
    assert acquisition_time({'SeriesTime': '1234'})[0] is None
    assert acquisition_time({'SeriesTime': 100})[0] == ('TM', 60)


def test_standard_diffusion_b_dataframe_alias(cfg):
    r = process_mri_dataframe(pd.DataFrame([row(ProtocolName='anonymous', SeriesDescription='',
                                               DiffusionBValue=1000)]), cfg).iloc[0]
    assert r.sequenceClass == 'DWI'
    assert r.b_value == 1000
