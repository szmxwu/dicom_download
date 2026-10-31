"""Header-only MR instance/frame index. Never claims NIfTI volume registration."""
import json
import os

from .mr_semantics import number, parse_multivalue, text_value


# Explicit paths, rather than a recursive first-match search, preserve frame provenance.
FUNCTIONAL_PATHS = {
    'EchoTime': ('MREchoSequence', 'EffectiveEchoTime'),
    'InversionTime': ('MRModifierSequence', 'InversionTimes'),
    'DiffusionBValue': ('MRDiffusionSequence', 'DiffusionBValue'),
    'DiffusionGradientOrientation': ('MRDiffusionSequence', 'DiffusionGradientDirectionSequence', 'DiffusionGradientOrientation'),
    'TemporalPositionIndex': ('FrameContentSequence', 'TemporalPositionIndex'),
    'FrameAcquisitionDateTime': ('FrameContentSequence', 'FrameAcquisitionDateTime'),
    'ImagePositionPatient': ('PlanePositionSequence', 'ImagePositionPatient'),
    'ImageOrientationPatient': ('PlaneOrientationSequence', 'ImageOrientationPatient'),
    'ImageType': ('MRImageFrameTypeSequence', 'FrameType'),
    'AcquisitionContrast': ('MRImageFrameTypeSequence', 'AcquisitionContrast'),
    'ComplexImageComponent': ('MRImageFrameTypeSequence', 'ComplexImageComponent'),
    'RepetitionTime': ('MRTimingAndRelatedParametersSequence', 'RepetitionTime'),
    'FlipAngle': ('MRTimingAndRelatedParametersSequence', 'FlipAngle'),
    'SpectrallySelectedSuppression': ('MRModifierSequence', 'SpectrallySelectedSuppression'),
    'InversionRecovery': ('MRModifierSequence', 'InversionRecovery'),
    'ContrastBolusAgentAdministered': ('ContrastBolusUsageSequence', 'ContrastBolusAgentAdministered'),
}
INDEX_FIELDS = tuple(dict.fromkeys((*FUNCTIONAL_PATHS, 'EchoNumbers', 'TemporalPositionIdentifier',
                                  'EchoPulseSequence', 'SteadyStatePulseSequence', 'AcquisitionDateTime', 'AcquisitionDate',
                                  'AcquisitionTime', 'SeriesDate', 'SeriesTime', 'FrameOfReferenceUID')))


def _items(ds, key):
    return getattr(ds, key, None) or []


def _plain(value):
    if isinstance(value, (str, int, float)):
        return str(value) if isinstance(value, str) else value
    if hasattr(value, '__iter__'):
        return [_plain(v) for v in value]
    return str(value)


def _functional_value(group, key):
    path = FUNCTIONAL_PATHS.get(key)
    if not path:
        return None
    current = group
    for sequence in path[:-1]:
        items = _items(current, sequence)
        if not items:
            return None
        current = items[0]
    return getattr(current, path[-1], None)


def frame_metadata(ds, frame_index=0):
    """Per-frame > shared > top-level. Return both values and exact source paths."""
    values, sources = {}, {}
    per = _items(ds, 'PerFrameFunctionalGroupsSequence')
    shared = _items(ds, 'SharedFunctionalGroupsSequence')
    for key in INDEX_FIELDS:
        candidates = []
        if frame_index < len(per):
            candidates.append((_functional_value(per[frame_index], key),
                               f'PerFrameFunctionalGroupsSequence[{frame_index}].' + '.'.join(FUNCTIONAL_PATHS.get(key, (key,)))))
        if shared:
            candidates.append((_functional_value(shared[0], key),
                               'SharedFunctionalGroupsSequence[0].' + '.'.join(FUNCTIONAL_PATHS.get(key, (key,)))))
        candidates.append((getattr(ds, key, None), key))
        for val, source in candidates:
            if val is not None and text_value(val):
                if key == 'InversionTime' and hasattr(val, '__iter__') and not isinstance(val, str):
                    if len(val) != 1:
                        # Multi-TI preparation is not one scalar TI for legacy rules.
                        continue
                    val = val[0]
                values[key] = _plain(val)
                sources[key] = source
                break
    # Legacy public alias; source is always explicit.
    if 'DiffusionBValue' in values:
        values['b_value'] = values['DiffusionBValue']
        sources['b_value'] = sources['DiffusionBValue']
    return values, sources


def representative_metadata(ds):
    """A representative frame is explicitly scoped; it is never a series-wide label."""
    values, sources = frame_metadata(ds)
    values['mr_metadata_sources'] = json.dumps(sources, ensure_ascii=False, sort_keys=True)
    values['mr_metadata_scope'] = 'representative_frame'
    return values


def build_mr_frame_index(paths):
    """Read all headers only, retain SOP/frame membership and heterogeneity.

    Volume groups are candidates. Repeated identical acquisitions may share a group;
    missing diffusion directions or temporal indices cannot establish exact volumes.
    """
    import pydicom
    groups, errors, series_uids = {}, [], set()
    echoes, b_values = set(), set()
    frame_count = 0
    for path in sorted(paths):
        try:
            ds = pydicom.dcmread(path, stop_before_pixels=True, force=True)
            if str(getattr(ds, 'Modality', '')).upper() != 'MR' or not text_value(getattr(ds, 'SOPInstanceUID', None)):
                raise ValueError('not an identifiable MR instance')
            uid = text_value(getattr(ds, 'SeriesInstanceUID', None))
            series_uids.add(uid)
            per = _items(ds, 'PerFrameFunctionalGroupsSequence')
            count = int(getattr(ds, 'NumberOfFrames', 1) or 1)
            if count < 1 or (per and len(per) != count):
                raise ValueError('frame-count/functional-group mismatch')
            for frame in range(count):
                values, sources = frame_metadata(ds, frame)
                echo = number(values.get('EchoTime'))
                b = number(values.get('DiffusionBValue'))
                if echo is not None:
                    echoes.add(echo)
                if b is not None:
                    b_values.add(b)
                # Slice position is excluded from the volume key and retained per member.
                attrs = {k: v for k, v in values.items() if k not in {'ImagePositionPatient', 'FrameAcquisitionDateTime',
                         'AcquisitionDateTime', 'AcquisitionDate', 'AcquisitionTime', 'SeriesDate', 'SeriesTime', 'b_value'}}
                key = json.dumps([uid, attrs], sort_keys=True)
                if key not in groups:
                    groups[key] = dict(candidate_volume_id=len(groups) + 1, attributes=attrs,
                                       sources=sources, members=[])
                groups[key]['members'].append(dict(file=os.path.basename(path),
                    sop_instance_uid=str(getattr(ds, 'SOPInstanceUID', '')), frame=frame + 1,
                    position=values.get('ImagePositionPatient'),
                    acquisition_datetime=values.get('FrameAcquisitionDateTime', values.get('AcquisitionDateTime')),
                    sources=sources))
                frame_count += 1
        except Exception as exc:
            errors.append(dict(file=os.path.basename(path), error=type(exc).__name__))
    complete = not errors and frame_count > 0 and len(series_uids) == 1 and '' not in series_uids
    return dict(schema_version='mr-frame-index-v1', status='complete' if complete else 'incomplete',
                scope='source_instances_and_frames', frame_count=frame_count, file_count=len(paths),
                echo_times=sorted(echoes), b_values=sorted(b_values),
                heterogeneous=len(groups) > 1, candidate_volumes=list(groups.values()), errors=errors,
                output_volume_mapping_status='UNVERIFIED')


def index_columns(index):
    """Excel-safe summary. Full membership lives in the JSON sidecar/cache."""
    return dict(mr_index_status=index['status'], mr_index_scope=index['scope'],
                echo_times=json.dumps(index['echo_times']), b_values=json.dumps(index['b_values']),
                mr_heterogeneous=index['heterogeneous'], mr_frame_count=index['frame_count'],
                mr_output_volume_mapping_status=index['output_volume_mapping_status'])


def indexing_enabled():
    return os.environ.get('MR_FRAME_INDEX_ENABLED', '').lower() in {'1', 'true', 'yes'}


def attach_mr_index(records, paths, series_dir):
    """Opt-in I/O, called before source deletion as well as on direct extraction."""
    if not indexing_enabled():
        return
    index = build_mr_frame_index(paths)
    sidecar = os.path.join(series_dir, 'mr_frame_index.json')
    with open(sidecar, 'w', encoding='utf-8') as stream:
        json.dump(index, stream, ensure_ascii=False, allow_nan=False)
    for record in records:
        record.update(index_columns(index))
        record['mr_frame_index_file'] = 'mr_frame_index.json'
