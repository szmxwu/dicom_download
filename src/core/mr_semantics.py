"""Evidence-bearing MR attributes. No pixel observations are inferred here."""
import ast
import hashlib
import json
import math
import re


RULE_VERSION = 'mr-semantic-v2.1'
MISSING = {'', 'nan', 'none', 'null', 'n/a', 'unknown', '<na>'}


def rules_fingerprint(config_path=None):
    """Invalidate result ZIPs when semantics/config changes, without importing pandas."""
    from pathlib import Path
    path = Path(config_path) if config_path else Path(__file__).resolve().parents[2] / 'mr_clean_config.json'
    try:
        contents = path.read_bytes()
    except OSError:
        contents = b'config-unavailable'
    return RULE_VERSION + ':' + hashlib.sha256(contents).hexdigest()


def text_value(value):
    text = str(value).strip() if value is not None else ''
    return '' if text.lower() in MISSING else text


def parse_multivalue(value):
    """Accept native MultiValue/list, JSON/Python list strings and DICOM VM strings."""
    if isinstance(value, str):
        value = value.strip()
        if value.startswith(('[', '(')):
            try:
                parsed = ast.literal_eval(value)
                if isinstance(parsed, (list, tuple)):
                    value = parsed
            except (ValueError, SyntaxError):
                pass
        if isinstance(value, str):
            value = value.split('\\')
    if hasattr(value, '__iter__') and not isinstance(value, (str, bytes, dict)):
        return [str(v).strip().upper() for v in value if text_value(v)]
    return [text_value(value).upper()] if text_value(value) else []


def number(value):
    try:
        val = float(value)
        return val if math.isfinite(val) else None
    except (TypeError, ValueError):
        return None


def fact(value='UNKNOWN', status=None, evidence=None, rule_id='missing'):
    return dict(value=value, status=status or ('unknown' if value == 'UNKNOWN' else 'known'),
                evidence=evidence or [], rule_id=rule_id, rule_version=RULE_VERSION)


def evidence(row, *fields):
    try:
        sources = json.loads(text_value(row.get('mr_metadata_sources')) or '{}')
    except (ValueError, TypeError):
        sources = {}
    return [{'field': f, 'value': str(row.get(f)), 'source': sources.get(f, f)}
            for f in fields if text_value(row.get(f))]


def resolve(candidates, rule_id):
    """Candidates are (value, status, evidence); contradictory evidence is never hidden."""
    candidates = [c for c in candidates if c[0] != 'UNKNOWN']
    if not candidates:
        return fact(rule_id=rule_id)
    ev = [item for _, _, entries in candidates for item in entries]
    values = {v for v, _, _ in candidates}
    if len(values) > 1:
        return fact('CONFLICT', 'conflict', ev, rule_id)
    return fact(candidates[0][0], 'known' if any(s == 'known' for _, s, _ in candidates)
                else 'inferred', ev, rule_id)


def name_text(row):
    return ' '.join([text_value(row.get('ProtocolName')) or text_value(row.get('protocolName_lower')),
                     text_value(row.get('SeriesDescription'))]).lower()


def _component_values(tokens):
    aliases = {'W': 'WATER_ONLY', 'WATER': 'WATER_ONLY', 'WATER_ONLY': 'WATER_ONLY',
               'F': 'FAT_ONLY', 'FAT': 'FAT_ONLY', 'FAT_ONLY': 'FAT_ONLY',
               'IP': 'IN_PHASE', 'INPHASE': 'IN_PHASE', 'IN_PHASE': 'IN_PHASE',
               'OP': 'OPPOSED_PHASE', 'OUTPHASE': 'OPPOSED_PHASE',
               'OUT_PHASE': 'OPPOSED_PHASE', 'OPPOSED_PHASE': 'OPPOSED_PHASE'}
    return {aliases[t] for t in tokens if t in aliases}


def water_fat_component(row):
    candidates = [(v, 'known', evidence(row, 'ImageType'))
                  for v in _component_values(parse_multivalue(row.get('ImageType')))]
    # Token boundaries prevent "fat" in fatsat/fat sat/fat suppression becoming a fat image.
    name = name_text(row)
    patterns = {
        'WATER_ONLY': r'(?<![a-z0-9])(?:water(?![ _-]?exc)(?:[ _-]?only)?|w)(?![a-z0-9])',
        'FAT_ONLY': r'(?<![a-z0-9])(?:fat(?![ _-]*(?:sat|supp|sep))|fat[ _-]?only|f)(?![a-z0-9])',
        'IN_PHASE': r'(?<![a-z0-9])(?:in[ _-]?phase|ip)(?![a-z0-9])',
        'OPPOSED_PHASE': r'(?<![a-z0-9])(?:out[ _-]?phase|opposed[ _-]?phase|op)(?![a-z0-9])',
    }
    candidates += [(v, 'inferred', evidence(row, 'ProtocolName', 'SeriesDescription'))
                   for v, pattern in patterns.items() if re.search(pattern, name)]
    return resolve(candidates, 'water-fat-component')


def field_thresholds(row, cfg):
    fields = cfg.get('thresholds', {}).get('field_strength', {})
    key = text_value(row.get('standardFieldStrength'))
    strength = number(row.get('MagneticFieldStrength'))
    if not key and strength is not None:
        key = 'Low-Field' if strength < 1 else '1.5T' if strength < 2 else '3.0T' if strength < 4 else 'High-Field'
    return fields.get(key, fields.get('default', {}))


def is_stir_candidate(row, cfg):
    tokens = parse_multivalue(row.get('ScanningSequence'))
    ir = 'IR' in tokens or text_value(row.get('InversionRecovery')).upper() == 'YES'
    ti = number(row.get('InversionTime'))
    fs = cfg.get('fat_suppression', {})
    upper = number(field_thresholds(row, cfg).get('stir_ti_max', fs.get('stir_ti_max', 250)))
    return bool(ir and ti is not None and upper is not None
                and float(fs.get('stir_ti_min', 100)) <= ti <= upper)


def fat_attributes(row, cfg):
    name = name_text(row)
    component = water_fat_component(row)
    separation = fact('DIXON', 'inferred', evidence(row, 'ProtocolName', 'SeriesDescription'),
                      'dixon-name') if 'dixon' in name else fact(rule_id='dixon-unknown')
    candidates = []
    suppression = text_value(row.get('SpectrallySelectedSuppression')).upper()
    if suppression in ('FAT', 'FAT_AND_WATER'):
        candidates.append(('SPECTRAL', 'known', evidence(row, 'SpectrallySelectedSuppression')))
    elif suppression == 'NONE':
        # NONE only rules out spectral suppression, not STIR or water excitation.
        pass
    if 'FS' in parse_multivalue(row.get('ScanOptions')):
        candidates.append(('SPECTRAL', 'inferred', evidence(row, 'ScanOptions')))
    patterns = {'STIR': r'(?<![a-z])stir(?![a-z])', 'SPAIR': r'spair',
                'WATER_EXCITATION': r'water[ _-]?exc(?:itation)?',
                'SPECTRAL': r'(?<![a-z])(?:fs|fat[ _-]?sat(?:uration)?|fat[ _-]?suppression|sp[ei]r)(?![a-z])'}
    for method, pattern in patterns.items():
        if re.search(pattern, name):
            candidates.append((method, 'inferred', evidence(row, 'ProtocolName', 'SeriesDescription')))
    if is_stir_candidate(row, cfg):
        candidates.append(('STIR', 'inferred', evidence(row, 'ScanningSequence', 'InversionRecovery',
                                                     'InversionTime', 'MagneticFieldStrength')))
    # SPAIR is a more specific spectral technique; FS + SPAIR is not a contradiction.
    if any(v == 'SPAIR' for v, _, _ in candidates):
        candidates = [(v, s, e) for v, s, e in candidates if v != 'SPECTRAL']
    method = resolve(candidates, 'fat-suppression-technique')
    nominal = fact(rule_id='nominal-fat-removal')
    if component['status'] == 'conflict' or method['status'] == 'conflict':
        nominal = fact('CONFLICT', 'conflict', component['evidence'] + method['evidence'], 'nominal-fat-removal')
    elif component['value'] == 'WATER_ONLY':
        nominal = fact(True, component['status'], component['evidence'], 'water-only-fat-removal')
    elif component['value'] in ('FAT_ONLY', 'IN_PHASE', 'OPPOSED_PHASE'):
        nominal = fact(False, component['status'], component['evidence'], 'component-not-fat-suppressed')
        if method['value'] != 'UNKNOWN':
            nominal = fact('CONFLICT', 'conflict', component['evidence'] + method['evidence'], 'component-suppression-conflict')
    elif method['value'] != 'UNKNOWN':
        nominal = fact(True, method['status'], method['evidence'], 'suppression-intent')
    return dict(fat_water_separation=separation, water_fat_component=component,
                fat_suppression_method=method, nominal_fat_signal_removed=nominal,
                fat_suppression_effect_observed=fact(rule_id='requires-image-qc'))


def contrast_attributes(row):
    agent = text_value(row.get('ContrastBolusAgent'))
    if not agent:
        recorded = fact(rule_id='contrast-agent-missing')
    elif agent.lower() in {'no', 'none', 'false', '0', '无', '未使用'}:
        recorded = fact(False, evidence=evidence(row, 'ContrastBolusAgent'), rule_id='contrast-agent-negative')
    else:
        recorded = fact(True, evidence=evidence(row, 'ContrastBolusAgent'), rule_id='contrast-agent-recorded')
    candidates = []
    for key in ('ProtocolName', 'SeriesDescription', 'dynamicPhase'):
        text = text_value(row.get(key)).lower()
        ev = evidence(row, key)
        if text == 'conflict':
            candidates.extend([('PRE', 'inferred', ev), ('POST', 'inferred', ev)])
        pre = r'(?<![a-z])(?:pre(?:[ _-]?(?:contrast|con|gd|enh))?|non[ _-]?contrast|unenhanced|without[ _-]?contrast)(?![a-z])|平扫|增强前'
        if re.search(pre, text):
            candidates.append(('PRE', 'inferred', ev))
        # Remove negative/pre phrases before checking positive evidence in the same field.
        remainder = re.sub(pre, '', text)
        post = r'\+c|(?<![a-z])(?:post(?:[ _-]?contrast)?|gd|enh(?:anced)?|contrast)(?![a-z])|增强后|增强'
        if re.search(post, remainder):
            candidates.append(('POST', 'inferred', ev))
    # DICOM frame-level administration indicator, unlike an agent name, describes timing.
    administered = text_value(row.get('ContrastBolusAgentAdministered')).upper()
    if administered in ('YES', 'NO'):
        candidates.append(('POST' if administered == 'YES' else 'PRE', 'known',
                           evidence(row, 'ContrastBolusAgentAdministered')))
    state = resolve(candidates, 'contrast-administration-state')
    if recorded['value'] is False and state['value'] == 'POST':
        state = fact('CONFLICT', 'conflict', state['evidence'] + recorded['evidence'], 'contrast-agent-state-conflict')
    return dict(contrast_agent_recorded=recorded, contrast_administration_state=state,
                enhancement_observed=fact(rule_id='requires-image-comparison'))


def acquisition_time(row):
    """Return (comparable key, source); never compare a full DT with a time-only value."""
    import datetime
    for field in ('FrameAcquisitionDateTime', 'AcquisitionDateTime'):
        val = text_value(row.get(field))
        if re.fullmatch(r'\d{14}(?:\.\d{1,6})?(?:[+-]\d{4})?', val):
            try:
                from pydicom.valuerep import DT
                dt = DT(val)
                if dt is not None:
                    aware = dt.tzinfo is not None
                    seconds = dt.timestamp() if aware else (dt - datetime.datetime(1970, 1, 1)).total_seconds()
                    return ('DT_TZ' if aware else 'DT', seconds), field
            except (ValueError, TypeError, OverflowError):
                pass
    for date_field, time_field in (('AcquisitionDate', 'AcquisitionTime'), ('SeriesDate', 'SeriesTime')):
        val = text_value(row.get(time_field))
        if not val:
            continue
        try:
            from pydicom.valuerep import DA, TM
            # Numeric Excel HHMMSS values may have lost leading zeroes.
            whole, _, frac = val.partition('.')
            raw = row.get(time_field)
            if not isinstance(raw, str):
                whole = whole.zfill(6)
            if len(whole) != 6:
                continue  # partial TM has ambiguous ordering; never invent precision
            tm = TM(whole + ('.' + frac if frac else ''))
            date_text = text_value(row.get(date_field))
            date = DA(date_text) if len(date_text) == 8 else None
            seconds = tm.hour * 3600 + tm.minute * 60 + tm.second + tm.microsecond / 1e6
            return (('DT', date.toordinal() * 86400 + seconds - datetime.date(1970, 1, 1).toordinal() * 86400)
                    if date else ('TM', seconds)), f'{date_field}/{time_field}' if date else time_field
        except (ValueError, TypeError, AttributeError):
            pass
    return None, ''


def build_semantics(row, cfg):
    attrs = fat_attributes(row, cfg)
    attrs.update(contrast_attributes(row))
    name = name_text(row)
    seq = text_value(row.get('sequenceClass'))
    acquisition = text_value(row.get('AcquisitionContrast')).upper()
    weighting = acquisition if acquisition in {'T1', 'T2', 'T2_STAR', 'PROTON_DENSITY', 'MIXED'} else 'UNKNOWN'
    weighting = {'T2_STAR': 'T2*', 'PROTON_DENSITY': 'PD'}.get(weighting, weighting)
    # The legacy class is a view; it never certifies V2 physics-derived labels.
    wc = [(weighting, 'known', evidence(row, 'AcquisitionContrast'))] if weighting != 'UNKNOWN' else []
    match = re.search(r'(?<![a-z0-9])(t1|t2\*?|pd)(?![a-z0-9])', name)
    if match:
        wc.append((match.group(1).upper(), 'inferred', evidence(row, 'ProtocolName', 'SeriesDescription')))
    attrs['contrast_weighting'] = resolve(wc, 'contrast-weighting')
    pulse = text_value(row.get('EchoPulseSequence')).upper()
    tokens = parse_multivalue(row.get('ScanningSequence'))
    pc = []
    if pulse in {'SPIN', 'GRADIENT', 'BOTH'}:
        pc.append(({'SPIN': 'SE', 'GRADIENT': 'GRE', 'BOTH': 'MIXED'}[pulse], 'known', evidence(row, 'EchoPulseSequence')))
    if 'SE' in tokens and 'GR' not in tokens:
        pc.append(('SE', 'known', evidence(row, 'ScanningSequence')))
    elif 'GR' in tokens and 'SE' not in tokens:
        pc.append(('GRE', 'known', evidence(row, 'ScanningSequence')))
    pulse_fact = resolve(pc, 'pulse-family')
    if pulse_fact['value'] == 'SE' and (number(row.get('EchoTrainLength')) or 0) > 1:
        pulse_fact = fact('TSE', 'inferred', pulse_fact['evidence'] + evidence(row, 'EchoTrainLength'), 'tse-echo-train')
    attrs['pulse_family'] = pulse_fact
    attrs['epi_readout'] = fact(True, evidence=evidence(row, 'ScanningSequence'), rule_id='epi-readout') if 'EP' in tokens else fact(rule_id='epi-unknown')
    steady = text_value(row.get('SteadyStatePulseSequence')).upper()
    attrs['steady_state'] = fact(steady, evidence=evidence(row, 'SteadyStatePulseSequence'), rule_id='steady-state') if steady else fact()
    ir = text_value(row.get('InversionRecovery')).upper()
    attrs['inversion_recovery'] = fact(True, evidence=evidence(row, 'ScanningSequence', 'InversionRecovery'), rule_id='ir-preparation') if 'IR' in tokens or ir == 'YES' else fact(False, evidence=evidence(row, 'InversionRecovery'), rule_id='ir-negative') if ir == 'NO' else fact()
    signal = text_value(row.get('ComplexImageComponent')).upper()
    sc = [(signal, 'known', evidence(row, 'ComplexImageComponent'))] if signal in {'MAGNITUDE', 'PHASE', 'REAL', 'IMAGINARY', 'MIXED'} else []
    for token, val in {'M': 'MAGNITUDE', 'P': 'PHASE', 'R': 'REAL', 'I': 'IMAGINARY'}.items():
        if token in parse_multivalue(row.get('ImageType')):
            sc.append((val, 'inferred', evidence(row, 'ImageType')))
    attrs['complex_signal_component'] = resolve(sc, 'complex-signal-component')
    localizers = cfg.get('excluded_patterns', {}).get('localizer_keywords', [])
    role = 'LOCALIZER' if any(k.lower() in name for k in localizers) or row.get('refinedImageType') == 'LOCALIZER' else 'UNKNOWN'
    if role == 'UNKNOWN':
        role = next((r for classes, r in [({'ADC', 'FA_MAP', 'T1_MAP', 'T2_MAP'}, 'QUANTITATIVE_MAP'),
                                         ({'DWI', 'DTI'}, 'DIFFUSION'), ({'SUBTRACTION'}, 'SUBTRACTION'),
                                         ({'MIP'}, 'PROJECTION')]
                     if seq in classes), 'ANATOMICAL' if seq.startswith(('T1_', 'T2_', 'PD_')) else 'UNKNOWN')
    attrs['image_role'] = fact(role, 'inferred' if role != 'UNKNOWN' else 'unknown',
                               evidence(row, 'ProtocolName', 'SeriesDescription', 'sequenceClass'), 'image-role')
    attrs['eligible_for_diagnostic_training'] = fact(False, 'inferred', attrs['image_role']['evidence'], 'localizer-policy') if role == 'LOCALIZER' else fact(rule_id='task-policy-unvalidated')
    attrs['eligible_for_anatomical_localization'] = fact(rule_id='task-policy-unvalidated')
    tokens = parse_multivalue(row.get('ImageType'))
    for key, positive, negative in [('is_derived', 'DERIVED', 'ORIGINAL'), ('is_secondary', 'SECONDARY', 'PRIMARY')]:
        attrs[key] = resolve([(t == positive, 'known', evidence(row, 'ImageType'))
                              for t in tokens if t in {positive, negative}], key)
    attrs['orientation'] = fact(rule_id='geometry-missing')
    vals = [number(v) for v in parse_multivalue(row.get('ImageOrientationPatient'))]
    if len(vals) == 6 and all(v is not None for v in vals):
        import numpy as np
        a, b = np.array(vals[:3]), np.array(vals[3:])
        if abs(np.linalg.norm(a) - 1) < .01 and abs(np.linalg.norm(b) - 1) < .01 and abs(a @ b) < .01:
            normal = np.cross(a, b)
            angle = float(np.degrees(np.arccos(np.clip(np.max(np.abs(normal)), 0, 1))))
            attrs['orientation'] = fact(dict(direction=vals, normal=normal.tolist(), oblique_angle_degrees=angle),
                                        evidence=evidence(row, 'ImageOrientationPatient'), rule_id='iop-geometry')
    return attrs


def v2_record(row, cfg):
    attrs = build_semantics(row, cfg)
    # A mask denotes usable metadata supervision, not empirically calibrated correctness.
    mask = {k: v['status'] == 'known' for k, v in attrs.items()}
    scope = text_value(row.get('mr_metadata_scope')) or 'series_summary_unverified'
    # No representative-frame label may supervise an entire heterogeneous series.
    heterogeneous = str(row.get('mr_heterogeneous', '')).lower() == 'true'
    if heterogeneous or text_value(row.get('mr_index_status')) in {'incomplete', 'failed'}:
        mask = {k: False for k in attrs}
    return dict(schema_version=RULE_VERSION, metadata_scope=scope,
                config_digest=hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest(),
                attributes=attrs, supervision_mask=mask)
