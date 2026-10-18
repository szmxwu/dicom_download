# -*- coding: utf-8 -*-
"""
方向一致性校验与镜像校正测试（合成 DICOM 端到端）

测试目标：
1. 用 pydicom 构造合成斜位 DICOM 序列：正常处方 vs 镜像处方（IOP 行方向 x 分量反号）
2. 分别跑 dcm2niix 路径与纯 Python 路径的完整转换流程
3. 断言：
   - 正常套件：不误伤（内容不被翻转；python 路径仅修正仿射）
   - 镜像套件：det(T)<0 被检测并校正，校正后体素方向与正常套件一致
     （用偏心亮球 + 梯度的不对称标记验证左右）
   - 非置换矩阵（重采样仿射）：不自动改，记 QC 告警
4. 单测覆盖：置换矩阵判定、真值仿射 handedness、探针布局识别

用法：
    cd /home/ubuntu/work/python/dicom_download
    python test/test_orientation_fix.py
    或 pytest test/test_orientation_fix.py
"""

import os
import shutil
import sys
import tempfile

import numpy as np
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage, generate_uid
import nibabel as nib

# 确保项目根目录在路径中
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.core.convert import (
    convert_with_dcm2niix,
    convert_with_python_libs,
    verify_and_fix_orientation,
    build_dicom_truth_affine,
    _analyze_signed_permutation,
    _probe_content_layout,
    ORIENTATION_CHECK_OK,
    ORIENTATION_CHECK_MIRROR_CORRECTED,
    ORIENTATION_CHECK_NOT_VERIFIABLE,
)

# ------------------------------------------------------------------------------
# 合成 DICOM 序列生成
# ------------------------------------------------------------------------------

ROWS, COLS, NSL = 32, 24, 5
RS, CS, STEP = 2.0, 2.0, 3.0
# 亮球位置须同时落在正常/镜像两个网格覆盖范围内（已数值验证两网格均有采样点在球内）
BALL_CENTER = np.array([5.0, -2.0, -8.0])   # LPS：x=+5 即患者左侧
BALL_R = 5.0
OFF_BALL = np.array([-5.0, -2.0, -8.0])     # 镜像对侧参照点（球外）

# 正常斜冠状处方：X=IOP[0:3]（沿行方向=c 递增方向），Y=IOP[3:6]（r 递增方向）
X_N = np.array([0.940, 0.0, 0.342])
Y = np.array([0.0, 1.0, 0.0])
MIRROR = np.diag([-1.0, 1.0, 1.0])          # 矢状面镜像 x -> -x
X_M = MIRROR @ X_N                          # 镜像处方：行方向 x 分量反号
IPP0_N = np.array([-20.0, -30.0, -20.0])
_NORMAL_N = np.cross(X_N, Y)
STEP_N = STEP * _NORMAL_N / np.linalg.norm(_NORMAL_N)
IPP0_M = MIRROR @ IPP0_N                    # 镜像网格覆盖同一解剖区域
STEP_M = MIRROR @ STEP_N                    # 注意 cross(X_M,Y) = -STEP_M 方向（镜像签名）


def anatomy(p):
    """世界空间(LPS)解剖函数：偏心亮球 + y 向梯度，保证左右/前后不对称。"""
    v = 2.0 * p[1]
    if np.linalg.norm(p - BALL_CENTER) <= BALL_R:
        v += 1000.0
    return v


def make_series(out_dir, X, ipp0, step, tag):
    """按给定处方生成 DICOM 序列；像素 = 解剖函数在该处方网格上的采样。"""
    os.makedirs(out_dir, exist_ok=True)
    suid = generate_uid()
    study_uid = generate_uid()
    for s in range(NSL):
        ipp = ipp0 + s * step
        px = np.zeros((ROWS, COLS), np.uint16)
        for r in range(ROWS):
            for c in range(COLS):
                pos = ipp + c * CS * X + r * RS * Y
                px[r, c] = int(max(0, anatomy(pos) + s * 50))  # 层偏移打破层对称
        meta = FileMetaDataset()
        meta.MediaStorageSOPClassUID = MRImageStorage
        meta.MediaStorageSOPInstanceUID = generate_uid()
        meta.TransferSyntaxUID = ExplicitVRLittleEndian
        ds = Dataset()
        ds.file_meta = meta
        ds.SOPClassUID = MRImageStorage
        ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
        ds.Modality = 'MR'
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = suid
        ds.SeriesDescription = tag
        ds.SeriesNumber = 701
        ds.InstanceNumber = s + 1
        ds.PatientPosition = 'HFS'
        ds.ImageOrientationPatient = [float(x) for x in list(X) + list(Y)]
        ds.ImagePositionPatient = [float(x) for x in ipp]
        ds.PixelSpacing = [RS, CS]
        ds.SliceThickness = STEP
        ds.Rows, ds.Columns = ROWS, COLS
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = 'MONOCHROME2'
        ds.PixelData = px.tobytes()
        pydicom.dcmwrite(os.path.join(out_dir, f'slice_{s:03d}.dcm'), ds,
                         write_like_original=False)


class MockClient:
    """convert 模块所需的最小 client 桩"""

    def _is_dicom_file(self, path):
        return path.endswith('.dcm')

    def _sanitize_folder_name(self, name):
        return name.replace(' ', '_')

    def _ensure_metadata_cache(self, *args, **kwargs):
        pass

    def _record_orientation_check(self, *args, **kwargs):
        pass

    def _load_sample_tags_from_cache(self, series_dir):
        return None


try:
    import pytest

    @pytest.fixture()
    def workdir():
        d = tempfile.mkdtemp(prefix='orientation_fix_test_')
        yield d
        shutil.rmtree(d, ignore_errors=True)
except ImportError:
    pass


def ball_world_lps(nii_path_or_img):
    """输出 NIfTI 中亮球体素的世界坐标（LPS）。"""
    img = nii_path_or_img if hasattr(nii_path_or_img, 'affine') else nib.load(nii_path_or_img)
    data = np.asanyarray(img.dataobj)
    idx = np.unravel_index(np.argmax(data), data.shape)
    world_ras = img.affine @ np.array([*idx[:3], 1.0])
    return world_ras[:3] * np.array([-1.0, -1.0, 1.0])


def value_at_world(nii_path_or_img, world_lps):
    """取指定世界坐标(LPS)最近体素的值。"""
    img = nii_path_or_img if hasattr(nii_path_or_img, 'affine') else nib.load(nii_path_or_img)
    world_ras = np.array(world_lps, dtype=float) * np.array([-1.0, -1.0, 1.0])
    idx = np.linalg.inv(img.affine) @ np.array([*world_ras, 1.0])
    idx = np.round(idx[:3]).astype(int)
    data = np.asanyarray(img.dataobj)
    if (idx < 0).any() or any(idx[a] >= data.shape[a] for a in range(3)):
        return 0.0
    return float(data[tuple(idx)])


# ------------------------------------------------------------------------------
# 单元测试
# ------------------------------------------------------------------------------

def test_signed_permutation_detection():
    """置换矩阵判定：det<0 反射、正常旋转、非置换（重采样）三条路径"""
    # 正常旋转/置换（det=+1）
    T_rot = np.array([[0., 1., 0., 0.], [-1., 0., 0., 31.], [0., 0., 1., 0.], [0., 0., 0., 1.]])
    perm = _analyze_signed_permutation(T_rot)
    assert perm is not None, 'rotation permutation should be accepted'
    assert round(np.linalg.det(perm[0])) == 1

    # 含反射（det=-1）
    T_ref = np.array([[0., 1., 0., 0.], [-1., 0., 0., 31.], [0., 0., -1., 4.], [0., 0., 0., 1.]])
    perm = _analyze_signed_permutation(T_ref)
    assert perm is not None, 'reflection permutation should be accepted'
    assert round(np.linalg.det(perm[0])) == -1

    # 非置换（重采样/插值：旋转 30°）
    ang = np.radians(30)
    T_res = np.array([[np.cos(ang), -np.sin(ang), 0., 0.],
                      [np.sin(ang), np.cos(ang), 0., 0.],
                      [0., 0., 1., 0.], [0., 0., 0., 1.]])
    assert _analyze_signed_permutation(T_res) is None, 'resampled transform should be rejected'
    print('   ✅ test_signed_permutation_detection')


def test_truth_affine_handedness(workdir):
    """真值仿射：正常处方右手性(det<0 in RAS)，镜像处方左手性(det>0)"""
    normal_dir = os.path.join(workdir, 'truth_normal')
    mirrored_dir = os.path.join(workdir, 'truth_mirrored')
    make_series(normal_dir, X_N, IPP0_N, STEP_N, 'truth_normal')
    make_series(mirrored_dir, X_M, IPP0_M, STEP_M, 'truth_mirrored')

    dets = {}
    for tag, d in [('normal', normal_dir), ('mirrored', mirrored_dir)]:
        datasets = [pydicom.dcmread(os.path.join(d, f))
                    for f in sorted(os.listdir(d)) if f.endswith('.dcm')]
        truth = build_dicom_truth_affine(datasets)
        assert truth is not None
        affine_d, _ = truth
        dets[tag] = np.linalg.det(affine_d[:3, :3])
    assert dets['normal'] < 0, f"normal prescription should be right-handed grid, got {dets['normal']}"
    assert dets['mirrored'] > 0, f"mirrored prescription should be left-handed grid, got {dets['mirrored']}"
    print('   ✅ test_truth_affine_handedness')


def test_probe_layout_recovery(workdir):
    """探针布局识别：对已知重排的数据能恢复其布局矩阵"""
    series_dir = os.path.join(workdir, 'probe_normal')
    make_series(series_dir, X_N, IPP0_N, STEP_N, 'probe_normal')
    datasets = [pydicom.dcmread(os.path.join(series_dir, f))
                for f in sorted(os.listdir(series_dir)) if f.endswith('.dcm')]
    truth = build_dicom_truth_affine(datasets)
    _, sorted_datasets = truth
    V = np.stack([ds.pixel_array.astype(np.float32) for ds in sorted_datasets], axis=2)

    # 模拟 dcm2niix 正常布局：transpose(1,0,2) + flip axis1
    D = np.flip(np.transpose(V, (1, 0, 2)), axis=1)
    probe = _probe_content_layout(D, sorted_datasets)
    assert probe is not None, 'probe should recover the known layout'
    layout, corr = probe
    assert corr >= 0.995
    # 布局矩阵应满足：D[L(p)] == V[p]
    p = np.array([5, 7, 2, 1.0])
    q = (layout @ p).astype(int)
    assert abs(float(D[q[0], q[1], q[2]]) - float(V[5, 7, 2])) < 1e-3
    print('   ✅ test_probe_layout_recovery')


def test_non_permutation_warning(workdir):
    """非置换矩阵（重采样仿射）：不自动改，记 QC 告警"""
    series_dir = os.path.join(workdir, 'warn_normal')
    make_series(series_dir, X_N, IPP0_N, STEP_N, 'warn_normal')
    datasets = [pydicom.dcmread(os.path.join(series_dir, f))
                for f in sorted(os.listdir(series_dir)) if f.endswith('.dcm')]
    data = np.random.rand(24, 32, 5).astype(np.float32)
    # 构造一个相对真值仿射为 30° 旋转的仿射
    truth = build_dicom_truth_affine(datasets)
    affine_d, _ = truth
    ang = np.radians(30)
    R = np.array([[np.cos(ang), -np.sin(ang), 0., 0.],
                  [np.sin(ang), np.cos(ang), 0., 0.],
                  [0., 0., 1., 0.], [0., 0., 0., 1.]])
    bad_affine = affine_d @ np.linalg.inv(R)
    img = nib.Nifti1Image(data, bad_affine)
    _, result = verify_and_fix_orientation(img, datasets, 'warn_normal')
    assert result['action'] == ORIENTATION_CHECK_NOT_VERIFIABLE, result
    assert not result['fixes']
    print('   ✅ test_non_permutation_warning')


# ------------------------------------------------------------------------------
# 端到端测试
# ------------------------------------------------------------------------------

def _convert_both_paths(workdir, tag, X, ipp0, step):
    """生成合成序列并分别跑 dcm2niix / python 路径，返回结果字典。"""
    src_dir = os.path.join(workdir, f'{tag}_src')
    make_series(src_dir, X, ipp0, step, tag)
    outcomes = {}
    for method in ['dcm2niix', 'python']:
        run_dir = os.path.join(workdir, f'{tag}_{method}')
        shutil.rmtree(run_dir, ignore_errors=True)
        shutil.copytree(src_dir, run_dir)
        client = MockClient()
        if method == 'dcm2niix':
            res = convert_with_dcm2niix(client, run_dir, tag, modality='MR')
        else:
            res = convert_with_python_libs(client, run_dir, tag, modality='MR')
        assert res and res.get('success'), f'{tag}/{method} conversion failed: {res}'
        outcomes[method] = {
            'result': res,
            'nii': os.path.join(run_dir, res['output_file']),
            'check': res.get('orientation_check') or {}
        }
    return outcomes


def test_normal_series_not_harmed(workdir):
    """正常斜位套件：内容不被翻转（dcm2niix 不动；python 仅修正仿射），世界位置正确"""
    outcomes = _convert_both_paths(workdir, 'e2e_normal', X_N, IPP0_N, STEP_N)

    dcm = outcomes['dcm2niix']
    assert dcm['check'].get('action') == ORIENTATION_CHECK_OK, dcm['check']
    assert not dcm['check'].get('fixes'), 'dcm2niix faithful output must not be modified'
    w = ball_world_lps(dcm['nii'])
    assert abs(w[0] - BALL_CENTER[0]) < 5 and abs(w[1] - BALL_CENTER[1]) < 5, \
        f'dcm2niix normal ball world mismatch: {w}'

    py = outcomes['python']
    # Python now builds the correct row/column geometry directly.
    assert py['check']['action']==ORIENTATION_CHECK_OK, py['check']
    assert not py['check'].get('fixes'), py['check']
    assert 'mirror_relayout' not in py['check'].get('fixes', []), \
        'normal series must not be content-flipped'
    w = ball_world_lps(py['nii'])
    assert w[0] > 2, f'python normal after fix: ball should be on patient-left (x>0), got {w}'
    assert value_at_world(py['nii'], BALL_CENTER) > 500
    assert value_at_world(py['nii'], OFF_BALL) < 500
    print('   ✅ test_normal_series_not_harmed (dcm2niix ok / python affine fixed)')


def test_mirrored_series_corrected(workdir):
    """镜像斜位套件：det(T)<0 被检测并校正；校正后与正常套件体素方向一致"""
    outcomes = _convert_both_paths(workdir, 'e2e_mirrored', X_M, IPP0_M, STEP_M)

    for method in ['dcm2niix', 'python']:
        out = outcomes[method]
        check = out['check']
        assert check.get('action') == ORIENTATION_CHECK_MIRROR_CORRECTED, \
            f'{method}: expected mirror correction, got {check}'
        assert 'mirror_relayout' in check.get('fixes', []), f'{method}: {check}'
        assert check.get('flip_axis') is not None, f'{method}: {check}'
    # dcm2niix 忠实输出的 det(T)<0 是镜像处方签名（python 路径 T 因仿射 bug 不适用）
    assert outcomes['dcm2niix']['check'].get('det_t') is not None \
        and outcomes['dcm2niix']['check']['det_t'] < 0, outcomes['dcm2niix']['check']

    for method in ['dcm2niix', 'python']:
        out = outcomes[method]
        check = out['check']

        # 校正后：世界几何保持正确（亮球仍在患者左侧 x>0）
        w = ball_world_lps(out['nii'])
        assert w[0] > 2, f'{method} mirrored after fix: ball should be on patient-left, got {w}'
        assert value_at_world(out['nii'], BALL_CENTER) > 500, \
            f'{method}: ball should be bright at truth center'
        assert value_at_world(out['nii'], OFF_BALL) < 500, \
            f'{method}: mirrored side should be dark'

        # 校正后：内容与仿射必须自洽（探针实测布局 == 仿射隐含布局）
        img = nib.load(out['nii'])
        data = np.asanyarray(img.dataobj)
        # DICOM 已被转换流程删除，从 src 目录重建真值仿射与探针
        src_dir = os.path.join(workdir, 'e2e_mirrored_src')
        datasets = [pydicom.dcmread(os.path.join(src_dir, f))
                    for f in sorted(os.listdir(src_dir)) if f.endswith('.dcm')]
        truth = build_dicom_truth_affine(datasets)
        affine_d, sorted_datasets = truth
        T = np.linalg.inv(img.affine) @ affine_d
        probe = _probe_content_layout(data, sorted_datasets)
        assert probe is not None, f'{method}: probe should recover layout after fix'
        layout, corr = probe
        assert np.allclose(layout, T, atol=0.02, rtol=0), \
            f'{method}: content-affine inconsistent after fix\nlayout=\n{layout}\nT=\n{T}'
    print('   ✅ test_mirrored_series_corrected (both paths un-mirrored)')


def test_mirrored_matches_normal_appearance(workdir):
    """校正后的镜像套件与正常套件在规范空间中的体素外观一致（左右不颠倒）"""
    normal = _convert_both_paths(workdir, 'cmp_normal', X_N, IPP0_N, STEP_N)
    mirrored = _convert_both_paths(workdir, 'cmp_mirrored', X_M, IPP0_M, STEP_M)

    for method in ['dcm2niix', 'python']:
        cn = nib.as_closest_canonical(nib.load(normal[method]['nii']))
        cm = nib.as_closest_canonical(nib.load(mirrored[method]['nii']))
        dn = np.asanyarray(cn.dataobj)
        dm = np.asanyarray(cm.dataobj)
        idx_n = np.unravel_index(np.argmax(dn), dn.shape)
        idx_m = np.unravel_index(np.argmax(dm), dm.shape)
        # 亮球在规范体素空间的第 0 轴（x 承载轴）应位于中心的同侧
        side_n = np.sign(idx_n[0] - (dn.shape[0] - 1) / 2.0)
        side_m = np.sign(idx_m[0] - (dm.shape[0] - 1) / 2.0)
        assert side_n == side_m, (
            f'{method}: mirrored-corrected appearance differs from normal '
            f'(normal argmax i={idx_n[0]}/{dn.shape[0]}, mirrored i={idx_m[0]}/{dm.shape[0]})'
        )
    print('   ✅ test_mirrored_matches_normal_appearance')


# ------------------------------------------------------------------------------
# 脚本入口
# ------------------------------------------------------------------------------

def main():
    workdir = tempfile.mkdtemp(prefix='orientation_fix_test_')
    print(f'Workdir: {workdir}')
    try:
        test_signed_permutation_detection()
        test_truth_affine_handedness(workdir)
        test_probe_layout_recovery(workdir)
        test_non_permutation_warning(workdir)
        test_normal_series_not_harmed(workdir)
        test_mirrored_series_corrected(workdir)
        test_mirrored_matches_normal_appearance(workdir)
        print('\n✅ All orientation fix tests PASSED')
        return 0
    except AssertionError as e:
        print(f'\n❌ TEST FAILED: {e}')
        import traceback
        traceback.print_exc()
        return 1
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
