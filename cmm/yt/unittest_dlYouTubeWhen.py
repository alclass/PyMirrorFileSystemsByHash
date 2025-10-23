import dlYouTubeWhenThereAreDubbed as dl_vi_w_lang
import unittest


class OSEntryTestCase(unittest.TestCase):

  def setUp(self):
    self.testdirpath = '/dir1/subdir/thislevel'
    self.ose = dl_vi_w_lang.OSEntry(
      workdir_abspath=self.testdirpath,
      basefilename='this video [GypUQgXZmlQ].mp4',
      videoonly_or_audio_code=160
    )

  def test_1(self):
    # dot_ext
    ret_dot_ext = self.ose.dot_ext
    exp_dot_ext = '.mp4'
    self.assertEqual(exp_dot_ext, ret_dot_ext)
    # fsufix
    ret_fsufix = self.ose.fsufix
    exp_fsufix = 'f160'
    self.assertEqual(exp_fsufix, ret_fsufix)
    # name
    ret_name = self.ose.name
    exp_name = 'this video [GypUQgXZmlQ]'
    self.assertEqual(exp_name, ret_name)
    # fn_as_name_ext
    ret_fn = self.ose.fn_as_name_ext
    exp_fn = 'this video [GypUQgXZmlQ].mp4'
    self.assertEqual(exp_fn, ret_fn)
    # fp_for_fn_as_name_ext
    ret_fp = self.ose.fp_for_fn_as_name_ext
    exp_fp = f'{self.testdirpath}/this video [GypUQgXZmlQ].mp4'
    self.assertEqual(exp_fp, ret_fp)
    # fn_as_name_fsufix_ext
    ret_fn = self.ose.fn_as_name_fsufix_ext
    exp_fn = 'this video [GypUQgXZmlQ].f160.mp4'
    self.assertEqual(exp_fn, ret_fn)
    # fp_for_fn_as_name_fsufix_ext
    ret_fp = self.ose.fp_for_fn_as_name_fsufix_ext
    exp_fp = f'{self.testdirpath}/this video [GypUQgXZmlQ].f160.mp4'
    self.assertEqual(exp_fp, ret_fp)
    # fn_as_name_fsufix_extbksufix[N]
    ret_fn = self.ose.get_fn_as_name_fsufix_ext_bksufix(3)
    exp_fn = 'this video [GypUQgXZmlQ].f160.mp4.bk3'
    self.assertEqual(exp_fn, ret_fn)
    # fp_for_fn_as_name_fsufix_extbksufix[N]
    ret_fp = self.ose.get_fp_for_fn_as_name_fsufix_ext_bksufix(3)
    exp_fp = f'{self.testdirpath}/this video [GypUQgXZmlQ].f160.mp4.bk3'
    self.assertEqual(exp_fn, ret_fn)
    # ytid
    ret_ytid = self.ose.ytid
    exp_ytid = 'GypUQgXZmlQ'
    self.assertEqual(exp_ytid, ret_ytid)
