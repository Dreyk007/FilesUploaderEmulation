import unittest
from multiprocessing import Manager

from ParallelFilesUploaderEmulation import Uploader, Report


class TestFilesUploader(unittest.TestCase):
    """ Test using normal stop """

    def setUp(self):
        self.files_list = ['file' + str(c) for c in range(20)]

        manager = Manager()
        self.q = manager.Queue()

        self.uploader = Uploader(self.files_list, 4, self.q)
        self.uploader.error_emulation = True
        self.uploader.worker_time = 0.3
        self.uploader.start()
        self.report = self.q.get(timeout=5)

    def testStart(self):
        self.assertTrue(self.uploader.is_active())

    def testReport(self):
        self.assertIsInstance(self.report, Report)
        self.assertIn(self.report.filename, self.files_list)
        self.assertIs(self.report.total_count, len(self.files_list))
        self.assertIn(self.report.status, 'done, error, aborted')

    def testStopAndResult(self):
        self.uploader.join()
        self.assertFalse(self.uploader.is_active())
        self.assertEqual(self.q.qsize(), len(self.files_list) - 1)

        uploaded, error, aborted = self.uploader._calc_result()
        self.assertIs(len(uploaded) + len(error) + len(aborted), len(self.files_list))
        self.assertIs(len(self.files_list) - len(error) - len(aborted), len(uploaded))


class TestFilesUploaderForceStop(TestFilesUploader):
    """ Test using force stop """

    def testStopAndResult(self):
        self.uploader.stop()
        self.assertFalse(self.uploader.is_active())
        self.assertTrue(self.uploader.terminated)

        uploaded, error, aborted = self.uploader._calc_result()
        self.assertEqual(self.q.qsize(), len(uploaded) + len(error) - 1)
        self.assertNotEqual(len(aborted), 0)

        self.assertIs(len(uploaded) + len(error) + len(aborted), len(self.files_list))
        self.assertIs(len(self.files_list) - len(error) - len(aborted), len(uploaded))
