import unittest
from multiprocessing import Manager

from ParallelFilesUploaderEmulation import Uploader
from ReportForUploader import Report


class TestFilesUploader(unittest.TestCase):
    """ Test using normal stop """

    def setUp(self):
        files_list = ['file' + str(c) for c in range(20)]

        manager = Manager()
        q = manager.Queue()

        uploader = Uploader(files_list, 4, q)
        uploader.error_emulation = True
        uploader.worker_time = 0.1

        uploader.start()
        uploader.join()

        self.files_list = files_list
        self.uploader = uploader

    def testInstance(self):
        self.assertFalse(self.uploader.is_active())

        self.assertEqual(self.files_list, self.uploader.files_to_upload)
        self.assertIs(len(self.uploader._result), len(self.files_list))

        self.assertTrue(all(file in self.files_list for file in self.uploader.uploaded_files))
        self.assertTrue(all(file in self.files_list for file in self.uploader.error_files))
        self.assertTrue(all(file in self.files_list for file in self.uploader.aborted_files))

        if self.uploader.uploaded_files:
            self.assertTrue(self.uploader.uploaded_count)

        if self.uploader.error_files:
            self.assertTrue(self.uploader.errors_count)

        if self.uploader.aborted_files:
            self.assertTrue(self.uploader.aborted_count)

    def testResult(self):
        result = self.uploader.result
        uploaded_count = self.uploader.uploaded_count
        errors_count = self.uploader.errors_count
        aborted_count = self.uploader.aborted_count

        self.assertFalse(self.uploader.is_active())
        self.assertFalse(self.uploader.terminated)

        self.assertIs(self.uploader.processed_count, len(self.files_list))
        self.assertIs(self.uploader.aborted_count, 0)

        self.assertIsInstance(result, str)

        self.assertTrue(all(file in result for file in self.uploader.error_files))
        self.assertTrue(all(file in result for file in self.uploader.uploaded_files))

        if self.uploader.errors_count:
            self.assertIn('WARNING: ERRORS!', result)
        else:
            self.assertIn('Successfully Completed', result)

        self.assertNotIn('WARNING: ABORTED!', result)

        self.assertIs(uploaded_count + errors_count + aborted_count, len(self.files_list))
        self.assertIs(len(self.files_list) - errors_count - aborted_count, uploaded_count)

    def testUpload(self):
        upload = self.uploader._upload
        queue = self.uploader.reports_q

        while not queue.empty():
            queue.get()

        from multiprocessing import Value

        total_count = self.uploader.total_count
        processed_count = Value('i', 0)
        errors_count = Value('i', 0)
        start_trigger = Value('i', False)

        self.uploader._initializer(total_count, processed_count, errors_count, start_trigger)

        for file in self.uploader.files_to_upload:
            result = upload(file)
            report = queue.get()
            self.assertEqual(result.filename, report.filename)
            self.assertEqual(result.status, report.status)

    def testReport(self):
        queue = self.uploader.reports_q
        report = queue.get()

        self.assertEqual(queue.qsize(), self.uploader.total_count - 1)
        self.assertIsInstance(report, Report)

        self.assertIn(report.filename, self.uploader.files_to_upload)
        self.assertIs(report.total_count, self.uploader.total_count)
        self.assertIn(report.status, 'done, error, aborted')

        if report.status == 'done':
            self.assertTrue(report.processed_count)
        elif report.status == 'error':
            self.assertTrue(report.processed_count)
            self.assertTrue(report.errors_count)
            self.assertTrue(report.error_message)
        elif report.status == 'aborted':
            self.assertTrue(report.aborted_count)

        self.assertIsInstance(report.progress, str)
        self.assertIn(f'of {self.uploader.total_count} files', report.progress)

        while not queue.empty():
            report = queue.get()
            self.assertIsInstance(report, Report)
            self.assertIn(report.filename, self.uploader.files_to_upload)

        self.assertFalse(queue.qsize())


class TestFilesUploaderForceStop(TestFilesUploader):
    """ Test using force stop """

    def setUp(self):
        files_list = ['file' + str(c) for c in range(20)]

        manager = Manager()
        q = manager.Queue()

        uploader = Uploader(files_list, 4, q)
        uploader.error_emulation = True
        uploader.worker_time = 0.1

        uploader.start()

        while uploader.processed_count < 5:
            q.put(q.get())

        uploader.stop()

        self.files_list = files_list
        self.uploader = uploader

    def testResult(self):
        result = self.uploader.result
        uploaded_count = self.uploader.uploaded_count
        errors_count = self.uploader.errors_count
        aborted_count = self.uploader.aborted_count

        self.assertFalse(self.uploader.is_active())
        self.assertTrue(self.uploader.terminated)

        self.assertTrue(self.uploader.aborted_count)

        self.assertIsInstance(result, str)

        self.assertTrue(all(file in result for file in self.uploader.error_files))
        self.assertTrue(all(file in result for file in self.uploader.uploaded_files))
        self.assertTrue(all(file in result for file in self.uploader.aborted_files))

        self.assertIn('WARNING: ABORTED!', result)

        self.assertIs(uploaded_count + errors_count + aborted_count, len(self.files_list))
        self.assertIs(len(self.files_list) - errors_count - aborted_count, uploaded_count)
