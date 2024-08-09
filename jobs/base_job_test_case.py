import unittest
from jobs.job import Job


class BaseJobTestCase(unittest.TestCase, Job):
    def setUp(self):
        Job.__init__(self)
        super().setUp()

    def launch(self):
        pass


def run_test_suit(test_suit_class):
    # please don't change the logic of test result checks here
    # it's intentionally done in this way to comply with jobs run result checks
    loader = unittest.TestLoader()
    tests = loader.loadTestsFromTestCase(test_suit_class)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    if not result.wasSuccessful():
        raise RuntimeError(
            "One or multiple tests failed. "
            "Please check job logs for additional information."
        )
