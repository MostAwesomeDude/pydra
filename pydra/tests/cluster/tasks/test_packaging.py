import os.path
import shutil
import tempfile
import unittest

from pydra.cluster.tasks.packaging import compute_sha1_hash

class PackagingTest(unittest.TestCase):

    def test_compute_sha1_hash(self):
        """
        Test compute_sha1_hash's invariance.
        """

        first_string = "Jackdaws love my big sphinx of quartz."
        second_string = "Watch \"Jeopardy!\", Alex Trebek's fun TV quiz game."

        tempdir = tempfile.mkdtemp()
        with open(os.path.join(tempdir, "one.txt"), "w") as f:
            f.write(first_string)
        with open(os.path.join(tempdir, "two.txt"), "w") as f:
            f.write(second_string)
        first = compute_sha1_hash(tempdir)
        shutil.rmtree(tempdir)

        # Again, but in the other order.
        tempdir = tempfile.mkdtemp()
        with open(os.path.join(tempdir, "two.txt"), "w") as f:
            f.write(second_string)
        with open(os.path.join(tempdir, "one.txt"), "w") as f:
            f.write(first_string)
        second = compute_sha1_hash(tempdir)
        shutil.rmtree(tempdir)

        self.assertEqual(first, second)
