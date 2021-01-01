import os
import secrets
import shutil
import tempfile
import time
import unittest

from datastore import DataStore


class TestDataStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tempdir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.tempdir, "data.db")
        cls.dstore = DataStore(cls.db_path)

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.dstore
        shutil.rmtree(cls.tempdir)

    def exception_tester(self, key, value, exc, msg):
        """ Tests exceptions for both TTL and with-out TTL keys"""
        for val in [value, (value, 3)]:
            with self.assertRaises(exc) as e:
                self.dstore[key] = val
            self.assertEqual(e.exception.args[0], msg)

    def test_simple_crd(self):
        key = "alice"
        value = {"age": 24, "gender": "male"}
        self.dstore[key] = value
        self.assertEqual(self.dstore[key], value)
        del self.dstore[key]
        self.assertIsNone(self.dstore[key])

    def test_ttl_crd(self):
        value = {"age": 24, "gender": "male"}
        self.dstore["alice"] = value, 1  # first is value and second is ttl
        self.assertEqual(self.dstore["alice"], value)
        time.sleep(1)
        with self.assertRaises(TimeoutError) as e:
            data = self.dstore["alice"]
        self.assertEqual(e.exception.args[0], "Key has been expired!")

    def test_duplicate_key_error(self):
        value = {"age": 24, "gender": "male"}
        self.dstore["alice"] = value
        self.exception_tester(
                key="alice",
                value={"age": 26, "gender": "male"},
                exc=KeyError,
                msg="Key already exists!"
        )
        del self.dstore["alice"]

    def test_memory_constraints(self):
        key = secrets.token_urlsafe(33)
        self.exception_tester(
                key=key,
                value={"age": 26, "gender": "male"},
                exc=KeyError,
                msg="length of the key has to be less than 32 characters!"
        )
        del self.dstore[key]

        self.exception_tester(
                key="alice",
                value={"age": 26, "gender": "male", "garbage": "g" * 16384},
                exc=ValueError,
                msg="JSON payload exceeds maximum memory limit (16KB)!"
        )
        del self.dstore["alice"]

    def test_non_json_value(self):
        self.exception_tester(
                key="alice",
                value="bob",
                exc=ValueError,
                msg="value has to be a dictionary!"
        )


if __name__ == '__main__':
    unittest.main()
