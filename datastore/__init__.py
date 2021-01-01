import json
import secrets
import sqlite3
import sys
from time import time
from typing import Union


def _row_factory(cursor, row):
    if row[2] and row[2] < time():
        raise TimeoutError
    return {row[0]: json.loads(row[1])} if row else None


class DataStore:
    """ DataStore is a file-based Key Value datastore.

    Datastore is meant to be used as a local storage for a single device.
    It uses sqlite3 database internally. So, it is a thread-safe,
    highly concurrent and efficient.

    Args:
        filename (str): filename of the datastore file. By default if will be random 8 character name.

    Attributes:
        name (str): name of the file where data has been stored.
        db (object): sqlite3 database connection object
    """
    def __init__(self, filename=f"{secrets.token_urlsafe(8)}.db"):
        self.name = filename
        self.db = sqlite3.connect(self.name)
        self.db.row_factory = _row_factory
        self.cursor = self.db.cursor()
        self.cursor.execute(
                """
                CREATE TABLE data(
                    key VARCHAR(32) PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL,
                    ttl INTEGER
                );
                """
        )

    def __del__(self):
        self.db.commit()
        self.db.close()

    def __getitem__(self, key: str) -> dict:
        """ Get the value associated with the given key.

        It will return a dictionary(deserialized JSON) associated with the given key
        if the key exists in the datastore otherwise returns None. If ttl specified
        for the key and current time exceeds the ttl it will throw a TimeoutError.

        Returns:
            value (dict): value associated with the given key

        Raises:
            TimeoutError: when ttl exceeds current time.
        """
        self.cursor.execute("SELECT * FROM data WHERE key=?", (key,))
        try:
            data = self.cursor.fetchone()
        except TimeoutError:
            self.cursor.execute("DELETE FROM data WHERE key=?", (key,))
            self.db.commit()
            raise TimeoutError("Key has been expired!")
        return data[key] if data else None

    def __setitem__(self, key: str, value: Union[dict, tuple]):
        """ Creates a key-value entry in the datastore.

        It will create a new entry in datastore for given key and value if not exists
        otherwise it will throw KeyError.

        Raises:
            KeyError: when key length exceeds 32 characters or duplicate key
            ValueError: when value exceeds 16KB or TTL is less than 1 seconds.
        """
        if len(key) > 32:
            raise KeyError("length of the key has to be less than 32 characters!")
        if isinstance(value, tuple):
            if value[1] <= 0:
                raise ValueError("TTL can't be less than 1!")
            value, ttl = value
            if not isinstance(value, dict):
                raise ValueError("value has to be a dictionary!")
            value = json.dumps(value)
            if sys.getsizeof(value) > 16384:
                raise ValueError("JSON payload exceeds maximum memory limit (16KB)!")
            try:
                self.cursor.execute(
                    "INSERT INTO data(key, value, ttl) VALUES (?, ?, ?)",
                    (key, value, ttl + time())
                )
            except sqlite3.IntegrityError:
                raise KeyError("Key already exists!")
        else:
            if not isinstance(value, dict):
                raise ValueError("value has to be a dictionary!")
            value = json.dumps(value)
            if sys.getsizeof(value) > 16384:
                raise ValueError("JSON payload exceeds maximum memory limit (16KB)!")
            try:
                self.cursor.execute(
                    "INSERT INTO data(key, value) VALUES (?, ?)",
                    (key, value)
                )
            except sqlite3.IntegrityError:
                raise KeyError("Key already exists!")
        self.db.commit()

    def __delitem__(self, key: str):
        """ Deletes the entry associated with the given key from the database."""
        self.cursor.execute("DELETE FROM data WHERE key=?", (key,))
        self.db.commit()

