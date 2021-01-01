# key_value_db

![Test Runner](https://github.com/Niraj-Kamdar/key_value_db/workflows/Test%20Runner/badge.svg)
[![codecov](https://codecov.io/gh/Niraj-Kamdar/key_value_db/branch/main/graph/badge.svg?token=50Qy9RGn6z)](https://codecov.io/gh/Niraj-Kamdar/key_value_db)
[![CodeFactor](https://www.codefactor.io/repository/github/niraj-kamdar/key_value_db/badge)](https://www.codefactor.io/repository/github/niraj-kamdar/key_value_db)

A file-based key-value data store that supports basic CRD operations.

## Installation

You can install the package with following command

```python
pip install git+https://github.com/Niraj-Kamdar/key_value_db.git#egg=key_value_db
```

## Usage

After installation, you should be able to import datastore module as below

```python
from datastore import DataStore
```

You can specify path to save data in the constructor of `DataStore` class.

```python
dstore = DataStore("data.db")
```

After Initialisation, you can use the `dstore` object just like you would use a
normal dictionary but instead of in-memory it will save data in the file.

```python
dstore["alice"] = {"age": 56, "name": "alice"}
```

You can also specify time-to-live as following

```python
dstore["alice"] = {"age": 56, "name": "alice"}, 5
# here second value is ttl and after 5 second you won't be able to access record of alice
```

You can also delete any key simply by `del` keyword.

```python
del dstore["alice"]
```

> Note: length of the key has to be less than 33 characters and unique. Value
> has to be dictionary (JSON) with less than 16KB of data.
