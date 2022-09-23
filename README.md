[![.github/workflows/test.yaml](https://github.com/google/python-spanner-orm/actions/workflows/test.yaml/badge.svg)](https://github.com/google/python-spanner-orm/actions/workflows/test.yaml)

# Google Cloud Spanner ORM

This is a lightweight ORM written in Python and built on top of Cloud Spanner.
This is not an officially supported Google product.

## Getting started

### How to install

Make sure that Python 3.8 or higher is the default version of python for your
environment, then run:
```pip install git+https://github.com/google/python-spanner-orm#egg=spanner_orm```

### Connecting
To connect the Spanner ORM to an existing Spanner database:
``` python
import spanner_orm
spanner_orm.from_connection(
    spanner_orm.SpannerConnection(instance_name, database_name))
```

`project` and `credentials` are optional parameters, and the standard Spanner
client library will attempt to infer them automatically if not specified.
A session pool may be also specified by the `pool` parameter if necessary. An
explanation of session pools may be found
[here](https://googleapis.github.io/google-cloud-python/latest/spanner/advanced-session-pool-topics.html),
but the implementation of TransactionPingingPool in the standard Spanner client
libraries seems to not work, and the thread code associated with using the PingingPool
also seems to not do what is intended (ping the pool every so often)

### Creating a model
In order to write to and read from a table on Spanner, you need to tell the ORM
about the table by writing a model class, which looks something like this:
``` python
import spanner_orm

class TestModel(spanner_orm.Model):
  __table__ = 'TestTable'  # Name of table in Spanner
  __interleaved__ = None  # Name of table that the current table is interleaved
                          # into. None or omitted if the table is not interleaved

  # Every column in the table has a corresponding Field, where the first parameter
  # is the type of field. The primary key is constructed by the fields labeled
  # with primary_key=True in the order they appear in the class.
  # The name of the column is the same as the name of the class attribute
  id = spanner_orm.Field(spanner_orm.String(), primary_key=True)
  value = spanner_orm.Field(spanner_orm.Integer(), nullable=True)
  number = spanner_orm.Field(spanner_orm.Float(), nullable=True)

  # Secondary indexes are specified in a similar manner to fields:
  value_index = spanner_orm.Index(['value'])

  # To indicate that there is a foreign key relationship from this table to
  # another one, use a ForeignKeyRelationship.
  foreign_key = spanner_orm.ForeignKeyRelationship(
    'OtherModel',
    {'referencing_key': 'referenced_key'})
```

If the model does not refer to an existing table on Spanner, we can create
the corresponding table on the database through the ORM in one of two ways. If
the database has not yet been created, we can create it and the table at the
same time by:

``` python
admin_api = spanner_orm.connect_admin(
  'instance_name',
  'database_name',
  create_ddl=spanner_orm.model_creation_ddl(TestModel))
admin_api.create_database()
```

If the database already exists, we can execute a Migration where the upgrade
method returns a CreateTable for the model you have just defined (see section
on migrations)


### Retrieve data from Spanner
All queries through Spanner take place in a
[transaction](https://cloud.google.com/spanner/docs/transactions). The ORM
usually expects a transaction to be present and provided, but if None is
specified, a new transaction will be created for that request.
The two main ways of retrieving data through the ORM are ```where()``` and
```find()```/```find_multi()```:

``` python
# where() is invokes on a model class to retrieve models of that type. it takes
# a sequence of conditions. Most conditions that specify a Field, Index,
# Relationship, or Model can take  either the name of the object or the object
# itself
test_objects = TestModel.where(spanner_orm.greater_than('value', '50'))

# To also retrieve related objects, the includes() condition should be used:
test_and_other_objects = TestModel.where(
    spanner_orm.greater_than(TestModel.value, '50'),
    spanner_orm.includes(TestModel.fake_relationship),
)

# To create a transaction, run_read_only() or run_write() are used with the
# method to be run inside the transaction and any arguments to passs to the method.
# The method is invoked with the transaction as the first argument and then the
# rest of the provided arguments:
def callback_1(transaction, argument):
  return TestModel.find(id=argument, transaction=transaction)

specific_object = spanner_orm.spanner_api().run_read_only(callback, 1)

# Alternatively, the transactional_read decorator can be used to clean up the
# call a bit:
@transactional_read
def finder(argument, transaction=None):
  return TestModel.find(id=argument, transaction=transaction)
specific_object = finder(1)
```

### Write data to Spanner
The simplest way to write data is to create a Model (or retrieve one and modify
it) and then call save() on it:
``` python
test_model = TestModel({'key': 'key', 'value': 1})
test_model.save()
```
Note that creating a model as per above will fail if there's already a row in
the database where the primary key matches, as it uses a Spanner INSERT instead
of an UPDATE, as the ORM thinks it's a new object, as it wasn't retrieved from
Spanner.

For modifying multiple objects at the same time, the Model ```save_batch()``` method
can be used:
``` python
models = []
for i in range(10):
  key = 'test_{}'.format(i)
  models.append(TestModel({'key': key, 'value': value}))
TestModel.save_batch(models)
```

```spanner_orm.spanner_api().run_write()``` can be used for executing read-write
transactions, or the ```transactional_write``` decorator can be used similarly
to the read decorator mentioned above. Note that if a transaction fails due to
data being modified after the read happened and before the transaction finished
executing, the called method will be re-run until it succeeds or a certain
number of failures happen.  Make sure that there are no side effects that could
cause issues if called multiple times. Exceptions thrown out of the called
method will abort the transaction.

Other helper methods exist for more complex use cases (```create```, ```update```,
```upsert```, and others), but you will have to do more work in order to use those
correctly. See the documentation on those methods for more information.

## Migrations
### Creating migrations
Running ```spanner-orm generate <migration name>``` will generate a new
migration file to be filled out in the directory specified (or 'migrations' by
default). The ```upgrade``` function is executed when migrating, and the
```downgrade``` function is executed when rolling back the migration. Each of
these should return a single MigrationUpdate object (e.g., CreateTable,
AddColumn, etc.), as Spanner cannot execute multiple schema updates atomically.

### Executing migrations
Running ```spanner-orm migrate <Spanner instance> <Spanner database>``` will
execute all the unmigrated migrations for that database in the correct order,
using the application default credentials. If that won't work for your use case,
```MigrationExecutor``` can be used instead:

``` python
connection = spanner_orm.SpannerConnection(
  instance_name,
  database_name,
  credentials)
executor = spanner_orm.MigrationExecutor(connection)
executor.migrate()
```

Note that there is no protection against trying execute migrations concurrently
multiple times, so try not to do that.

If a migration needs to be rolled back,
```spanner-orm rollback <migration_name> <Spanner instance> <Spanner database>```
or the corresponding ```MigrationExecutor``` method should be used.

## Tests

Note: we suggest using a Python 3.8
[virtualenv](https://docs.python.org/3/library/venv.html)
for running tests and type checking.


Before running any tests, you'll need to download the Cloud Spanner Emulator.
See https://github.com/GoogleCloudPlatform/cloud-spanner-emulator for several
options. If you're on Linux, we recommend:

```
VERSION=1.2.0
wget https://storage.googleapis.com/cloud-spanner-emulator/releases/${VERSION}/cloud-spanner-emulator_linux_amd64-${VERSION}.tar.gz
tar zxvf cloud-spanner-emulator_linux_amd64-${VERSION}.tar.gz
chmod u+x gateway_main emulator_main
```

```
git clone git@github.com:GoogleCloudPlatform/cloud-spanner-emulator.git
```

To check type annotations, run:

```
pip install pytype
pytype spanner_orm
```

To check formatting, run (change `--diff` to `--in-place` to fix formatting):

```
pip install yapf
yapf --diff --recursive --parallel .
```

Then run tests with:

```
SPANNER_EMULATOR_BINARY_PATH=$(pwd)/emulator_main pytest
```
