
# QtDuckdbPluginv2

 QtDuckdbPluginv2 is sql plugin in Qt for Duckdb uses shared library  distruited by team of Duckdb.



## Setup
The plugin generates the sqlduckdb.dll , which should be copied to the sqldrivers directory of your Qt installation, for example:

```
C:\Qt\5.15.16\mingw81_64\plugins\sqldrivers
```

The driver use interface in c, so it requires the duckdb.dll, Simply copy the duckdb.dll copy in the folder of executable. 

## Example Uses:

```
QSqlDatabase db = QSqlDatabase::addDatabase("DUCKDB");
db.setDatabaseName("database.db");  //persistent database file "database.db"
db.open();
db.exec("CREATE TABLE product (id int, description VARCHAR(40), price  DECIMAL(18, 8));");
db.close()
```


## Current status
This is an alpha version and is still a work in progress.


## Version tested:
- Qt: 5.15.16
- Duckdb: v1.1.3
- OS: Windows 10

TODO list:

1. Open db fail on test but not in demo app
2. close of Qsqldatabase does not close the connection
3. test with multithread
4. add performance test

References:

- https://duckdb.org/docs/api/c/overview