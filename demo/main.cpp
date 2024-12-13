#include <QtSql>
#include <QCoreApplication>
#include <QTemporaryDir>

int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    Q_UNUSED(app)

    qDebug() << QSqlDatabase::drivers();

    Q_ASSERT(QSqlDatabase::isDriverAvailable("QSQLITE"));
    Q_ASSERT(QSqlDatabase::isDriverAvailable("DUCKDB"));

    QTemporaryDir tmp;
    Q_ASSERT(tmp.isValid());
    QString databasePath = "local.db";
    qDebug() << "DB File Path is:" << databasePath;


    QSqlDatabase dbconn = QSqlDatabase::addDatabase("DUCKDB");
    dbconn.setDatabaseName(databasePath);
    ///dbconn.setConnectOptions("");//TODO
    dbconn.open();
    qDebug() << "isOpen(): " << dbconn.isOpen() << "isOpenError" <<dbconn.isOpenError();
    if (!dbconn.isOpen())
    {
        qDebug() << "Connection failed: " << dbconn.lastError().driverText();
        exit(-1);
    }


    QSqlQuery query(dbconn);
    if(!query.exec("CREATE TABLE IF NOT EXISTS test (id int, name varchar)"))
    {
        qDebug() << query.lastError().text();
    }

    int i = 0;
    dbconn.transaction();
    do
    {
        if(!query.exec(QStringLiteral("insert into test values (%1, 'PRODUCT %1')").arg(i++)))
        {
            qCritical() << query.lastError().text();
            break;
        }
    }while(i<10);
    dbconn.commit();

    if(!query.exec("select id, name from test LIMIT 1"))
    {
        qCritical() << query.lastError().text();
    }
    while (query.next()) {
        qCritical() << query.record();
    }

    dbconn.close();

    return 0;
}
