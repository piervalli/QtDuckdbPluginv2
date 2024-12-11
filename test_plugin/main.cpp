#include <QtTest/QtTest>
#include <QTemporaryDir>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>


class TestDuckdbPlugin: public QObject
{
    Q_OBJECT
private slots:
    void initTestCase() // will run once before the first test
    {
        // Check that the driver exists
        if(!QSqlDatabase::contains("db"))
        {
            qCritical() << QSqlDatabase::drivers();
            QVERIFY2(QSqlDatabase::isDriverAvailable("DUCKDB"), "DUCKDB driver not found.");
            // Set the database file
            QString dbname = QStringLiteral("test.db");
            QSqlDatabase db = QSqlDatabase::addDatabase("DUCKDB","db");
            db.setDatabaseName(dbname);
            qCritical() << db.isOpen() << db.isOpenError();
        }
    }
    void cleanup()
    {
        QSqlDatabase db = QSqlDatabase::database("db");
        db.setConnectOptions();
        db.setPassword(QString());
        db.close();
    }
    void open()
    {
        QSqlDatabase db = QSqlDatabase::database("db");
        qCritical() << db.isOpen() << db.isOpenError();
        if(db.isOpen())
            db.close();
        qCritical() << db.isOpen() << db.isOpenError();
        auto ok = db.open();
        auto msg = QStringLiteral("error database not open %1").arg(db.lastError().text());
        QVERIFY2(ok,msg.toLatin1().constData());
         db.close();
    }

    void cleanupTestCase()
    {
        QSqlDatabase::removeDatabase("db");
    }
private:
    QTemporaryDir tmpDir;
};


QTEST_GUILESS_MAIN(TestDuckdbPlugin)
#include "main.moc"
