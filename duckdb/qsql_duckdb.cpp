/****************************************************************************
**
** Copyright (C) 2016 The Qt Company Ltd.
** Contact: https://www.qt.io/licensing/
**
** This file is part of the QtSql module of the Qt Toolkit.
**
** $QT_BEGIN_LICENSE:LGPL$
** Commercial License Usage
** Licensees holding valid commercial Qt licenses may use this file in
** accordance with the commercial license agreement provided with the
** Software or, alternatively, in accordance with the terms contained in
** a written agreement between you and The Qt Company. For licensing terms
** and conditions see https://www.qt.io/terms-conditions. For further
** information use the contact form at https://www.qt.io/contact-us.
**
** GNU Lesser General Public License Usage
** Alternatively, this file may be used under the terms of the GNU Lesser
** General Public License version 3 as published by the Free Software
** Foundation and appearing in the file LICENSE.LGPL3 included in the
** packaging of this file. Please review the following information to
** ensure the GNU Lesser General Public License version 3 requirements
** will be met: https://www.gnu.org/licenses/lgpl-3.0.html.
**
** GNU General Public License Usage
** Alternatively, this file may be used under the terms of the GNU
** General Public License version 2.0 or (at your option) the GNU General
** Public license version 3 or any later version approved by the KDE Free
** Qt Foundation. The licenses are as published by the Free Software
** Foundation and appearing in the file LICENSE.GPL2 and LICENSE.GPL3
** included in the packaging of this file. Please review the following
** information to ensure the GNU General Public License requirements will
** be met: https://www.gnu.org/licenses/gpl-2.0.html and
** https://www.gnu.org/licenses/gpl-3.0.html.
**
** $QT_END_LICENSE$
**
****************************************************************************/

#include "qsql_duckdb_p.h"

#include <qcoreapplication.h>
#include <qdatetime.h>
#include <qvariant.h>
#include <qsqlerror.h>
#include <qsqlfield.h>
#include <qsqlindex.h>
#include <qsqlquery.h>
#include <QtSql/private/qsqlcachedresult_p.h>
#include <QtSql/private/qsqldriver_p.h>
#include <qstringlist.h>
#include <qvector.h>
#include <qdebug.h>
#if QT_CONFIG(regularexpression)
#include <qcache.h>
#include <qregularexpression.h>
#endif
#include <QScopedValueRollback>

#if defined Q_OS_WIN
# include <qt_windows.h>
#else
# include <unistd.h>
#endif

#include <functional>

Q_DECLARE_OPAQUE_POINTER(duckdb_database *)
Q_DECLARE_METATYPE(duckdb_database *)

Q_DECLARE_OPAQUE_POINTER(duckdb_connection *)
Q_DECLARE_METATYPE(duckdb_connection *)

Q_DECLARE_OPAQUE_POINTER(duckdb_result *)
Q_DECLARE_METATYPE(duckdb_result *)

Q_DECLARE_OPAQUE_POINTER(duckdb_prepared_statement *)
Q_DECLARE_METATYPE(duckdb_prepared_statement *)


QT_BEGIN_NAMESPACE

static QString _q_escapeIdentifier(const QString &identifier, QSqlDriver::IdentifierType type)
{
    QString res = identifier;
    // If it contains [ and ] then we assume it to be escaped properly already as this indicates
    // the syntax is exactly how it should be
    if (identifier.contains(QLatin1Char('[')) && identifier.contains(QLatin1Char(']')))
        return res;
    if (!identifier.isEmpty() && !identifier.startsWith(QLatin1Char('"')) && !identifier.endsWith(QLatin1Char('"'))) {
        res.replace(QLatin1Char('"'), QLatin1String("\"\""));
        res.prepend(QLatin1Char('"')).append(QLatin1Char('"'));
        if (type == QSqlDriver::TableName)
            res.replace(QLatin1Char('.'), QLatin1String("\".\""));
    }
    return res;
}

static QVariant::Type qGetColumnType(const QString &tpName)
{
    const QString typeName = tpName.toLower();

    if (typeName == QLatin1String("integer")
        || typeName == QLatin1String("int"))
        return QVariant::Int;
    if (typeName == QLatin1String("double")
        || typeName == QLatin1String("float")
        || typeName == QLatin1String("real")
        || typeName.startsWith(QLatin1String("numeric")))
        return QVariant::Double;
    if (typeName == QLatin1String("blob"))
        return QVariant::ByteArray;
    if (typeName == QLatin1String("boolean")
        || typeName == QLatin1String("bool"))
        return QVariant::Bool;
    return QVariant::String;
}

static QSqlError qMakeError(const QString &descr,const char *error_message,QSqlError::ErrorType type,
                            int errorCode)
{
    return QSqlError(descr,QString::fromLocal8Bit(error_message),type, QString::number(errorCode));
}

class QDuckdbResultPrivate;

class QDuckdbResult : public QSqlCachedResult
{
    Q_DECLARE_PRIVATE(QDuckdbResult)
    friend class QDuckdbDriver;

public:
    explicit QDuckdbResult(const QDuckdbDriver* db);
    ~QDuckdbResult();
    QVariant handle() const override;

protected:
    bool gotoNext(QSqlCachedResult::ValueCache& row, int idx) override;
    bool reset(const QString &query) override;
    bool prepare(const QString &query) override;
    bool execBatch(bool arrayBind) override;
    bool exec() override;
    int size() override;
    int numRowsAffected() override;
    QVariant lastInsertId() const override;
    QSqlRecord record() const override;
    void detachFromResultSet() override;
    void virtual_hook(int id, void *data) override;
};

class QDuckdbDriverPrivate : public QSqlDriverPrivate
{
    Q_DECLARE_PUBLIC(QDuckdbDriver)

public:
    inline QDuckdbDriverPrivate() : QSqlDriverPrivate(QSqlDriver::SQLite) {
        access=new duckdb_database;
        conn=new duckdb_connection;
    }
    duckdb_database *access=nullptr;
    duckdb_connection  *conn=nullptr;
    duckdb_prepared_statement stmt;
    QVector<QDuckdbResult *> results;
    QStringList notificationid;
};


class QDuckdbResultPrivate : public QSqlCachedResultPrivate
{
    Q_DECLARE_PUBLIC(QDuckdbResult)

public:

    Q_DECLARE_SQLDRIVER_PRIVATE(QDuckdbDriver)
    using QSqlCachedResultPrivate::QSqlCachedResultPrivate;
    void cleanup();
    bool fetchNext(QSqlCachedResult::ValueCache &values, int idx, bool initialFetch);
    // initializes the recordInfo and the cache
    void initColumns();
    void finalize();

    duckdb_prepared_statement  *stmt=nullptr;
    duckdb_result *result=nullptr;
    QSqlRecord rInf;
    QVector<QVariant> firstRow;
    idx_t row_count=0;
};

void QDuckdbResultPrivate::cleanup()
{
    Q_Q(QDuckdbResult);
    finalize();
    rInf.clear();
    q->setAt(QSql::BeforeFirstRow);
    q->setActive(false);
    q->cleanup();
}

void QDuckdbResultPrivate::finalize()
{
    if(result!=nullptr)
        duckdb_destroy_result(result);
    if (stmt!=nullptr)
        duckdb_destroy_prepare(stmt);
    stmt = nullptr;
    result = nullptr;
}

void QDuckdbResultPrivate::initColumns()
{
    Q_Q(QDuckdbResult);
    int nCols = duckdb_column_count(result);
    if (nCols <= 0)
        return;

    q->init(nCols);

    for (int i = 0; i < nCols; ++i) {
        QString colName = QString::fromUtf8(duckdb_column_name(result, i)).remove(QLatin1Char('"'));
        const QString tableName=QStringLiteral("query");
        int stp =  duckdb_column_type(result, i);

        QVariant::Type fieldType;

        // Get the proper type for the field based on stp value
        switch (stp) {
        case DUCKDB_TYPE_INTEGER:
            fieldType = QVariant::Int;
            break;
        case DUCKDB_TYPE_FLOAT:
            fieldType = QVariant::Double;
            break;
        case DUCKDB_TYPE_BLOB:
            fieldType = QVariant::ByteArray;
            break;
        case DUCKDB_TYPE_VARCHAR:
            fieldType = QVariant::String;
            break;
        case DUCKDB_TYPE_SQLNULL:
            fieldType = QVariant::Invalid;
            break;
        default:
            fieldType = QVariant::Invalid;
            qCritical() <<  "unsupported type" << stp << colName;
            break;
        }

        QSqlField fld(colName, fieldType, tableName);
        fld.setSqlType(stp);
        rInf.append(fld);
    }
}

bool QDuckdbResultPrivate::fetchNext(QSqlCachedResult::ValueCache &values, int idx, bool initialFetch)
{
    Q_Q(QDuckdbResult);
    int row_count = duckdb_row_count(result);
    qCritical() << values.size()<<row_count << idx;
    if(row_count < idx) return false;
    int i;


    if (!stmt) {
        q->setLastError(QSqlError(QCoreApplication::translate("QDuckdbResult", "Unable to fetch row"),
                                  QCoreApplication::translate("QDuckdbResult", "No query"), QSqlError::ConnectionError));
        q->setAt(QSql::AfterLastRow);
        return false;
    }
    // if(!result){
    //     result=new duckdb_result;
    // }


    // if(initialFetch) {
    //     res = duckdb_execute_prepared(*stmt, result);
    //     if(res==DuckDBError){
    //         const char *error_message = duckdb_result_error(result);
    //         q->setLastError(qMakeError(QCoreApplication::translate("QDuckdbResult","Unable to execute statement"), error_message,QSqlError::StatementError, res));
    //         q->setAt(QSql::AfterLastRow);
    //         return false;
    //     }
    //     firstRow.clear();
    //     idx_t column_count = duckdb_column_count(result);
    //     row_count = duckdb_row_count(result);
    //     firstRow.resize(column_count);
    //     if (rInf.isEmpty())
    //         initColumns(false);// must be first call.
    //     for(idx_t i=0;i<row_count;i++)
    //         values[i]=firstRow[i];
    // }


    if (idx < 0 && !initialFetch)
        return true;
    for (i = 0; i < rInf.count(); ++i)
    {
        // qCritical() << "column_count" << i << idx;
        auto column_type = duckdb_column_type(result, i);
        switch (column_type) {
        case DUCKDB_TYPE_BLOB:
            //     // values[i + idx] = QByteArray(static_cast<const char *>(
            //     //             sqlite3_column_blob(stmt, i)),
            //     //             sqlite3_column_bytes(stmt, i));
            values[i + idx]=QByteArray{};//TODO
            break;
        case DUCKDB_TYPE_INTEGER:
            values[i+idx] = duckdb_value_int64(result, i,idx);
            break;
        // case DUCKDB_TYPE_FLOAT:
        //     switch(q->numericalPrecisionPolicy()) {
        //         case QSql::LowPrecisionInt32:
        //             values[i + idx] = sqlite3_column_int(stmt, i);
        //             break;
        //         case QSql::LowPrecisionInt64:
        //             values[i + idx] = sqlite3_column_int64(result, i);
        //             break;
        //         case QSql::LowPrecisionDouble:
        //         case QSql::HighPrecision:
        //         default:
        //             values[i + idx] = sqlite3_column_double(result, i);
        //             break;
        //     };
        //     break;
        case DUCKDB_TYPE_SQLNULL:
            values[i + idx] = QVariant(QVariant::String);
            break;
        case DUCKDB_TYPE_VARCHAR:
        {
            char *val = duckdb_value_varchar(result, i,idx);
            if(val!=nullptr)
            {
                values[i + idx] = QString::fromUtf8(val);
                duckdb_free(val);
            }else{
                values[i + idx] = QVariant(QVariant::String);
            }
        }
        break;
        default:
            qCritical() << "unsupported type " << column_type << duckdb_column_name(result,idx);
            break;
        }
    }

    return true;
}

QDuckdbResult::QDuckdbResult(const QDuckdbDriver* db)
    : QSqlCachedResult(*new QDuckdbResultPrivate(this, db))
{
    Q_D(QDuckdbResult);
    const_cast<QDuckdbDriverPrivate*>(d->drv_d_func())->results.append(this);
}

QDuckdbResult::~QDuckdbResult()
{
    Q_D(QDuckdbResult);
    if (d->drv_d_func())
        const_cast<QDuckdbDriverPrivate*>(d->drv_d_func())->results.removeOne(this);
    d->cleanup();
}

void QDuckdbResult::virtual_hook(int id, void *data)
{
    QSqlCachedResult::virtual_hook(id, data);
}

bool QDuckdbResult::reset(const QString &query)
{
    if (!prepare(query))
        return false;
    return exec();
}

bool QDuckdbResult::prepare(const QString &query)
{
    Q_D(QDuckdbResult);
    if (!driver() || !driver()->isOpen() || driver()->isOpenError())
        return false;

    d->cleanup();

    setSelect(false);
    if(!d->stmt){
        d->stmt=new duckdb_prepared_statement ;
    }
    int res = duckdb_prepare(*d->drv_d_func()->conn, query.toUtf8().constData(), d->stmt);
    if (res != DuckDBSuccess) {
        const char *error_message = duckdb_prepare_error(*d->stmt);
        setLastError(qMakeError(QCoreApplication::translate("QDuckdbResult","Unable to execute statement"), error_message,QSqlError::StatementError, res));
        d->finalize();
        return false;
    }
    // setSelect(true);
    return true;
}

bool QDuckdbResult::execBatch(bool arrayBind)
{
    Q_UNUSED(arrayBind);
    Q_D(QSqlResult);
    QScopedValueRollback<QVector<QVariant>> valuesScope(d->values);
    QVector<QVariant> values = d->values;
    if (values.count() == 0)
        return false;

    for (int i = 0; i < values.at(0).toList().count(); ++i) {
        d->values.clear();
        QScopedValueRollback<QHash<QString, QVector<int>>> indexesScope(d->indexes);
        QHash<QString, QVector<int>>::const_iterator it = d->indexes.constBegin();
        while (it != d->indexes.constEnd()) {
            bindValue(it.key(), values.at(it.value().first()).toList().at(i), QSql::In);
            ++it;
        }
        if (!exec())
            return false;
    }
    return true;
}

bool QDuckdbResult::exec()
{
    Q_D(QDuckdbResult);
    QVector<QVariant> values = boundValues();

    d->rInf.clear();
    clearValues();
    setLastError(QSqlError());
    setActive(false);
    setAt(QSql::BeforeFirstRow);

    int res=0;
    // if(d->stmt) {
    res=duckdb_clear_bindings(*d->stmt);
    if (res == DuckDBError) {
        setLastError(qMakeError(QCoreApplication::translate("QDuckdbResult",
                                                            "Unable to reset statement"),"", QSqlError::StatementError, res));
        d->finalize();
        return false;
    }
    // }

    // int res = sqlite3_reset(d->drv_d_func()->stmt);
    // if (res != SQLITE_OK) {
    //     setLastError(qMakeError(d->drv_d_func()->access, QCoreApplication::translate("QDuckdbResult",
    //                  "Unable to reset statement"), QSqlError::StatementError, res));
    //     d->finalize();
    //     return false;
    // }

    int paramCount = duckdb_nparams(*d->stmt);
    bool paramCountIsValid = paramCount == values.count();

    // #if (SQLITE_VERSION_NUMBER >= 3003011)
    // In the case of the reuse of a named placeholder
    // We need to check explicitly that paramCount is greater than or equal to 1, as sqlite
    // can end up in a case where for virtual tables it returns 0 even though it
    // has parameters
    if (paramCount >= 1 && paramCount < values.count()) {
        const auto countIndexes = [](int counter, const QVector<int> &indexList) {
            return counter + indexList.length();
        };

        const int bindParamCount = std::accumulate(d->indexes.cbegin(),
                                                   d->indexes.cend(),
                                                   0,
                                                   countIndexes);

        paramCountIsValid = bindParamCount == values.count();
        // When using named placeholders, it will reuse the index for duplicated
        // placeholders. So we need to ensure the QVector has only one instance of
        // each value as SQLite will do the rest for us.
        QVector<QVariant> prunedValues;
        QVector<int> handledIndexes;
        for (int i = 0, currentIndex = 0; i < values.size(); ++i) {
            if (handledIndexes.contains(i))
                continue;
            const char *parameterName = duckdb_parameter_name(*d->stmt, currentIndex + 1);
            if (!parameterName) {
                paramCountIsValid = false;
                continue;
            }
            const auto placeHolder = QString::fromUtf8(parameterName);
            const auto &indexes = d->indexes.value(placeHolder);
            handledIndexes << indexes;
            prunedValues << values.at(indexes.first());
            ++currentIndex;
        }
        values = prunedValues;
    }
    // #endif

    if (paramCountIsValid) {
        for (int i = 0; i < paramCount; ++i) {
            res = DuckDBSuccess;
            const QVariant &value = values.at(i);

            if (value.isNull()) {
                res = duckdb_bind_null(*d->stmt, i + 1);
            } else {
                switch (value.userType()) {
                case QVariant::ByteArray: {
                    res = DuckDBError;
                    // const QByteArray *ba = static_cast<const QByteArray*>(value.constData());
                    // res = duckdb_bind_blob(d->stmt, i + 1, ba->constData(),
                    //                         ba->size(), SQLITE_STATIC);
                    break; }
                case QVariant::Int:
                case QVariant::Bool:
                    res = duckdb_bind_int32(*d->stmt, i + 1, value.toInt());
                    break;
                case QVariant::Double:
                    res = duckdb_bind_double(*d->stmt, i + 1, value.toDouble());
                    break;
                case QVariant::UInt:
                case QVariant::LongLong:
                    res = duckdb_bind_int64(*d->stmt, i + 1, value.toLongLong());
                    break;
                case QVariant::DateTime: {
                    res = DuckDBError;
                    const QDateTime dateTime = value.toDateTime();
                    const QString str = dateTime.toString(Qt::ISODateWithMs);
                    // res = duckdb_bind_text16(d->stmt, i + 1, str.utf16(),
                    //                           str.size() * sizeof(ushort), SQLITE_TRANSIENT);
                    break;
                }
                case QVariant::Time: {
                    res = DuckDBError;
                    const QTime time = value.toTime();
                    const QString str = time.toString(u"hh:mm:ss.zzz");
                    // res = duckdb_bind_text16(d->stmt, i + 1, str.utf16(),
                    //                           str.size() * sizeof(ushort), SQLITE_TRANSIENT);
                    break;
                }
                case QVariant::String: {
                    // lifetime of string == lifetime of its qvariant
                    const QString *str = static_cast<const QString*>(value.constData());
                    res = duckdb_bind_varchar(*d->stmt, i + 1, str->toUtf8().constData());
                    break; }
                default: {
                    QString str = value.toString();
                    // SQLITE_TRANSIENT makes sure that sqlite buffers the data
                    res = duckdb_bind_varchar(*d->stmt, i + 1, str.toUtf8().constData());
                    break; }
                }
            }
            if (res != DuckDBSuccess) {
                setLastError(qMakeError(QCoreApplication::translate("QDuckdbResult",
                                                                    "Unable to bind parameters"),"", QSqlError::StatementError, res));
                d->finalize();
                return false;
            }
        }
    } else {
        setLastError(QSqlError(QCoreApplication::translate("QDuckdbResult",
                                                           "Parameter count mismatch"), QString(), QSqlError::StatementError));
        return false;
    }

    if (lastError().isValid()) {
        setSelect(false);
        setActive(false);
        return false;
    }
    // setSelect(!d->rInf.isEmpty());
    if(d->result==nullptr){
        d->result=new duckdb_result;
    }
    res = duckdb_execute_prepared(*d->stmt, d->result);
    if(res==DuckDBError){
        const char *error_message = duckdb_result_error(d->result);
        setLastError(qMakeError(QCoreApplication::translate("QDuckdbResult","Unable to execute statement"), error_message,QSqlError::StatementError, res));
        setAt(QSql::AfterLastRow);
        return false;
    }
    d->initColumns();
    setSelect(true);
    setActive(true);

    return true;
}

bool QDuckdbResult::gotoNext(QSqlCachedResult::ValueCache& row, int idx)
{
    Q_D(QDuckdbResult);
    return d->fetchNext(row, idx, false);
}

int QDuckdbResult::size()
{
    return -1;
}

int QDuckdbResult::numRowsAffected()
{
    Q_D(const QDuckdbResult);
    return duckdb_row_count(d->result);
}

QVariant QDuckdbResult::lastInsertId() const
{
    Q_D(const QDuckdbResult);
    // if (isActive()) {
    //     qint64 id = sqlite3_last_insert_rowid(d->drv_d_func()->access);
    //     if (id)
    //         return id;
    // }
    return QVariant();
}

QSqlRecord QDuckdbResult::record() const
{
    Q_D(const QDuckdbResult);
    if (!isActive() || !isSelect())
        return QSqlRecord();
    return d->rInf;
}

void QDuckdbResult::detachFromResultSet()
{
    Q_D(QDuckdbResult);
    if (d->stmt)
        duckdb_clear_bindings(*d->stmt);//sqlite3_reset(d->stmt);
}

QVariant QDuckdbResult::handle() const
{
    Q_D(const QDuckdbResult);
    return QVariant::fromValue(d->stmt);
}

/////////////////////////////////////////////////////////

#if QT_CONFIG(regularexpression)
// static void _q_regexp(sqlite3_context* context, int argc, sqlite3_value** argv)
// {
//     if (Q_UNLIKELY(argc != 2)) {
//         sqlite3_result_int(context, 0);
//         return;
//     }

//     const QString pattern = QString::fromUtf8(
//         reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
//     const QString subject = QString::fromUtf8(
//         reinterpret_cast<const char*>(sqlite3_value_text(argv[1])));

//     auto cache = static_cast<QCache<QString, QRegularExpression>*>(sqlite3_user_data(context));
//     auto regexp = cache->object(pattern);
//     const bool wasCached = regexp;

//     if (!wasCached)
//         regexp = new QRegularExpression(pattern, QRegularExpression::DontCaptureOption);

//     const bool found = subject.contains(*regexp);

//     if (!wasCached)
//         cache->insert(pattern, regexp);

//     sqlite3_result_int(context, int(found));
// }

// static void _q_regexp_cleanup(void *cache)
// {
//     delete static_cast<QCache<QString, QRegularExpression>*>(cache);
// }
#endif

QDuckdbDriver::QDuckdbDriver(QObject * parent)
    : QSqlDriver(*new QDuckdbDriverPrivate, parent)
{
}

QDuckdbDriver::QDuckdbDriver(duckdb_database *connection, QObject *parent)
    : QSqlDriver(*new QDuckdbDriverPrivate, parent)
{
    Q_D(QDuckdbDriver);
    d->access = connection;
    setOpen(true);
    setOpenError(false);
}


QDuckdbDriver::~QDuckdbDriver()
{
    close();
}

bool QDuckdbDriver::hasFeature(DriverFeature f) const
{
    switch (f) {
    case BLOB:
    case Transactions:
    case Unicode:
    case LastInsertId:
    case PreparedQueries:
    case PositionalPlaceholders:
    case SimpleLocking:
    case FinishQuery:
    case LowPrecisionNumbers:
    case EventNotifications:
        return true;
    case QuerySize:
    case BatchOperations:
    case MultipleResultSets:
    case CancelQuery:
        return false;
    case NamedPlaceholders:
#if (SQLITE_VERSION_NUMBER < 3003011)
        return false;
#else
        return true;
#endif

    }
    return false;
}

/*
   SQLite dbs have no user name, passwords, hosts or ports.
   just file names.
*/
bool QDuckdbDriver::open(const QString & db, const QString &, const QString &, const QString &, int, const QString &conOpts)
{
    Q_D(QDuckdbDriver);
    if (isOpen())
        close();


    int timeOut = 5000;
    bool sharedCache = false;
    bool openReadOnlyOption = false;
    bool openUriOption = false;
#if QT_CONFIG(regularexpression)
    static const QLatin1String regexpConnectOption = QLatin1String("QDUCKDB_ENABLE_REGEXP");
    bool defineRegexp = false;
    int regexpCacheSize = 25;
#endif

    const auto opts = conOpts.splitRef(QLatin1Char(';'));
    for (auto option : opts) {
        option = option.trimmed();
        if (option.startsWith(QLatin1String("QDUCKDB_BUSY_TIMEOUT"))) {
            option = option.mid(20).trimmed();
            if (option.startsWith(QLatin1Char('='))) {
                bool ok;
                const int nt = option.mid(1).trimmed().toInt(&ok);
                if (ok)
                    timeOut = nt;
            }
        } else if (option == QLatin1String("QDUCKDB_OPEN_READONLY")) {
            openReadOnlyOption = true;
        } else if (option == QLatin1String("QDUCKDB_OPEN_URI")) {
            openUriOption = true;
        } else if (option == QLatin1String("QDUCKDB_ENABLE_SHARED_CACHE")) {
            sharedCache = true;
        }
#if QT_CONFIG(regularexpression)
        else if (option.startsWith(regexpConnectOption)) {
            option = option.mid(regexpConnectOption.size()).trimmed();
            if (option.isEmpty()) {
                defineRegexp = true;
            } else if (option.startsWith(QLatin1Char('='))) {
                bool ok = false;
                const int cacheSize = option.mid(1).trimmed().toInt(&ok);
                if (ok) {
                    defineRegexp = true;
                    if (cacheSize > 0)
                        regexpCacheSize = cacheSize;
                }
            }
        }
#endif
    }
    duckdb_config config;
    // create the configuration object
    if (duckdb_create_config(&config) == DuckDBError) {
        // handle error
    }
    // set some configuration options

    duckdb_set_config(config, "threads", "8");
    duckdb_set_config(config, "max_memory", "8GB");
    duckdb_set_config(config, "default_order", "DESC");

    if(openReadOnlyOption)
        duckdb_set_config(config, "access_mode", "READ_ONLY"); // or READ_ONLY
    else
        duckdb_set_config(config, "access_mode", "READ_WRITE"); // or READ_ONLY

    //     int openMode = (openReadOnlyOption ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE));
    //     openMode |= (sharedCache ? SQLITE_OPEN_SHAREDCACHE : SQLITE_OPEN_PRIVATECACHE);
    //     openMode |= SQLITE_OPEN_NOMUTEX;


    char *error_message = nullptr;
    QString msg{};
    int res=0;
    do{
        res = duckdb_open_ext(db.toUtf8().constData(), d->access,config,&error_message);
        if (res == DuckDBError)
        {
            setLastError(qMakeError(tr("Error opening database"),error_message, QSqlError::ConnectionError, -1));
            setOpenError(true);
            break;
        }
        // sqlite3_busy_timeout(d->access, timeOut);

        res = duckdb_connect(*d->access, d->conn);
        if (res == DuckDBError)
        {
            duckdb_disconnect(d->conn);
            setLastError(qMakeError(tr("Error connection to database"),"", QSqlError::ConnectionError, -1));
            if (d->access) {
                duckdb_close(d->access);
                d->access = 0;
            }
            setOpenError(true);
            break;
        }
        setOpen(true);
        setOpenError(false);
    } while(false);

    if(error_message!=nullptr)
        duckdb_free((void*)error_message);
    duckdb_destroy_config(&config);
    return res == DuckDBSuccess;
}

void QDuckdbDriver::close()
{
    Q_D(QDuckdbDriver);
    if (isOpen()) {
        for (QDuckdbResult *result : qAsConst(d->results))
            result->d_func()->finalize();

        if (d->access && (d->notificationid.count() > 0)) {
            d->notificationid.clear();
            // sqlite3_update_hook(d->access, NULL, NULL);
        }

        // const int res = sqlite3_close(d->access);

        // if (res != SQLITE_OK)
        //     setLastError(qMakeError(d->access, tr("Error closing database"), QSqlError::ConnectionError, res));
        d->access = 0;
        setOpen(false);
        setOpenError(false);
    }
}

QSqlResult *QDuckdbDriver::createResult() const
{
    return new QDuckdbResult(this);
}

bool QDuckdbDriver::beginTransaction()
{
    if (!isOpen() || isOpenError())
        return false;

    QSqlQuery q(createResult());
    if (!q.exec(QLatin1String("BEGIN"))) {
        setLastError(QSqlError(tr("Unable to begin transaction"),
                               q.lastError().databaseText(), QSqlError::TransactionError));
        return false;
    }

    return true;
}

bool QDuckdbDriver::commitTransaction()
{
    if (!isOpen() || isOpenError())
        return false;

    QSqlQuery q(createResult());
    if (!q.exec(QLatin1String("COMMIT"))) {
        setLastError(QSqlError(tr("Unable to commit transaction"),
                               q.lastError().databaseText(), QSqlError::TransactionError));
        return false;
    }

    return true;
}

bool QDuckdbDriver::rollbackTransaction()
{
    if (!isOpen() || isOpenError())
        return false;

    QSqlQuery q(createResult());
    if (!q.exec(QLatin1String("ROLLBACK"))) {
        setLastError(QSqlError(tr("Unable to rollback transaction"),
                               q.lastError().databaseText(), QSqlError::TransactionError));
        return false;
    }

    return true;
}

QStringList QDuckdbDriver::tables(QSql::TableType type) const
{
    QStringList res;
    if (!isOpen())
        return res;

    QSqlQuery q(createResult());
    q.setForwardOnly(true);

    QString sql = QLatin1String("SELECT name FROM sqlite_master WHERE %1 "
                                "UNION ALL SELECT name FROM sqlite_temp_master WHERE %1");
    if ((type & QSql::Tables) && (type & QSql::Views))
        sql = sql.arg(QLatin1String("type='table' OR type='view'"));
    else if (type & QSql::Tables)
        sql = sql.arg(QLatin1String("type='table'"));
    else if (type & QSql::Views)
        sql = sql.arg(QLatin1String("type='view'"));
    else
        sql.clear();

    if (!sql.isEmpty() && q.exec(sql)) {
        while(q.next())
            res.append(q.value(0).toString());
    }

    if (type & QSql::SystemTables) {
        // there are no internal tables beside this one:
        res.append(QLatin1String("sqlite_master"));
    }

    return res;
}

static QSqlIndex qGetTableInfo(QSqlQuery &q, const QString &tableName, bool onlyPIndex = false)
{
    QString schema;
    QString table(tableName);
    const int indexOfSeparator = tableName.indexOf(QLatin1Char('.'));
    if (indexOfSeparator > -1) {
        const int indexOfCloseBracket = tableName.indexOf(QLatin1Char(']'));
        if (indexOfCloseBracket != tableName.size() - 1) {
            // Handles a case like databaseName.tableName
            schema = tableName.left(indexOfSeparator + 1);
            table = tableName.mid(indexOfSeparator + 1);
        } else {
            const int indexOfOpenBracket = tableName.lastIndexOf(QLatin1Char('['), indexOfCloseBracket);
            if (indexOfOpenBracket > 0) {
                // Handles a case like databaseName.[tableName]
                schema = tableName.left(indexOfOpenBracket);
                table = tableName.mid(indexOfOpenBracket);
            }
        }
    }
    q.exec(QLatin1String("PRAGMA ") + schema + QLatin1String("table_info (") +
           _q_escapeIdentifier(table, QSqlDriver::TableName) + QLatin1Char(')'));
    QSqlIndex ind;
    while (q.next()) {
        bool isPk = q.value(5).toInt();
        if (onlyPIndex && !isPk)
            continue;
        QString typeName = q.value(2).toString().toLower();
        QString defVal = q.value(4).toString();
        if (!defVal.isEmpty() && defVal.at(0) == QLatin1Char('\'')) {
            const int end = defVal.lastIndexOf(QLatin1Char('\''));
            if (end > 0)
                defVal = defVal.mid(1, end - 1);
        }

        QSqlField fld(q.value(1).toString(), qGetColumnType(typeName), tableName);
        if (isPk && (typeName == QLatin1String("integer")))
            // INTEGER PRIMARY KEY fields are auto-generated in sqlite
            // INT PRIMARY KEY is not the same as INTEGER PRIMARY KEY!
            fld.setAutoValue(true);
        fld.setRequired(q.value(3).toInt() != 0);
        fld.setDefaultValue(defVal);
        ind.append(fld);
    }
    return ind;
}

QSqlIndex QDuckdbDriver::primaryIndex(const QString &tblname) const
{
    if (!isOpen())
        return QSqlIndex();

    QString table = tblname;
    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);

    QSqlQuery q(createResult());
    q.setForwardOnly(true);
    return qGetTableInfo(q, table, true);
}

QSqlRecord QDuckdbDriver::record(const QString &tbl) const
{
    if (!isOpen())
        return QSqlRecord();

    QString table = tbl;
    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);

    QSqlQuery q(createResult());
    q.setForwardOnly(true);
    return qGetTableInfo(q, table);
}

QVariant QDuckdbDriver::handle() const
{
    Q_D(const QDuckdbDriver);
    return QVariant::fromValue(d->conn);
}

QString QDuckdbDriver::escapeIdentifier(const QString &identifier, IdentifierType type) const
{
    return _q_escapeIdentifier(identifier, type);
}

// static void handle_sqlite_callback(void *qobj,int aoperation, char const *adbname, char const *atablename,
//                                    sqlite3_int64 arowid)
// {
//     Q_UNUSED(aoperation);
//     Q_UNUSED(adbname);
//     QDuckdbDriver *driver = static_cast<QDuckdbDriver *>(qobj);
//     if (driver) {
//         QMetaObject::invokeMethod(driver, "handleNotification", Qt::QueuedConnection,
//                                   Q_ARG(QString, QString::fromUtf8(atablename)), Q_ARG(qint64, arowid));
//     }
// }

bool QDuckdbDriver::subscribeToNotification(const QString &name)
{
    Q_D(QDuckdbDriver);
    if (!isOpen()) {
        qWarning("Database not open.");
        return false;
    }

    // if (d->notificationid.contains(name)) {
    //     qWarning("Already subscribing to '%s'.", qPrintable(name));
    //     return false;
    // }

    // //sqlite supports only one notification callback, so only the first is registered
    // d->notificationid << name;
    // if (d->notificationid.count() == 1)
    //     sqlite3_update_hook(d->access, &handle_sqlite_callback, reinterpret_cast<void *> (this));

    return true;
}

bool QDuckdbDriver::unsubscribeFromNotification(const QString &name)
{
    Q_D(QDuckdbDriver);
    if (!isOpen()) {
        qWarning("Database not open.");
        return false;
    }

    // if (!d->notificationid.contains(name)) {
    //     qWarning("Not subscribed to '%s'.", qPrintable(name));
    //     return false;
    // }

    // d->notificationid.removeAll(name);
    // if (d->notificationid.isEmpty())
    //     sqlite3_update_hook(d->access, NULL, NULL);

    return true;
}

QStringList QDuckdbDriver::subscribedToNotifications() const
{
    Q_D(const QDuckdbDriver);
    return d->notificationid;
}

void QDuckdbDriver::handleNotification(const QString &tableName, qint64 rowid)
{
    Q_D(const QDuckdbDriver);
    if (d->notificationid.contains(tableName)) {
#if QT_DEPRECATED_SINCE(5, 15)
        QT_WARNING_PUSH
            QT_WARNING_DISABLE_DEPRECATED
            emit notification(tableName);
        QT_WARNING_POP
#endif
            emit notification(tableName, QSqlDriver::UnknownSource, QVariant(rowid));
    }
}

QT_END_NAMESPACE

#include "moc_qsql_duckdb_p.cpp"
