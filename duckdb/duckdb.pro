TARGET = sqlduckdb

TEMPLATE = lib

QT_FOR_CONFIG += sqldrivers-private

CONFIG  += c++14 plugin

include($$PWD/duckdb/duckdb.pri)

# QT_INSTALL_PLUGINS=C:/Qt/5.15.16/mingw81_64/plugins
# target.path = $$[QT_INSTALL_PLUGINS]/sqldrivers/
# INSTALLS += target

HEADERS += $$PWD/qsql_duckdb_p.h
SOURCES += $$PWD/qsql_duckdb.cpp $$PWD/smain.cpp

OTHER_FILES += duckdb.json


LIBS += $$QT_LFLAGS_SQLITE
QMAKE_CXXFLAGS *= $$QT_CFLAGS_SQLITE


QT = core core-private sql-private

PLUGIN_CLASS_NAME = QDuckdbDriverPlugin

PLUGIN_TYPE = sqldrivers
load(qt_plugin)

DEFINES += QT_NO_CAST_TO_ASCII QT_NO_CAST_FROM_ASCII

QMAKE_CFLAGS += -march=native

DISTFILES += \
    ../readme.md
