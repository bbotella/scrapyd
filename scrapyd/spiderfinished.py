import sqlite3
from scrapyd.sqlite import JsonSqliteDict, JsonSqliteList, SqliteList


class SqliteSpiderFinished(JsonSqliteDict):

    def __init__(self, database=None, table="spider_finished"):
        self.database = database or ':memory:'
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (key text primary key, value blob)" \
            % table
        self.conn.execute(q)
