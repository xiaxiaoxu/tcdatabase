import pymysql.cursors
import datetime
import json


class TcDatabase:
    """ Python3基于pymysql封装常用操作 """

    connected = False
    __conn = None

    # 构造函数，初始化时直接连接数据库
    def __init__(self, host, port, user, password, database, charset='utf8'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        try:
            self.__conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=self.database,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor)
            self.connected = True
        except pymysql.Error as e:
            print('数据库连接失败:', end='')

    # 插入数据到数据表
    def insert(self, table, val_obj):
        if val_obj and type(val_obj) is not dict:
            try:
                val_obj = json.loads(val_obj)
            except:
                return ("val_obj is not dict and cannot parsed to dict")
        else:
            return ("no content argument to insert into table")
        sql_top = 'INSERT INTO ' + table + ' ('
        sql_tail = ') VALUES ('
        try:
            for key, val in val_obj.items():
                sql_top += key + ','
                sql_tail += "'%s'" % val + ','
            sql = sql_top[:-1] + sql_tail[:-1] + ')'
            print("sql: %s" % sql)
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return True
        except pymysql.Error as e:
            self.__conn.rollback()
            print("error in insert function: %s" % e)
            return False

    # 更新数据到数据表
    def update(self, table, val_obj, condition_str):
        if val_obj and type(val_obj) is not dict:
            try:
                val_obj = json.loads(val_obj)
            except:
                return ("val_obj is not dict and cannot parsed to dict")
        else:
            return ("no content argument to insert into table")
        sql = 'UPDATE ' + table + ' SET '
        try:
            for key, val in val_obj.items():
                sql += key + '=' + '"%s"'%val + ','
            sql = sql[:-1] + ' WHERE ' + condition_str
            print("sql: %s"% sql)
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            print("error in update: %s" % e)
            self.__conn.rollback()
            return False

    # 删除数据在数据表中
    def delete(self, table, condition_str):
        sql = 'DELETE FROM ' + table + ' WHERE ' + condition_str
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            self.__conn.rollback()
            return False

    # 查询唯一数据在数据表中，返回dict格式数据
    def select_one(self, table, condition_str="", field='*'):
        condition_str = (' WHERE ' + condition_str) if condition_str else ""
        sql = 'SELECT ' + field + ' FROM ' + table + condition_str
        print("sql: %s" % sql)
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.fetchall()[0]
        except pymysql.Error as e:
            print("error: %s" % e)
            return e

    # 查询表中多条数据，返回数据是list格式，list中每条数据是dict格式
    def select_all(self, table, condition_str, field='*'):
        condition_str = (' WHERE ' + condition_str) if condition_str else ""
        sql = 'SELECT ' + field + ' FROM ' + table + condition_str
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.fetchall()
        except pymysql.Error as e:
            return False

    # 统计某表某条件下的总行数
    def count(self, table, condition_str='1'):
        sql = 'SELECT count(*)res FROM ' + table + ' WHERE ' + condition_str
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.fetchall()[0]['res']
        except pymysql.Error as e:
            return False

    # 统计某字段（或字段计算公式）的合计值
    def sum(self, table, field, condition_str='1'):
        sql = 'SELECT SUM(' + field + ') AS res FROM ' + table + ' WHERE ' + condition_str
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.fetchall()[0]['res']
        except pymysql.Error as e:
            return False

    # 执行sql语句并返回执行结果
    def execute_sql(self, sql):
        if sql.strip():
            try:
                with self.__conn.cursor() as cursor:
                    cursor.execute(sql)
                self.__conn.commit()
                return cursor.fetchall()
            except pymysql.Error as e:
                return False
        else:
            return "sql is null"

    # 断言sql语句查询结果是否可以查到数据，可以返回yes，否則返回no
    def mysql_assert(self, sql):
        pass

    # 获取一条数据中某个字段的值
    def get_column_from_result(self):
        pass

    # 销毁对象时关闭数据库连接
    def __del__(self):
        """
        __conn实例对象被释放是调用此方法，用于关闭cursor和connection连接
        :return:
        """
        try:
            self.__conn.close()
        except pymysql.Error as e:
            pass

    # 关闭数据库连接
    def close(self):
        self.__del__()


if __name__ == '__main__':
    # ['host', 'port', 'user', 'pw', 'db']
    host, port, user, password, database, = '127.0.0.1', 3306, 'root', 'root', 'testdatabase'
    tcdatabase = TcDatabase(host, port, user, password, database)
    # print(dir(tcdatabase))
    sql = "select * from auto_test_testcase;"
    # print(tcdatabase.execute_sql(sql))
    # print(tcdatabase.select_one("auto_test_testcase", "author='吴老师'"))
    # print(tcdatabase.select_all("auto_test_testcase","author='吴老师'"))
    val_obj = """{"id": 1, "name": "\\u641c\\u8d85\\u4eba\\u7535\\u5f71", "author": "\\u5434\\u8001\\u5e08",
               "create_time": "2020-07-02 02:42:57.602000", "update_time": "2020-07-04 02:42:57.602000",
               "belong_module_id": 1, "belong_project_id": 2, "user_id": 2}"""
    # print(tcdatabase.insert("auto_test_testcase", val_obj))
    print(tcdatabase.update("auto_test_testcase", val_obj, "id=1"))
