import pymysql.cursors
import datetime
import json


class TcDatabase:
    """ Python3基于pymysql封装常用操作 """

    connected = False
    __conn = None

    # 构造函数
    def __init__(self):
        pass

    def connect(self, host, port, user, password, database, charset='utf8'):
        """
        连接数据库，参数需要host, port, user, password, database
        :param host: ip
        :param port: 端口
        :param user: 用户名
        :param password: 密码
        :param database: 数据库名
        :param charset: 字符集
        :return: 返回pymysql连接对象
        """
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

    def insert(self, table, val_obj):
        """
        插入数据
        :param table: 表名
        :param val_obj: 字典或json对象
        :return:
        """
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
        """

        :param table: 表名
        :param val_obj: 字典对象
        :param condition_str: 条件表达式如：id=1, name='test'
        :return:
        """
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
                sql += key + '=' + '"%s"' % val + ','
            sql = sql[:-1] + ' WHERE ' + condition_str
            print("sql: %s" % sql)
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

    def select_one(self, table, field='*', condition_str=""):
        """
        # 查询唯一数据在数据表中，返回dict格式数据
        :param table: 表名
        :param condition_str: 条件表达式，如id=1, name='test'
        :param field: 指定字段名称，多个以逗号分隔，默认是"*"
        :return:
        """
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

    #
    def select_all(self, table, field='*', condition_str=""):
        """
        查询表中多条数据，返回数据是list格式，list中每条数据是dict格式
        :param table: 表名
        :param field: 字段名称，逗号分隔，如：id, name, age
        :param condition_str: 条件表达式，如id=1, name='test'
        :return:
        """
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
    def count(self, table, condition_str):
        condition_str = (' WHERE ' + condition_str) if condition_str else ""
        sql = 'SELECT count(*)res FROM ' + table + condition_str
        print("sql: %s" % sql)
        try:
            with self.__conn.cursor() as cursor:
                cursor.execute(sql)
            self.__conn.commit()
            return cursor.fetchall()[0]['res']
        except pymysql.Error as e:
            print("error in count: %s" % e)
            return False

    # 统计某字段（或字段计算公式）的合计值
    def sum(self, table, field, condition_str):
        condition_str = (' WHERE ' + condition_str) if condition_str else ""
        sql = 'SELECT SUM(' + field + ') AS res FROM ' + table + condition_str
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
    def mysql_result_assert(self, sql):
        if sql.strip():
            try:
                res = self.execute_sql(sql)
                if res:
                    return "yes"
                else:
                    return "no"
            except pymysql.Error as e:
                print("error in result_assert: %s" % e)
        else:
            return "sql is None"

    def get_field_value(self, table, field="*", condition_str=""):
        result = self.select_one(table, field=field, condition_str=condition_str)
        # if type(result)
        print("result: %s" % result)

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
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    # print(dir(tctools))
    sql = "select * from auto_test_testcase;"
    # print(tcdb.execute_sql(sql))
    # print(tcdb.select_one("auto_test_testcase", "author='吴老师'",'id'))
    print(tcdb.get_field_value("auto_test_testcase", "author='吴老师'", ))
    # print(tcdb.select_all("auto_test_testcase","author='吴老师'"))
    val_obj = """{"id": 16, "name": "\\u641c\\u8d85\\u4eba\\u7535\\u5f71", "author": "\\u5434\\u8001\\u5e08",
               "create_time": "2020-07-02 02:42:57.602000", "update_time": "2020-07-04 02:42:57.602000",
               "belong_module_id": 1, "belong_project_id": 2, "user_id": 2}"""
    # print(tcdb.insert("auto_test_testcase", val_obj))
    # print(tcdb.update("auto_test_testcase", val_obj, "id=16"))
    # print(tcdb.count("auto_test_testcase", "id=16"))
    # print(tcdb.sum("auto_test_testcase", "id", "belong_project_id=1"))
    # print(tcdb.execute_sql("select * from auto_test_testcase;"))
    # print(tcdb.mysql_result_assert("select * from auto_test_testcase;"))
