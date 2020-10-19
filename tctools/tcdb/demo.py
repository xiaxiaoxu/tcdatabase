#encoding='utf-8'

from tctools.tcdb.tcdb import TcDatabase

host, port, user, password, database, = '127.0.0.1', 3306, 'root', 'root', 'testdatabase'
num = 1

def test_connect():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    assert tcdb.connected == True, 'failed'

def test_execute_sql():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    sql = "select * from auto_test_testsuitexecuterecord;"
    result = tcdb.execute_sql(sql)
    print("result in test_execute_sql: %s\n" % result)
    assert type(result) == list
    assert len(result) >= 1 , 'failed in test_execute_sql'

def test_select_one():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.select_one("auto_test_testsuitexecuterecord", "creator='admin'")
    print("result in test_select_one: %s\n" % result)
    assert type(result) == dict , 'failed in test_select_one'
    assert result['creator'] == 'admin', 'failed in test_select_one'

def test_select_all():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.select_all("auto_test_testsuitexecuterecord","creator='admin'")
    print("result in test_select_all: %s\n" % result)
    assert len(result) >= 1

def test_get_field_value():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.get_field_value("auto_test_testsuitexecuterecord", 'creator', "creator='admin'")
    print("result in test_get_field_value: %s\n" % result)
    assert result == 'admin'

def test_insert():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    val_obj = """{"id": %s, "name": "\\u641c\\u8d85\\u4eba\\u7535\\u5f71", "author": "\\u5434\\u8001\\u5e08",
           "create_time": "2020-07-02 02:42:57.602000", "update_time": "2020-07-04 02:42:57.602000",
           "belong_module_id": 1, "belong_project_id": 2, "user_id": 2}""" % num
    result = tcdb.insert("auto_test_testcase", val_obj)
    print("result in test_insert: %s\n" % result)
    assert result == True

def test_update():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    val_obj = """{"id": 1, "name": "\\u641c\\u8d85\\u4eba\\u7535\\u5f71", "author": "\\u5434\\u8001\\u5e08",
               "create_time": "2020-07-02 02:42:57.602000", "update_time": "2020-07-04 02:42:57.602000",
               "belong_module_id": 1, "belong_project_id": 1, "user_id": 2}"""
    result = tcdb.update("auto_test_testcase", val_obj, "id=1")
    print("result in test_update: %s\n" % result)
    assert result >= 0

def test_count():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.count("auto_test_testcase", "id=16")
    print("result in test_count: %s\n" % result)
    assert result >= 1

def test_sum():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.sum("test_sum", "id", "belong_project_id=1")
    print("result in test_update: %s\n" % result)
    assert result >= 0

def test_msyql_result_assert():
    tcdb = TcDatabase()
    tcdb.connect(host, port, user, password, database)
    result = tcdb.mysql_result_assert("select * from auto_test_testcase;")
    print("result in test_msyql_result_assert: %s\n" % result)
    assert result == 'yes'


if __name__ == '__main__':
    test_connect()
    test_execute_sql()
    test_select_one()
    test_select_all()
    test_get_field_value()
    test_insert()
    test_update()
    test_count()
    test_sum()
    test_msyql_result_assert()
