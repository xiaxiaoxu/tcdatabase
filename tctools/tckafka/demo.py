#encoding=utf-8
from tctools.tckafka.tckafka import TcKafka
import time
import random

host = "172.17.9.151:9092"
topic = 'xiaxxtopic6'
tckafka = TcKafka(host)

def test_connect():
    assert tckafka.connected == True, 'failed'

def test_get_brokers():
    brokers = tckafka.get_brokers()
    assert len(brokers) >= 1, "failed"

def test_get_topics():
    topics = tckafka.get_topics()
    assert len(topics) >= 1, "failed"

def test_create_topic(topic):
    result = tckafka.create_topic(topic)
    # 创建成功或topic已存在都返回True
    assert result == True, "failed in create_topic"
    time.sleep(3)
    # 需要重新连接kafka获取最新的topic
    tckafka1 = TcKafka(host)
    # print("tckafka.get_topics().keys(): {}".format(tckafka1.get_topics().keys()))
    # print("topic.encode(): %s" % topic.encode())
    assert topic.encode() in tckafka1.get_topics().keys(), "failed in create_topic"

def test_get_topic_partitions():
    partitions = tckafka.get_topic_partitions(topic)
    assert type(partitions) == dict, "failed in get_topic_partitions"

# 测试查看topic可用的偏移量
def test_get_topic_available_offset():
    available_offset = tckafka.get_topic_available_offset(topic)
    print(available_offset[0][0][0])
    assert available_offset[0][0][0] >= 0, "get_topic_available_offset"

# 测试生产者发送消息
def test_produce_message():
    endStr = time.strftime("%Y-%m-%d-%H-%M")
    message = "test message%s" % endStr
    result = tckafka.produce_message(topic, message)
    assert result == True, "failed in procude_message"

# 测试指定分区写消息
def test_produce_message_to_partition():
    endStr = time.strftime("%Y-%m-%d-%H-%M")
    message = "test message%s" % endStr
    result = tckafka.produce_message_to_partition(topic, message, 0)
    assert result == True, "failed in test_procude_message_to_partition"

# 测试以simple方式从指定分区的某个位移开始消费消息
def test_consume_from_offset_as_simple():
    partition = 0
    offset = 2
    result = tckafka.consume_from_offset_as_simple(topic, partition, offset)
    assert result == True, "failed in test_consume_from_offset_as_simple"

# 以simple方式消费分区号为0的最后一条消息
def test_consume_last_message_as_simple():
    partition = 0
    endStr = time.strftime("%Y-%m-%d-%H-%M")
    message = "test message%s" % endStr
    tckafka.produce_message(topic, message)
    time.sleep(1)
    msgList = tckafka.consume_last_message_as_simple(topic, partition)
    print("msgList: %s" % msgList)
    assert message == msgList[0], "failed in test_consume_last_message_as_simple"

# 测试以balance方式消费消息
def test_consume_all_message_as_balance():
    endStr = time.strftime("%Y-%m-%d-%H-%M")
    message = "test message%s" % endStr
    tckafka.produce_message(topic, message)
    time.sleep(1)
    msgList = tckafka.consume_all_message_as_balance(topic)
    print("msgList: %s" % msgList)
    assert message in msgList, "failed in test_consume_all_message_as_balance"

# 以balance方式消费一次消条
def test_consume_one_message_as_balance():
    msgList = tckafka.consume_one_message_as_balance(topic)
    print("msgList: %s" % msgList)
    assert len(msgList) >= 1, "failed in test_consume_one_message_as_balance"

# 测试以balance方式消费n条消息
def test_consume_n_messages_as_balance():
    num = 10
    msgList = tckafka.consume_n_messages_as_balance(topic, num)
    print("msgList: %s" % msgList)
    assert len(msgList) == num, "failed in test_consume_n_messages_as_balance"


if __name__ == '__main__':
    # 测试连接
    test_connect()
    # 测试获取kafka实例节点信息
    test_get_brokers()
    # 测试获取所有topic
    test_get_topics()
    # 测试创建topic
    topicNew = 'xiaxxtopic'+ str(random.randrange(100))
    test_create_topic(topicNew)
    # 测试获取topic的分区信息
    test_get_topic_partitions()
    # 测试获取topic最早可用的offset
    test_get_topic_available_offset()
    # 测试生产者发送消息
    test_produce_message()
    # 测试指定分区写消息
    test_produce_message_to_partition()
    # 测试以simple方式从0号分区的某个位移开始消费消息
    test_consume_from_offset_as_simple()
    # 以simple方式消费分区号为0的最后一条消息
    test_consume_last_message_as_simple()
    # 测试以balance方式消费消息
    test_consume_all_message_as_balance()
    # 以balance方式消费一次消条
    test_consume_one_message_as_balance()
    # 测试以balance方式消费n条消息
    test_consume_n_messages_as_balance()






