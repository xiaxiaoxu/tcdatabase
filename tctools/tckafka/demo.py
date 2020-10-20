#encoding=utf-8
from tctools.tckafka.tckafka import TcKafka
import time


if __name__ == '__main__':
    # host = "172.17.9.151:9092"
    host = "172.17.9.155:9092"
    kfkInstance = TcKafka(host)
    topic = 'xiaxxtopic6'
    # 获取kafka实例节点信息
    kfkInstance.get_brokers()
    # 测试获取所有topic
    print(kfkInstance.get_topics())
    # 测试创建topic
    kfkInstance.create_topic(topic)
    # 测试获取topic的分区
    kfkInstance.get_topic_partitions(topic)
    # 测试查看topic可用的偏移量
    kfkInstance.get_topic_available_offset(topic)
    # 测试生产者发送消息
    endStr = time.strftime("%Y-%m-%d-%H-%M")
    kfkInstance.procude_message(topic, "test message%s" % endStr)
    # 测试指定分区写消息
    kfkInstance.procude_message_to_partition(topic, "test message1738", 0)
    # 以simple方式从0号分区的某个位移开始消费消息
    kfkInstance.consume_from_offset_as_simple(topic, 0, 29)
    # 以simple方式消费分区号为0的最后一条消息
    kfkInstance.consume_last_message_as_simple(topic, 0)
    # 测试以balance方式消费消息
    kfkInstance.consume_all_message_as_balance(topic)
    # 以balance方式消费一次消条
    kfkInstance.consume_one_message_as_balance(topic)
    # 测试以balance方式消费n条消息
    kfkInstance.consume_n_messages_as_balance(topic, 30)
