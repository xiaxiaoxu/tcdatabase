#encoding=utf-8
from tctools.tckafka.tckafka import TcKafka



if __name__ == '__main__':
    host = "172.17.9.151:9092"
    kfkInstance = TcKafka(host)
    topic = 'xiaxxtopic6'
    #测试获取所有topic
    print(kfkInstance.get_topics())
    #测试创建topic
    kfkInstance.create_topic(topic)
    #测试获取topic的分区
    kfkInstance.get_topic_partitions(topic)
    #测试查看topic可用的偏移量
    kfkInstance.get_topic_available_offset(topic)
    #测试生产者发送消息
    kfkInstance.procude_message(topic, "test message xiaxx.")
    # 测试simple consume
    kfkInstance.consume_as_simple(topic)
    # 测试balance consume
    kfkInstance.consume_as_balance(topic, 3)
