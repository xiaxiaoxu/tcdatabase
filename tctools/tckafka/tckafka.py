# coding="utf-8"
import time
import re
from pykafka import KafkaClient


class Topic():
    def __init__(self, topic_name, num_partitions=1, replication_factor=1, replica_assignment=[], config_entries=[]):
        """
        :param topic_name:
        :param num_partitions:分区数
        :param replication_factor: 副本数
        :param replica_assignment: [(partition, replicas)]
        :param config_entries: [(config_name, config_value)]
        """
        self.topic_name = topic_name.encode()
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignment = replica_assignment
        self.config_entries = config_entries


class TcKafka(object):
    """
    测试kafka常用api
    测试topic：xiaxxtopic、xiaxxtopic1
    """

    def __init__(self, host=""):
        self.host = host
        self.client = KafkaClient(hosts=self.host)

    # 获取kafka实例节点
    def get_brokers(self):
        try:
            return self.client.brokers.values()
        except Exception as e:
            print("error occurs in get_brokers, error: %s" % e)

    # 获取client的topic
    def get_topics(self):
        return self.client.topics

    # 场景较少
    def create_topic(self, topicName):
        brokers = self.get_brokers()
        for broker in brokers:
            try:
                broker.create_topics(topic_reqs=(Topic(topicName),))
            # 创建结果会抛出异常，如果异常中的数字是7，表示创建成功
            except Exception as e:
                print("error: %s" % e)
                if re.search("41", str(e)):
                    print("该broker 不是 leader，交由下一个broker创建")
                elif re.search("7", str(e)):
                    print("创建完成")
                    break
                elif re.search("36", str(e)):
                    print("topic 已存在")
                    break
                else:
                    raise e

    def get_topic_partitions(self, topicName):
        topic = self.get_topics()[topicName]
        try:
            partitions = topic.partitions
            print("partitions of topic %s : %s" % (topicName, partitions))
            return partitions
        except Exception as e:
            print("error occurs in get _topic_partition, error: %s" % e)

    def get_topic_available_offset(self,topicName):
        topic = self.get_topics()[topicName]
        try:
            earliest_offset = topic.earliest_available_offsets()
            print("获取最早可用的offset %s" % earliest_offset)
            return earliest_offset
        except Exception as e:
            print("error occurs in get_topic_available_offset, error: %s" % e)

    # 生产者发送消息
    def procude_message(self,topicName, message):
        topic = self.get_topics()[topicName]
        try:
            p = topic.get_producer(sync=True)
            print("message: %s" % message)
            print("start to procude message...")
            p.produce(message.encode())
            print("procude message successfully.")
        except Exception as e:
            print("error occurs in procude_message, error: %s" % e)


    def get_producer_partition(self, topicName):
        """
        生产者分区查看，主要查看生产消息时offset的变化
        :return:
        """
        topic = self.get_topics()[topicName]
        partitions = topic.partitions
        print(u"查看所有分区 {}".format(partitions))

        earliest_offset = topic.earliest_available_offsets()
        print(u"获取最早可用的offset {}".format(earliest_offset))

        # 生产消息之前看看offset
        last_offset = topic.latest_available_offsets()
        print(u"最近可用offset {}".format(last_offset))

        # 生产者发送消息
        p = topic.get_producer(sync=True)
        p.produce(str(time.time()).encode())

        # 查看offset的变化
        last_offset = topic.latest_available_offsets()
        print(u"最近可用offset {}".format(last_offset))

    def producer_designated_partition(self, topic):
        """
        往指定分区写消息，如果要控制打印到某个分区，
        需要在获取生产者的时候指定选区函数，
        并且在生产消息的时候额外指定一个key
        :return:
        """

        def assign_patition(pid, key):
            """
            指定特定分区, 这里测试写入第一个分区(id=0)
            :param pid: 为分区列表
            :param key:
            :return:
            """
            print("为消息分配partition {} {}".format(pid, key))
            return pid[0]

        topic = self.client.topics[topic.encode()]
        p = topic.get_producer(sync=True, partitioner=assign_patition)
        p.produce(str(time.time()).encode(), partition_key=b"partition_key_0")

    def async_produce_message(self, topic):
        """
        异步生产消息，消息会被推到一个队列里面，
        另外一个线程会在队列中消息大小满足一个阈值（min_queued_messages）
        或到达一段时间（linger_ms）后统一发送,默认5s
        :return:
        """
        topic = self.client.topics[topic.encode()]
        last_offset = topic.latest_available_offsets()
        print("最近的偏移量 offset {}".format(last_offset))

        # 记录最初的偏移量
        old_offset = last_offset[0].offset[0]
        p = topic.get_producer(sync=False, partitioner=lambda pid, key: pid[0])
        p.produce(str(time.time()).encode())
        s_time = time.time()
        while True:
            last_offset = topic.latest_available_offsets()
            print("最近可用offset {}".format(last_offset))
            if last_offset[0].offset[0] != old_offset:
                e_time = time.time()
                print('cost time {}'.format(e_time - s_time))
                break
            time.sleep(1)


    def get_produce_message_report(self, topicName):
        '''
            pykafka消费者分为simple和balanced两种:
            查看异步发送消报告,默认会等待5s后才能获得报告
        :param topic:
        :return:
        '''
        topic = self.client.topics[topicName.encode()]
        last_offset = topic.latest_available_offsets()
        print("最近的偏移量 offset {}".format(last_offset))
        p = topic.get_producer(sync=False, delivery_reports=True, partitioner=lambda pid, key: pid[0])
        p.produce(str(time.time()).encode())
        s_time = time.time()
        delivery_report = p.get_delivery_report()
        e_time = time.time()
        print('等待{}s, 递交报告{}'.format(e_time - s_time, delivery_report))
        last_offset = topic.latest_available_offsets()
        print("最近的偏移量 offset {}".format(last_offset))

    def consume_as_simple(self, topicName, offset=0):
        '''
            pykafka消费者分为simple和balanced两种:
            simple适用于需要消费指定分区且不需要自动的重分配(自定义)
            balanced自动分配则选择
            查看异步发送消报告,默认会等待5s后才能获得报告
        :param topic:
        :return:
        '''
        topic = self.topic = self.get_topics()[topicName]
        # topic = self.client.topics[topic.encode()]
        # partitions = topic.partitions
        last_offset = topic.latest_available_offsets()
        print("最近可用offset {}".format(last_offset))  # 查看所有分区
        partitions = self.get_topic_partitions(topicName)
        consumer = topic.get_simple_consumer("simple_consumer_group", partitions=[partitions[0]])  # 选择一个分区进行消费
        offset_list = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset_list))  # 消费者拥有的分区offset的情况
        consumer.reset_offsets([(partitions[0], offset)])  # 设置offset
        msg = consumer.consume()
        print("消费 :{}".format(msg.value.decode()))
        msg = consumer.consume()
        print("消费 :{}".format(msg.value.decode()))
        msg = consumer.consume()
        print("消费 :{}".format(msg.value.decode()))
        offset = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset))  # 3

    def consume_as_balance(self, topicName, offset=0):
        """
        使用balance consumer去消费kafka
        :return:
        """
        topic = self.get_topics()[topicName]
        # managed=True 设置后，使用新式reblance分区方法，不需要使用zk，而False是通过zk来实现reblance的需要使用zk
        consumer = topic.get_balanced_consumer("consumer_group_balanced", managed=True)
        partitions = topic.partitions
        print("分区 {}".format(partitions))
        earliest_offsets = topic.earliest_available_offsets()
        print("最早可用offset {}".format(earliest_offsets))
        last_offsets = topic.latest_available_offsets()
        print("最近可用offset {}".format(last_offsets))
        print("消息数量：%s"% last_offsets[0].offset[0])
        # 指定offset偏移量
        consumer.reset_offsets([(partitions[0], offset)])  # 设置offset
        offset = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset))
        # 从指定偏移位置进行消费消息，直到最后
        try:
            # 获得指定偏移量往后所有消息的数量
            msgLen = last_offsets[0].offset[0] - offset[0]
            print("msgLen: %s" % msgLen)
            for i in range(msgLen - 1):
                print("i: %s" % i)
                msg = consumer.consume()
                offset = consumer.held_offsets
                print("{}, 当前消费者分区offset情况{}".format(msg.value.decode(), offset))
        except Exception as e:
            print("error occurs in consume_as_balance, error: %s" % e)

if __name__ == '__main__':
    host = "172.17.9.151:9092"
    kfkInstance = TcKafka(host)
    topic = 'xiaxxtopic6'
    # 测试获取所有topic
    # print(kfkInstance.get_topics())
    # 测试创建topic
    # kfkInstance.create_topic(topic)
    # 测试获取topic的分区
    # kfkInstance.get_topic_partitions(topic)
    # 测试查看topic可用的偏移量
    # kfkInstance.get_topic_available_offset(topic)
    # 测试生产者发送消息
    kfkInstance.procude_message(topic, "test message xiaxx.")
    # 测试simple consume
    # kfkInstance.consume_as_simple(topic)
    # 测试balance consume
    kfkInstance.consume_as_balance(topic,3)
    # kfkInstance.get_producer_partitions(topic)
    # kafka_ins.producer_partition(topic)
    # kafka_ins.producer_designated_partition(topic)
    # kafka_ins.async_produce_message(topic)
    # kafkaInstance.get_produce_message_report(topic)
    # kafkaInstance.producer_partition(topic)
    # kfkInstance.async_produce_message(topic)


