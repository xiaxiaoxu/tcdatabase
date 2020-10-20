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
    connected = False

    def __init__(self, host=""):
        self.host = host
        self.client = KafkaClient(hosts=self.host)
        self.connected = True

    # 获取kafka实例节点
    def get_brokers(self):
        try:
            return self.client.brokers.values()
        except Exception as e:
            print("error occurs in get_brokers, error: %s" % e)

    # 获取client的topic
    def get_topics(self):
        return self.client.topics

    # 创建topic，场景较少
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
                    return False
                elif re.search("7", str(e)):
                    print("创建完成")
                    return True
                elif re.search("36", str(e)):
                    print("topic 已存在")
                    return True
                else:
                    raise e

    # 获取topic的分区
    def get_topic_partitions(self, topicName):
        topic = self.get_topics()[topicName]
        try:
            partitions = topic.partitions
            print("partitions of topic %s : %s" % (topicName, partitions))
            return partitions
        except Exception as e:
            print("error occurs in get _topic_partition, error: %s" % e)

    # 获取topic的可用偏移量
    def get_topic_available_offset(self, topicName):
        topic = self.get_topics()[topicName]
        try:
            earliest_offset = topic.earliest_available_offsets()
            print("获取最早可用的offset %s" % earliest_offset)
            return earliest_offset
        except Exception as e:
            print("error occurs in get_topic_available_offset, error: %s" % e)

    # 生产者发送消息
    def produce_message(self, topicName, message):

        topic = self.get_topics()[topicName]
        try:
            p = topic.get_producer(sync=True)
            print("message to produce: '%s'" % message)
            print("start to procude message...")
            p.produce(message.encode())
            print("procude message successfully.")
            return True
        except Exception as e:
            print("error occurs in procude_message, error: %s" % e)
            return False


    def produce_message_to_partition(self, topic, message, partision_num=0):
        """
        往指定分区号写消息，如果要控制打印到某个分区，
        需要在获取生产者的时候指定选区函数，
        并且在生产消息的时候额外指定一个key
        ：topic：
        ：partision_num：分区号
        :return:
        """

        def assign_patition(pid, key):
            """
            选区函数，指定特定分区, 这里测试写入第一个分区(id=0)
            :param pid: 为分区列表
            :param key:
            :return:
            """
            print("为消息分配partition {} {}".format(pid, key))
            return pid[int(partision_num)]

        topic = self.client.topics[topic.encode()]
        p = topic.get_producer(sync=True, partitioner=assign_patition)
        print("message to produce: %s" % message)
        try:
            p.produce(message.encode(), partition_key=b"partition_key_0")
            return True
        except Exception as e:
            print("error occurs in procude_message_to_partition, error: %s" % e)
            return False
    #
    # def async_produce_message(self, topic):
    #     """
    #     异步生产消息，消息会被推到一个队列里面，
    #     另外一个线程会在队列中消息大小满足一个阈值（min_queued_messages）
    #     或到达一段时间（linger_ms）后统一发送,默认5s
    #     :return:
    #     """
    #     topic = self.client.topics[topic.encode()]
    #     last_offset = topic.latest_available_offsets()
    #     print("最近的偏移量 offset {}".format(last_offset))
    #
    #     # 记录最初的偏移量
    #     old_offset = last_offset[0].offset[0]
    #     p = topic.get_producer(sync=False, partitioner=lambda pid, key: pid[0])
    #     p.produce(str(time.time()).encode())
    #     s_time = time.time()
    #     while True:
    #         last_offset = topic.latest_available_offsets()
    #         print("最近可用offset {}".format(last_offset))
    #         if last_offset[0].offset[0] != old_offset:
    #             e_time = time.time()
    #             print('cost time {}'.format(e_time - s_time))
    #             break
    #         time.sleep(1)

    # def get_produce_message_report(self, topicName):
    #     '''
    #         pykafka消费者分为simple和balanced两种:
    #         查看异步发送消报告,默认会等待5s后才能获得报告
    #     :param topic:
    #     :return:
    #     '''
    #     topic = self.client.topics[topicName.encode()]
    #     last_offset = topic.latest_available_offsets()
    #     print("最近的偏移量 offset {}".format(last_offset))
    #     p = topic.get_producer(sync=False, delivery_reports=True, partitioner=lambda pid, key: pid[0])
    #     p.produce(str(time.time()).encode())
    #     s_time = time.time()
    #     delivery_report = p.get_delivery_report()
    #     e_time = time.time()
    #     print('等待{}s, 递交报告{}'.format(e_time - s_time, delivery_report))
    #     last_offset = topic.latest_available_offsets()
    #     print("最近的偏移量 offset {}".format(last_offset))

    def consume_last_message_as_simple(self, topicName, partitionNum=0):
        """
        以simple的方式消费分区中最后新的一条消息
        :param topicName:
        :param partitionNum: 分区号，默认为0
        :return:
        """
        topic = self.get_topics()[topicName]
        partitions = self.get_topic_partitions(topicName)
        partition_to_consume = partitions[int(partitionNum)]
        print("指定的partition为： %s" % partition_to_consume)
        # 根据分区号获取偏移量信息
        partition_offset = topic.latest_available_offsets()[int(partitionNum)]
        print("partition_offset: {}".format(partition_offset))
        last_offset = topic.latest_available_offsets()[int(partitionNum)][0][0]
        print("分区下最新偏移量位置：:%s" % last_offset)
        # 根据指定分区获取消费对象consumer
        consumer = topic.get_simple_consumer("simple_consumer_group", partitions=[partition_to_consume])  # 选择一个分区进行消费
        offset = consumer.held_offsets  # 当前消费者分区offet位置
        print("消费者分区当前offset位置{}".format(offset))  # 消费者拥有的分区offset的情况
        # 设置分区和offset为最后一个位置，需注意这里的最后一个消息的offset应为last_offset的值-2，实际测试得出的结论
        consumer.reset_offsets([(partition_to_consume, int(last_offset) - 2)])  # 设置offset为最后一个位置
        offset = consumer.held_offsets  # 重置offset后的消费者分区offet位置
        print("重置offset后消费者分区offset位置{}".format(offset))
        msgList = []
        # 从指定偏移位置进行消费消息，直到最后
        try:
            msg = consumer.consume()
            print("消费的消息是 :{}".format(msg.value.decode()))
            offset = consumer.held_offsets
            print("{}, 当前消费者分区offset位置{}".format(msg.value.decode(), offset))
            msgList.append(msg.value.decode())
            return msgList
        except Exception as e:
            print("error occurs in consume_from_offset_as_simple, error: %s" % e)


    def consume_from_offset_as_simple(self, topicName, partitionNum=0, offsetNum=0):
        '''
            以simple方式在指定分区中从指定偏移位置消费消息
            pykafka消费者分为simple和balanced两种:
            simple适用于需要消费指定分区且不需要自动重分配(自定义)
            balanced则自动分配分区
        :param topic:
        :param partitionNum：分区号，默认为0
        :param offet：消息在分区中的偏移量
        :return:
        '''
        topic = self.get_topics()[topicName]
        partitions = self.get_topic_partitions(topicName)
        partition_to_consume = partitions[int(partitionNum)]
        print("指定的partition为： %s" % partition_to_consume)
        # 根据分区号获取偏移量信息
        last_offset = topic.latest_available_offsets()[int(partitionNum)][0][0]
        print("分区下最新偏移量位置：:%s" % last_offset)
        # 根据指定分区获取消费对象consumer
        consumer = topic.get_simple_consumer("simple_consumer_group", partitions=[partition_to_consume])  # 选择一个分区进行消费
        offset = consumer.held_offsets  # 当前消费者分区offet位置
        print("当前消费者分区offset位置{}".format(offset))  # 消费者拥有的分区offset的情况
        # 指定分区
        consumer.reset_offsets([(partition_to_consume, offsetNum)])  # 设置offset
        offset = consumer.held_offsets  # 重置offset后的消费者分区offet位置
        print("重置偏移后当前消费者分区offset情况{}".format(offset))
        # 从指定偏移位置进行消费消息，直到最后
        try:
            # 获得指定偏移量往后所有消息的数量
            msgLen = int(last_offset) - int(offsetNum)
            print("msgLen: %s" % msgLen)
            for i in range(msgLen - 1):
                print("i: %s" % i)
                msg = consumer.consume()
                print("消费的消息是 :{}".format(msg.value.decode()))
                offset = consumer.held_offsets
                print("{}, 当前消费者分区offset位置{}".format(msg.value.decode(), offset))
            return True
        except Exception as e:
            print("error occurs in consume_from_offset_as_simple, error: %s" % e)
            return False

    def consume_n_messages_as_balance(self, topicName, num=0):
        """

        :param topicName:
        :param num: 消费次数，默认为1
        :return:
        """
        topic = self.get_topics()[topicName]
        # managed=True 设置后，使用新式reblance分区方法，不需要使用zk，而False是通过zk来实现reblance的需要使用zk
        consumer = topic.get_balanced_consumer(b"consumer_group_balanced", managed=True)
        partitions = topic.partitions
        print("分区 {}".format(partitions))
        earliest_offsets = topic.earliest_available_offsets()
        print("最早可用offset {}".format(earliest_offsets))
        last_offset = topic.latest_available_offsets()
        print("最近可用offset {}".format(last_offset))
        print("最新位移数：%s" % last_offset[0][0][0])
        offset = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset))
        latest_available_offset = topic.latest_available_offsets()[0][0][0]
        print("latest_available_offset: %s" % latest_available_offset)
        current_offset = consumer.held_offsets[0]  # 因为helo_offsets的值是字典{0: 0}，所以取key为0的值就是当前位移
        print("current_offset: %s" % current_offset)
        msgList = []
        for i in range(int(num)):
            if current_offset + 2 <= latest_available_offset:
                try:
                    msg = consumer.consume()
                    print("消费的消息是 :{}".format(msg.value.decode()))
                    offset = consumer.held_offsets
                    print("{}, 当前消费者分区offset情况{}".format(msg.value.decode(), offset))
                    msgList.append(msg.value.decode())
                except Exception as e:
                    print("error occurs in consume_as_balance, error: {} ".format(e))
        return msgList

    def consume_one_message_as_balance(self, topicName):
        """
                使用balance consumer去消费kafka，因为分区是自动指定，无法指定偏移量进行消费
                :return:
                """
        topic = self.get_topics()[topicName]
        # managed=True 设置后，使用新式reblance分区方法，不需要使用zk，而False是通过zk来实现reblance的需要使用zk
        consumer = topic.get_balanced_consumer(b"consumer_group_balanced", managed=True)
        partitions = topic.partitions
        print("分区 {}".format(partitions))
        earliest_offsets = topic.earliest_available_offsets()
        print("最早可用offset {}".format(earliest_offsets))
        last_offset = topic.latest_available_offsets()
        print("最近可用offset {}".format(last_offset))
        print("最新位移数：%s" % last_offset[0][0][0])
        offset = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset))
        latest_available_offset = topic.latest_available_offsets()[0][0][0]
        print("latest_available_offset: %s" % latest_available_offset)
        current_offset = consumer.held_offsets[0]  # 因为helo_offsets的值是字典{0: 0}，所以取key为0的值就是当前位移
        print("current_offset: %s" % current_offset)
        msgList = []
        if current_offset + 2 <= latest_available_offset:
            try:
                msg = consumer.consume()
                print("消费的消息是 :{}".format(msg.value.decode()))
                offset = consumer.held_offsets
                print("{}, 当前消费者分区offset情况{}".format(msg.value.decode(), offset))
                msgList.append(msg.value.decode())
                return msgList
            except Exception as e:
                print("error occurs in consume_as_balance, error: {} ".format(e))

    def consume_all_message_as_balance(self, topicName):
        """
        使用balance consumer去消费kafka，因为分区是自动指定，无法指定偏移量进行消费，只能消费所有或消费指定次数
        :return:
        """
        topic = self.get_topics()[topicName]
        # managed=True 设置后，使用新式reblance分区方法，不需要使用zk，而False是通过zk来实现reblance的需要使用zk
        consumer = topic.get_balanced_consumer(b"consumer_group_balanced", managed=True)
        partitions = topic.partitions
        print("分区 {}".format(partitions))
        earliest_offsets = topic.earliest_available_offsets()
        print("最早可用offset {}".format(earliest_offsets))
        last_offset = topic.latest_available_offsets()
        print("最近可用offset {}".format(last_offset))
        latest_available_offset = last_offset[0][0][0]
        print("最近位移数: %s" % latest_available_offset)
        offset = consumer.held_offsets
        print("当前消费者分区offset情况{}".format(offset))
        msgList = []
        while True:
            current_offset = consumer.held_offsets[0]  # 因为helo_offsets的值是字典{0: 0}，所以取key为0的值就是当前位移
            print("current_offset: %s" % current_offset)
            if current_offset + 2 <= latest_available_offset:
                try:
                    msg = consumer.consume()
                    print("消费的消息是 :{}".format(msg.value.decode()))
                    offset = consumer.held_offsets
                    print("{}, 当前消费者分区offset情况{}".format(msg.value.decode(), offset))
                    msgList.append(msg.value.decode())
                except Exception as e:
                    print("error occurs in consume_as_balance, error: {} ".format(e))
            else:
                break
        return msgList


if __name__ == '__main__':
    # host = "172.17.9.151:9092"
    host = "172.17.9.155:9092"
    kfkInstance = TcKafka(host)
    topic = 'xiaxxtopic122'
    # 测试获取所有topic
    print(kfkInstance.get_topics())
    # 测试创建topic
    result = kfkInstance.create_topic(topic)
    print("result: %s" % result)
    # # 测试获取topic的分区
    # kfkInstance.get_topic_partitions(topic)
    # # 测试查看topic可用的偏移量
    # kfkInstance.get_topic_available_offset(topic)
    # # 测试生产者发送消息
    # endStr = time.strftime("%Y-%m-%d-%H-%M")
    # kfkInstance.procude_message(topic, "test message%s" % endStr)
    # # 测试指定分区写消息
    # kfkInstance.procude_message_to_partition(topic, "test message1738", 0)
    # # 以simple方式从0号分区的某个位移开始消费消息
    # kfkInstance.consume_from_offset_as_simple(topic,0,29)
    # # 以simple方式消费分区号为0的最后一条消息
    # kfkInstance.consume_last_message_as_simple(topic,0)
    # # 测试以balance方式消费消息
    # kfkInstance.consume_all_message_as_balance(topic)
    # # 以balance方式消费一次消条
    # kfkInstance.consume_one_message_as_balance(topic)
    # # 测试以balance方式消费n条消息
    # kfkInstance.consume_n_messages_as_balance(topic, 30)

