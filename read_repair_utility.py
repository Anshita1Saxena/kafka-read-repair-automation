#!/usr/bin/python

"""
This program to take the messages from Kafka and do read repair for Scylla.
"""
__author__ = "Anshita Saxena"
__copyright__ = "(c) Copyright IBM 2020"
__contributors__ = "Benu Mohta, Meghnath Saha"
__credits__ = ["BAT DMS IBM Team"]
__email__ = "anshita333saxena@gmail.com"
__status__ = "Production"

# Import the required libraries
# Import the sys library to parse the arguments
import sys

# Import the parsing library
import configparser

# Import the json library
import json

# Import the logging library
import logging
import logging.config

# Import traceback library
import traceback

# Import the time library
import time

# Import requests for slack access
import requests

# Import the required concurrency libraries
from six.moves import queue

# Import the Scylla libraries
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

# Import the Kafka libraries
from kafka import KafkaConsumer
from kafka import TopicPartition

# Initialising the configparser object to parse the properties file
CONFIG = configparser.ConfigParser()
global failed_msg

# Set the logging criteria for the generated logs
LOGFILENAME = '/root/kafka-read-repair-automation/logs/read_repair_automation.log'
logging.config.fileConfig(fname='/root/kafka-read-repair-automation/conf/log_config.conf',
                          defaults={'logfilename': LOGFILENAME},
                          disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)


def set_env(p_app_config_file):
    """
    :param p_app_config_file:
    :return user, password, node, port, topic_name, kafka_servers, group_id,
    enable_auto_commit, auto_offset_reset, slack_token, slack_channel_name,
    control_connection_timeout, connect_timeout, max_partition_fetch_bytes,
    keyspace, concurency_level:
    """

    user = None
    password = None
    node = None
    port = None
    topic_name = None
    kafka_servers = None
    group_id = None
    enable_auto_commit = None
    auto_offset_reset = None
    slack_token = None
    slack_channel_name = None
    keyspace = None
    control_connection_timeout = None
    connect_timeout = None
    max_partition_fetch_bytes = None
    concurency_level = None
    try:
        # Reading configuration parameters from .ini file.
        # print(p_app_config_file)
        CONFIG.read(p_app_config_file)

        # Username for Scylla DB Database
        user = CONFIG['ApplicationParams']['user']
        user = str(user)
        # Password for Scylla DB Database
        password = CONFIG['ApplicationParams']['password']
        password = str(password)
        # Server IP addresses for Scylla DB Nodes
        nodes = CONFIG['ApplicationParams']['node']
        nodes = str(nodes)
        node = nodes.split(",")
        # Port for Scylla DB Connection
        port = CONFIG['ApplicationParams']['port']
        port = int(port)
        # Message captures from Kafka Topic Name for reading from DB Table
        topic_name = CONFIG['ApplicationParams']['topic_name']
        topic_name = str(topic_name)
        # Server IP Addresses for Kafka Nodes
        kafka_servers = CONFIG['ApplicationParams']['bootstrap_servers']
        kafka_servers = str(kafka_servers)
        # Consumer Name of Kafka Topic
        group_id = CONFIG['ApplicationParams']['group_id']
        group_id = str(group_id)
        # Disable auto commit parameter to ensure the message is processed
        enable_auto_commit = CONFIG['ApplicationParams']['enable_auto_commit']
        enable_auto_commit = eval(enable_auto_commit)
        """
        Set the auto offset reset to earliest to process the latest message
        always
        """
        auto_offset_reset = CONFIG['ApplicationParams']['auto_offset_reset']
        auto_offset_reset = str(auto_offset_reset)
        # Slack connection to Server
        slack_token = CONFIG['ApplicationParams']['slack_token']
        slack_token = str(slack_token)
        # Slack channel name
        slack_channel_name = CONFIG['ApplicationParams']['slack_channel_name']
        slack_channel_name = str(slack_channel_name)
        # Scylla DB keyspace name (Schema name)
        keyspace = CONFIG['ApplicationParams']['keyspace']
        keyspace = str(keyspace)
        # Set control connection timeout for Scylla DB
        control_connection_timeout = CONFIG['ApplicationParams']['\
                                            control_connection_timeout']
        control_connection_timeout = float(control_connection_timeout)
        # Set connection timeout for Scylla DB
        connect_timeout = CONFIG['ApplicationParams']['connect_timeout']
        connect_timeout = int(connect_timeout)
        # Maximum bytes retrieved from Kafka Topic
        max_partition_fetch_bytes = CONFIG['ApplicationParams']['\
                                            max_partition_fetch_bytes']
        max_partition_fetch_bytes = int(max_partition_fetch_bytes)
        # Number of queries that can be executed concurrently
        concurency_level = CONFIG['ApplicationParams']['concurency_level']
        concurency_level = int(concurency_level)

    except Exception as e:
        raise Exception('Exception encountered in set_env() while '
                        'setting up application configuration parameters.')

    return \
        user, password, node, port, topic_name, kafka_servers, group_id, \
        enable_auto_commit, auto_offset_reset, slack_token, \
        slack_channel_name, keyspace, control_connection_timeout, \
        connect_timeout, max_partition_fetch_bytes, concurency_level


def extract_command_params(arguments):
    """
    Passing arguments from command line.
    """

    # There should be only one argument
    if len(arguments) != 2:
        raise Exception('Illegal number of arguments. '
                        'Usage: '
                        'python read_repair_utility.py conf/parameter.ini')

    app_config_file = arguments[1]
    return app_config_file


def scylla_connection(user, password, node, port, keyspace,
                      control_connection_timeout, connect_timeout):
    """
    :param connect_timeout:
    :param control_connection_timeout:
    :param keyspace:
    :param port:
    :param node:
    :param password:
    :param user:
    :return query_status:
    """

    global auth_provider, cluster, session, table_a_stmt, table_b_stmt, \
        table_c_stmt, table_d_stmt, table_e_stmt

    # Creating a ScyllaDB connection
    if user:
        auth_provider = PlainTextAuthProvider(username=user,
                                              password=password)
        cluster = Cluster(
                        auth_provider=auth_provider,
                        contact_points=node, port=port,
                        connect_timeout=connect_timeout,
                        control_connection_timeout=control_connection_timeout)
    else:
        cluster = Cluster(
                        contact_points=node, port=port,
                        connect_timeout=connect_timeout,
                        control_connection_timeout=control_connection_timeout)

    session = cluster.connect()
    failed_msg = 0
    # Creating the prepared statements and setting the consistency level
    session.default_consistency_level = ConsistencyLevel.ALL
    # table_a_stmt is the table name in Scylla DB
    # columna is the primary key column in table_a_stmt
    table_a_stmt = session.prepare(
        "SELECT * FROM " + keyspace + ".tablea WHERE columna=?")
    table_a_stmt.consistency_level = ConsistencyLevel.ALL
    # table_b_stmt is the table name in Scylla DB
    # columnb is the primary key column in table_b_stmt
    table_b_stmt = session.prepare(
        "SELECT * FROM " + keyspace + ".tableb WHERE columnb=?")
    table_b_stmt.consistency_level = ConsistencyLevel.ALL
    # table_c_stmt is the table name in Scylla DB
    # columnc is the primary key column in table_c_stmt
    table_c_stmt = session.prepare(
        "SELECT * FROM " + keyspace + ".tablec WHERE columnc=?")
    table_c_stmt.consistency_level = ConsistencyLevel.ALL
    # table_d_stmt is the table name in Scylla DB
    # columnd is the primary key column in table_d_stmt
    table_d_stmt = session.prepare(
        "SELECT * FROM " + keyspace + ".tabled WHERE columnd=?")
    table_d_stmt.consistency_level = ConsistencyLevel.ALL
    # table_e_stmt is the table name in Scylla DB
    # columne is the primary key column in table_e_stmt
    table_e_stmt = session.prepare(
        "SELECT * FROM " + keyspace + ".tablee WHERE columne =?")
    table_e_stmt.consistency_level = ConsistencyLevel.ALL
    logging.info('Cluster Connection setup is done!')


def scylla_operation(items, concurency_level):
    # Bound statement for the Async Messages
    execution_stmt = []
    # Loading message in JSON object
    query_status = []
    # print('Message processing: ' + items.value)
    message_json_info = 'Message processing: ' + items.value
    logging.info(message_json_info)
    json_message = json.loads(items.value)

    # Loading IDs and bind them in the statement
    columnas = json_message['columna']
    columna_info = 'Number of IDs: ' + str(len(columnas))
    logging.info(columna_info)
    if len(columnas) > 0:
        for columna in columnas:
            # Id values are same for tablea and tableb
            tablea_lookup_stmt = table_a_stmt.bind([columna])
            execution_stmt.append(tablea_lookup_stmt)
            table_a_b_lookup_stmt = table_b_stmt.bind([columna])
            execution_stmt.append(table_a_b_lookup_stmt)

    # Loading AUIs and bind them in the statement
    columncs = json_message['columnc']
    columnc_info = 'Number of IDs: ' + str(len(columncs))
    logging.info(columnc_info)
    condition_a_field = json_message['condition_a_field']
    condition_b_field = json_message['condition_b_field']
    if len(columncs) > 0:
        for columnc in columncs:
            tablec_lookup_stmt = table_c_stmt.bind([columnc])
            execution_stmt.append(tablec_lookup_stmt)
            # Id values are same for tableb and tablec
            table_b_c_lookup_stmt = table_b_stmt.bind([columnc])
            execution_stmt.append(table_b_c_lookup_stmt)
            if condition_b_field or condition_a_field == 'field_value_a':
                tabled_lookup_stmt = table_d_stmt.bind([columnc])
                execution_stmt.append(tabled_lookup_stmt)

    # Loading Principal ID and bind it into the statement
    if condition_a_field == 'field_value_b':
        principal_id = json_message['condition_c_field']
        tablee_lookup_stmt = table_e_stmt.bind([principal_id])
        execution_stmt.append(tablee_lookup_stmt)

    """
    Clear Queue Function to clear the queries from the object
    once the desired CONCURRENCY LEVEL reached
    """

    def clear_queue():
        while True:
            try:
                futures.get_nowait().result()
            except queue.Empty:
                logger.debug(traceback.format_exc())
                failed_msg = 1
                break

    # To check the start time of query execution
    start = time.time()
    # Execute queries with user provided concurrency level
    futures = queue.Queue(maxsize=concurency_level)

    try:
        for stmt in execution_stmt:
            try:
                # Execute asynchronously
                future = session.execute_async(stmt)
                logging.info(stmt)
            except Exception as e:
                failed_msg = 1
                logging.error(
                    'Exception occurred at executing the asynchronous query')
                logger.debug(traceback.format_exc())
            try:
                futures.put_nowait(future)
            except queue.Full:
                clear_queue()
                futures.put_nowait(future)

        clear_queue()
        query_status.append("passed")
    except Exception as e:
        query_status.append("failed")
        failed_msg = 1
        logging.error('Exception occurred at executing the message queries')
        logger.debug(traceback.format_exc())

    # To check the end time of query execution
    end = time.time()

    logging.info("Query Execution Finished for the Message!")
    query_finish_info = 'Finished executing queries with a concurrency level' \
                        ' of ' + str(concurency_level) + ' in ' \
                        + str(end - start)
    logging.info(query_finish_info)
    return query_status


def process_messages(user, password, node, port, topic_name, kafka_servers,
                     group_id, enable_auto_commit, auto_offset_reset,
                     slack_token, slack_channel_name, keyspace,
                     control_connection_timeout, connect_timeout,
                     max_partition_fetch_bytes, concurency_level):
    """
    :param max_partition_fetch_bytes:
    :param concurency_level:
    :param connect_timeout:
    :param control_connection_timeout:
    :param slack_channel_name:
    :param slack_token:
    :param keyspace:
    :param auto_offset_reset:
    :param enable_auto_commit:
    :param group_id:
    :param kafka_servers:
    :param topic_name:
    :param port:
    :param node:
    :param password:
    :param user
    :return:
    """

    # Creating a Scylla Connection
    scylla_connection(
                    user, password, node, port, keyspace,
                    control_connection_timeout, connect_timeout)

    # Creating the Kafka Consumer Group
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=(kafka_servers),
        group_id=group_id,
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=auto_offset_reset,
        max_partition_fetch_bytes=max_partition_fetch_bytes)

    logging.info('Kafka Consumer setup is done!')
    total_lag = 0
    failed_msg = 0
    total_message_processed = 0
    lag = 0

    try:
        # print(kafka_consumer)
        partition_lag = {}
        # Checking the Kafka lag
        for partition in kafka_consumer.partitions_for_topic(topic_name):
            tp = TopicPartition(topic_name, partition)
            kafka_consumer.assign([tp])
            committed = kafka_consumer.committed(tp)
            logging.info(
                        'Committed: ' + str(committed) + ' for partition: ' +
                        str(partition))
            """
            When topic received the first message.
            In kafka-python library, if there is no prior message committed
            then it assigns NONE instead of 0."""
            if committed is None:
                logging.info('Executed Committed None!')
                committed = 0
            logging.info(
                        'Committed: ' + str(committed) + ' for partition: ' +
                        str(partition))
            kafka_consumer.seek_to_end(tp)
            last_offset = kafka_consumer.position(tp)
            logging.info(
                    'End offset: ' + str(last_offset) + ' for partition: ' +
                    str(partition))
            # Calculate the Kafka Topic lag here
            if last_offset is not None and committed is not None:
                lag = last_offset - committed
                partition_lag[partition] = lag

            partition_info = 'group: ' + str(group_id) \
                             + ' topic: ' + str(topic_name) \
                             + ' partition: ' + str(partition) \
                             + ' lag: ' + str(lag)
            logging.info(partition_info)

        if lag > 0:
            total_lag += lag

        # Starting message processing partition by partition
        logging.info('Message processing started partition by partition!')
        for partition, lag in partition_lag.items():
            partition_lag_info = 'Processing on partition: ' \
                                 + str(partition) \
                                 + ' having lag: ' \
                                 + str(lag)
            logging.info(partition_lag_info)
            if lag > 0:
                tp = TopicPartition(topic_name, partition)
                kafka_consumer.assign([tp])
                committed = kafka_consumer.committed(tp)
                kafka_consumer.position(tp)
                logging.info(
                    'Committed: ' + str(committed) + ' for partition: ' +
                    str(partition))
                """
                When topic received the first message.
                In kafka-python library, if there is no prior message committed
                then it assigns NONE instead of 0.
                """
                if committed is None:
                    logging.info('Executed Committed None!')
                    committed = 0
                # print(current_position)
                committed_current_info = 'Message Committed: ' + str(committed)
                logging.info(committed_current_info)
                message_count = 0
                end_offset = committed + lag
                """
                Run the following loop for every message to read (process) them
                in Scylla DB.
                """
                for msg in kafka_consumer:
                    start = time.time()
                    current_position = kafka_consumer.position(tp)
                    message_count_lag_position_info = 'Message Count: ' \
                                                      + str(message_count) \
                                                      + ', lag: ' \
                                                      + str(lag) \
                                                      + 'and current ' \
                                                        'position: ' \
                                                      + str(current_position)
                    logging.info(message_count_lag_position_info)
                    if message_count >= lag:
                        break
                    elif message_count < lag:
                        message_start_info = 'Message processing started for' \
                                             ' partition= ' + str(partition) \
                                             + ' at position= ' \
                                             + str(current_position)
                        logging.info(message_start_info)
                        """Sending message to the Scylla DB function to
                        create queries and fire them at the provided
                        concurrency level.
                        """
                        query_status = scylla_operation(msg, concurency_level)
                        if "failed" not in query_status:
                            logging.info("Message Processed!!!")
                            kafka_consumer.commit()
                            total_message_processed += 1
                        else:
                            logging.info("Message not processed!!!")
                            failed_msg = 1
                        end = time.time()
                        message_end_info = 'Message processing ended for ' \
                                           'partition= ' + str(partition) \
                                           + ' at position= ' \
                                           + str(current_position) \
                                           + ' with total time= ' \
                                           + str(end - start)
                        logging.info(message_end_info)

                    message_count += 1
                    if end_offset == current_position:
                        break

        logging.info('Message processing ended partition by partition!')

    except Exception as e:
        print("Exception:::: ", e)
        failed_msg = 1
        logging.error('Exception occurred at executing the message queries')
        logger.debug(traceback.format_exc())

    finally:
        print("Message Failed or not:- ", failed_msg)
        if total_message_processed > 0 and total_lag > 0 and failed_msg == 0:
            # Create a message to be posted in Slack
            message_on_slack = 'Read Replication done on ' \
                               + str(total_message_processed) \
                               + ' messages in Production Environment in FRA'
            # Create a body that needs to be delivered in Slack
            data = {
                'token': slack_token,
                'channel': slack_channel_name,
                'text': message_on_slack
            }
            logging.info("Process Ran Successfully!!!")
            """
            Post the message in Slack for Success where there are messages
            processed
            """
            requests.post(
                        url='https://slack.com/api/chat.postMessage',
                        data=data)
        if total_message_processed == 0 and total_lag == 0 and failed_msg == 0:
            message_on_slack = 'Read Replication triggered and found zero \
                                messages for processing at FRA DC Production'
            data = {
                'token': slack_token,
                'channel': slack_channel_name,
                'text': message_on_slack
            }
            """
            Post the message in Slack for Success where there is no message
            processed
            """
            requests.post(
                        url='https://slack.com/api/chat.postMessage',
                        data=data)
        if failed_msg == 1:
            message_on_slack = 'Read Replication failed in FRA DC Production\
                                Environment'
            data = {
                'token': slack_token,
                'channel': slack_channel_name,
                'text': message_on_slack
            }
            logging.info("Process Failed!!!!")
            """
            Post the message in Slack for Success where there is a failure
            in functionality
            """
            requests.post(
                        url='https://slack.com/api/chat.postMessage',
                        data=data)
        # Shut down consumer
        kafka_consumer.close()
        # Shut down cassandra cluster
        cluster.shutdown()


def main():
    """
    Usage: python read_repair_utility.py conf/parameter.ini
    :return:
    """

    try:
        logging.info("==== Processing Started ====")
        # Extract command line parameters
        p_app_config_file = extract_command_params(sys.argv)

        # Set environment
        user, password, node, port, topic_name, kafka_servers, group_id, \
            enable_auto_commit, auto_offset_reset, slack_token, \
            slack_channel_name, keyspace, control_connection_timeout, \
            connect_timeout, max_partition_fetch_bytes, \
            concurency_level = set_env(p_app_config_file)

        # Process Messages
        process_messages(user, password, node, port, topic_name, kafka_servers,
                         group_id, enable_auto_commit, auto_offset_reset,
                         slack_token, slack_channel_name, keyspace,
                         control_connection_timeout, connect_timeout,
                         max_partition_fetch_bytes, concurency_level)
        logging.info("==== Processing Ended ====")

    except Exception as e:
        logging.error('Exception message in main thread::::')
        logging.error(e)
        raise Exception('Exception message in main thread::::', e)


if __name__ == '__main__':
    main()
    logging.shutdown()
