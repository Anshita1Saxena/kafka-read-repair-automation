# kafka-read-repair-automation
<p align='justify'>
The purpose of this project is to replicate messages which are not written in the Scylla database from Kafka Queue. This scenario of interruption in writing messages to the Scylla database can happen due to node (server) failure. No mitigation of this scenario can cause inconsistency between replicas (Scylla Database is a cluster of several nodes where nodes are replicas depending on Replication Factor).
</p>

## Description
<p align='justify'>
This project is used for replicating messages (client requests) within all the nodes. When a client sends the request to the system, the request is received by Kafka Queues and then those requests (messages) will be written to the Scylla Database. If there is any node failure that happened, then the failed messages will be written to the Failure Kafka Queue which was designed to hold these failed messages. Once the node failure got resolved then this utility helps in replicating messages within the servers of the cluster by taking the messages from the Failure Kafka Queue. Failure on the server can happen due to many causes: network interruption, disk failure, or server failure, etc.
</p>
<p align='justify'>
Our system is compliant for high availability, hence it has two DCs: Amsterdam and Frankfurt. If the server failure happens within Frankfurt Servers, then Failure Kafka Queue of Amsterdam records the failed messages and if the server failure happens within Amsterdam Servers, then Failure Kafka Queue of Frankfurt records the failed messages. Once the failure got resolved then the utility reads those messages from Failure Kafka Queue and replicates them within the servers.
</p>

## Advantages
1. This automated solution reduces the burden of searching the messages manually which needs to be replicated.
2. Utility helps in increasing the parallelism as the messages are replicated asynchronously and concurrently. The performance of the utility is given below:

![Performace Screenshot](https://github.com/Anshita1Saxena/kafka-read-repair-automation/blob/main/demo-image/Performance%20Screenshot.JPG)

## Environment Details
This utility is configured to poll the Failure Kafka Topic every hour to verify if there is any failed message to replicate. If messages exist in the Failure Kafka Queue, then this utility will pick up those messages, replicate them within the nodes of the cluster, and notify on Slack. This helps to avoid login into the server and checking into logs as the number of messages will be posted directly on Slack.

This code is running on RHEL (Red Hat Enterprize Linux) server. The required packages are listed in `requirements.txt` file. The command to install the required packages is given below:

`pip3 install -r requirements.txt`

Logs are collected in this file under logs directory: `/root/kafka-read-repair-automation/logs/read_repair_automation.log`

It follows the log format defined in the file: `/root/kafka-read-repair-automation/conf/log_config.conf`

`conf` directory also consists of `parameter.ini` which holds the several required parameters. This code is configuation based.

`kafka_queue_bot` bot and `#client-replication-channel` channel was configured in Slack.

## Working Details
The cronjob is configured via the following command to run every hour:

`00 * * * * /usr/bin/python3.7 /root/kafka-read-repair-automation/read_repair_utility.py /root/kafka-read-repair-automation/conf/parameter.ini`
This utility runs in Production. The data, parameter names, tables, and column names which is provided here is modified to maintain confidentiality.

## Highlights
1. Integration with Kafka
2. Integration with Scylla
3. Integration with Slack
4. No requirement for log checking by login into the server.

## Demo Screenshot
Messages delivered on Slack:
![Slack Screenshot File](https://github.com/Anshita1Saxena/kafka-read-repair-automation/blob/main/demo-image/Demo%20Slack%20Screenshot.JPG)
