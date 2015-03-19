# ClusterMaster

This project created for managing of defining master or slave in local cluster network. In this purpose was taken a floating way of defining, which provides a changing roles of clusters for best reliability. Machines, where is running this application, take part in voting for being master, by generating random numbers and cluster with bigger number will become a master. They send each other UDP packets with generated numbers and get answer is it bigger or smaller. This program should be running on all clusters and then administrator get known fast that one of machines knocked out.

ClusterReferee is a class which sends packets with number and state of cluster. This class have two methods for sending and receiving messages. If all machines finished negotiations, method of sending messages will check if were some conflicts, such as: two master machines, and if all good, this method will wait for a new voting. In receiving method, messages from other machines are processed. This messages have three fields:
- address, from where message came;
- if sender a primary;
- if it in processing, if it false, the previous field is final.

ClusterDiscoveryService is a class which creates and sends messages to other machines. Machine which was a initiator of voting makes other machines to answer. The result of successful negotiation will be a one master machine and other slaves machines.
