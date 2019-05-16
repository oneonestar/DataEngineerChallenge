# Analyze ELB log using Apache Flink
## Features
* Using Apache Flink and Dataflow Programming Model
* Can be used to handle large scale streaming log data in real-time (power by Flink Cluster)
* Implement a custom window aggregater(UserSessionAggregate), minimize the memory usage for streaming aggregation

## Limitations (TODO)
* Handle late data after watermark instead of holds back the watermark
* Handle parsing error (invalid / malformatted log)
* Create test for watermark and trigger
* Verify the output result with the provided dataset

## How to compile and run
```bash
# Compile and run
$ cd data-engineer-challenge
$ mvn clean package
$ mvn exec:java -Dexec.mainClass=challenge.flink.StreamingJob -Dexec.cleanupDaemonThreads=false


# See outputs. Beware Flink output file as hidden file
$ head output/task1/*/.*
==> output/task1/2019-05-16--10/.part-0-0.inprogress.fb1f00d2-a93a-4b06-9806-a929a6ebcd9d <==
(112.79.36.98,1)
(107.167.109.45,1)
(106.51.133.0,1)
(49.249.39.29,1)
(178.255.152.2,1)
(106.206.149.203,2)
(223.176.5.63,1)
(70.39.184.200,2)
(202.142.81.245,7)
(37.228.107.126,1)

==> output/task1/2019-05-16--10/.part-1-0.inprogress.d3ef4941-d196-4e24-874a-01cae7e7d11b <==
(117.227.142.117,1)
(120.56.218.165,3)
(106.51.155.229,1)
(199.190.46.39,1)
....


$ head output/task2/*/.*
100.72751039354888
```

## Sample output
```text
1. All page hits by visitor/IP during a session.
====================
(ip, page hit count)
--------------------
(123.236.61.25,5)
(14.96.205.187,6)
(117.226.247.46,2)
(213.239.204.204,83)
(125.63.102.8,8)
(216.35.100.190,5)
(106.216.161.16,1)
(103.224.156.230,21)
(70.39.187.219,1)
(117.205.84.21,1)
(180.151.178.59,2)
(101.223.179.155,5)
====================

2. Determine the average session time
100.7275103935488

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
======================
(ip, unique hit count)
----------------------
(116.203.200.165,43)
(27.131.213.130,3)
(106.51.132.39,3)
(14.98.76.185,3)
(103.48.108.3,3)
(117.206.134.133,6)
(107.167.108.143,3)
(117.201.82.93,1)
(107.167.107.10,3)
(164.100.1.92,11)
(103.37.201.252,7)
(182.68.136.65,104)
(1.187.143.206,7)
(123.201.175.102,1)
(103.16.69.11,11)
(117.220.128.57,2)
======================

4. Find the most engaged users, ie the IPs with the longest session times
===================================
(ip, total session duration in sec)
-----------------------------------
(220.226.206.7,6795.972)
(52.74.219.71,5259.206999999999)
(119.81.61.166,5252.601)
(54.251.151.39,5236.79)
(121.58.175.128,4989.3949999999995)
(106.186.23.95,4931.67)
(125.19.44.66,4652.686999999999)
(54.169.191.85,4621.897)
(207.46.13.22,4533.383)
(180.179.213.94,4513.17)
===================================

```

---

# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

1. Fork this repo in github
2. Complete the processing and analytics as defined first to the best of your ability with the time provided.
3. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the PayPay interview team.
4. Include the test code and data in your solution. 
5. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
