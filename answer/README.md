#WeblogChallenge
For Processing & Analytical, I have solved these questions by Hadoop 2.7.3 and Java 1.8.
And for MLE questions, I implemented the models by Keras(backgroud as Tessorflow).

##Processing & Analytical
Q1. I aggregated hits for sessions with 15 minutes fixed time window, and left session counts for each ip.
Source code is src/java/paytmlabs.Sessionize.java, and output is answer/Q1_sessionize_session_count.txt.
Top 10 results are as follow.
| ip            | session count |
| ------------- |:-------------:|
|220.226.206.7	|13             |
|59.144.58.37	|10             |
|54.255.254.236	|10             |
|54.252.79.172	|10             |
|54.252.254.204	|10             |
|54.251.31.140	|10             |
|54.251.151.39	|10             |
|54.250.253.236	|10             |
|54.248.220.44	|10             |
|54.245.168.44	|10             |

Q2. Calculated the average times of sessions for each ip.
Source code is src/java/paytmlabs.SessionAverage.java. The total average session time is 88125(ms). 
Top 10 results for average session times are as follow. (output is answer/Q2_session_average_time.txt)
| ip            | avg session time(ms)|
| ------------- |:-------------------:|
|103.29.159.138	|2065413              |
|125.16.218.194	|2064475              |
|14.99.226.79	|2062909              |
|122.169.141.4	|2060159              |
|14.139.220.98	|2058319              |
|117.205.158.11	|2057223              |
|111.93.89.14	|2054976              |
|182.71.63.42	|2050458              |
|223.176.3.130	|2047721              |
|183.82.103.131	|2042724              |

Q3. Calculated unique urls for sessions.
Source code is src/java/paytmlabs.SessionUniqueUrls.java, and output is answer/Q3_session_unique_url_count.txt.
Sample 10 results are as follow.
| ip            | {sessionId:unique url count} |
| ------------- |:----------------------------:|
|1.186.101.79	|{1:12}                        |
|1.186.103.240	|{1:5}                         |
|1.186.103.78	|{1:4}                         |
|1.186.108.213	|{1:2}                         |
|1.186.108.230	|{1:5}                         |
|1.186.108.242	|{1:3}                         |
|1.186.108.28	|{1:80}                        |
|1.186.108.29	|{1:3}                         |
|1.186.108.79	|{1:3}                         |
|1.186.111.224	|{1:9}{2:12}                   |

Q4. Calculated the longest session for each ip.
Source code is src/java/paytmlabs.SessionLongest.java, and output is answer/Q4_session_longest_time.txt.
Top 10 results are as follow.
| ip            | {sessionId:unique url count} |
| ------------- |:----------------------------:|
|52.74.219.71	|2069162                       |
|119.81.61.166	|2068849                       |
|106.186.23.95	|2068756                       |
|125.19.44.66	|2068713                       |
|125.20.39.66	|2068320                       |
|192.8.190.10	|2067235                       |
|54.251.151.39	|2067023                       |
|180.211.69.209	|2066961                       |
|180.179.213.70	|2065638                       |
|203.189.176.14	|2065594                       |

##Questions for Machine Learning Engineer
Q1.
Q2.
Q3.
