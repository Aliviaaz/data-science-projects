"""
Load and parse the data. 
Every entry in the dataset is a comma-delimited line. 
The data consists of different attributes captured from connection data. 
For this exercise, you only need the 1st attribute (duration), the 2nd attribute (protocol-type), and the last attribute (label). 

- Duration: length (number of seconds) of the connection 
- Protocol-type: type of the protocol, e.g. tcp, udp, etc. 
- Label: types of the connection, e.g. normal, guess-passwd, etc. 

Q1: Get the total number of connections based on the type of connectivity protocol. 
Q2: Find out the total duration time for each protocol. 
Q3: Find out the average duration for each protocol
Q4: Find out which protocol is most vulnerable to attacks. 
In other words, which protocol has the highest number of attacks. 
“normal” is no attack; other values of the attribute Label are considered as attack. 
Save the analysis results as JSON files in S3.
"""

from pyspark.sql import SparkSession

sqlCtx = SparkSession.builder.appName("myapp").getOrCreate()

df = sqlCtx.read.csv("s3://cpsc-4330-extra-credit-3/kddcup.data.gz", header=False, sep=",")

df.printSchema()

#c0 = duration, c1 = protocol-type, c41 = label
parse_df = df.select(df._c0, df._c1, df._c41)

new_rdd = parse_df.rdd

from pyspark.sql import Row

parsed_rdd = new_rdd.map(lambda r: Row(duration=int(r[0]), protocol=r[1], label=r[-1]))
                         
df = sqlCtx.createDataFrame(parsed_rdd)

df.createOrReplaceTempView("network")

#get the total number of connections based on the type of connectivity protocol
total_connections = sqlCtx.sql(""" SELECT protocol, COUNT(*) as total_connections FROM network GROUP BY protocol ORDER BY total_connections DESC """)
total_connections.show()
total_connections.coalesce(1).write.format('json').save("s3://cpsc-4330-extra-credit-3/q1")

#find out the total duration time for each protocol
total_durations = sqlCtx.sql(""" SELECT protocol, SUM(duration) AS total_duration FROM network GROUP BY protocol ORDER BY total_duration DESC """)
total_durations.show()
total_durations.coalesce(1).write.format('json').save("s3://cpsc-4330-extra-credit-3/q2")

#find out the average duration for each protocol
avg_durations = sqlCtx.sql(""" SELECT protocol, AVG(duration) AS average_duration FROM network GROUP BY protocol ORDER BY average_duration DESC """)
avg_durations.show()
avg_durations.coalesce(1).write.format('json').save("s3://cpsc-4330-extra-credit-3/q3")

#find out which protocol is most vulnerable to attacks
highest_attack = sqlCtx.sql(""" SELECT protocol, COUNT(*) AS num_attacks FROM network WHERE label NOT IN ('normal') GROUP BY protocol ORDER BY num_attacks DESC LIMIT 1""")
highest_attack.show()
highest_attack.coalesce(1).write.format('json').save("s3://cpsc-4330-extra-credit-3/q4")