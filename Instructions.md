# Hadoop TA PART 1

## Launch hadoop

``` bash
rm -rf /tmp/hadoop*
/opt/hadoop-3.2.1/bin/hdfs namenode -format
/opt/hadoop-3.2.1/sbin/start-dfs.sh
/opt/hadoop-3.2.1/sbin/start-yarn.sh
```

## Copy data to HDFS

``` bash
/opt/hadoop-3.2.1/bin/hadoop fs -mkdir /user
/opt/hadoop-3.2.1/bin/hadoop fs -mkdir /user/marcos
/opt/hadoop-3.2.1/bin/hadoop fs -copyFromLocal /home/marcos/googlebooks-spa-all-1gram-20120701-a /user/marcos/
/opt/hadoop-3.2.1/bin/hadoop fs -copyFromLocal /home/marcos/googlebooks-spa-all-2gram-20120701-a_ /user/marcos/
/opt/hadoop-3.2.1/bin/hadoop fs -copyFromLocal /home/marcos/googlebooks-spa-all-2gram-20120701-al /user/marcos/
```

## Launch task 2

``` bash
~/TAP2_PART1/launchtask2$    ./launchtask2
```

## Launch task 1 (LOCAL)

``` bash
/opt/hadoop-3.2.1/bin/mapred streaming -input /user/marcos/googlebooks-spa-all-1gram-20120701-a -output output.task1 -mapper "/home/marcos/TAP2_PART1/Task1/task1 -task 0 -phase map" -reducer "/home/marcos/TAP2_PART1/Task1/task1 -task 0 -phase reduce" -io typedbytes
```

## Remove folder

``` bash
/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r /user/marcos/output.task1
```

## Check output

``` bash
/opt/hadoop-3.2.1/bin/hdfs dfs -cat /user/marcos/output.task1/part-00000
```

## HADOOP CONFIGURATION

### core-site.xml

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>io.file.buffer.size</name>
    <value>131072</value>
  </property>
</configuration>
```

### hdfs-site.xml

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

### yarn-site.xml

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
</configuration>
```

### mapred-site.xml

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

### hadoop-env.sh

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.2.1
```

### ~/.bashrc

```bash
export PDSH_RCMD_TYPE=ssh

# JAVA
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# HADOOP
export HADOOP_HOME=/opt/hadoop-3.2.1
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# GO
export GOROOT=/usr/local/go
export GOPATH=$HOME
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
```
