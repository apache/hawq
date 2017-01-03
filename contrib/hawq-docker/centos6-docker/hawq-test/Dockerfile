FROM hawq/hawq-dev:centos6

MAINTAINER Richard Guo <riguo@pivotal.io>

USER root

# install HDP 2.5.0
RUN curl -L "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.5.0.0/hdp.repo" -o /etc/yum.repos.d/hdp.repo && \
 yum install -y hadoop hadoop-hdfs hadoop-libhdfs hadoop-yarn hadoop-mapreduce hadoop-client hdp-select && \
 yum clean all

RUN ln -s /usr/hdp/current/hadoop-hdfs-namenode/../hadoop/sbin/hadoop-daemon.sh /usr/bin/hadoop-daemon.sh

COPY conf/* /etc/hadoop/conf/

COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY start-hdfs.sh /usr/bin/start-hdfs.sh

USER gpadmin

ENTRYPOINT ["entrypoint.sh"]
CMD ["bash"]

