FROM centos:6

ARG COMMIT_ID=unknown
LABEL io.confluent.docker.git.id=$COMMIT_ID
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker=true

# EPEL
#RUN yum install -y epel-release

COPY CentOS-Base.repo /etc/yum.repos.d/

# kerberos
RUN yum clean all -y \
    && yum update -y \
    && yum install -y krb5-server krb5-libs krb5-auth-dialog krb5-workstation

EXPOSE 88 749

ADD ./config.sh /config.sh

RUN mkdir /etc/krb5kdc

ENTRYPOINT ["/config.sh"]
