FROM hub-new.pingcap.net/guojiangtao/base-tiflash-mock-autoscaler

RUN rm -rf /tiflash-mock-autoscaler && mkdir /tiflash-mock-autoscaler
ADD . /tiflash-mock-autoscaler
WORKDIR /tiflash-mock-autoscaler

ENV PATH="/usr/local/go/bin:${PATH}"
RUN make && chmod 777 run.sh
EXPOSE 8888
CMD /tiflash-mock-autoscaler/run.sh
