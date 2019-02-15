FROM            alpine:latest

COPY ./service/linux_krc.bin /krc
RUN mkdir /etc/krc && \
    mkdir /etc/krc/krcdb && \
    touch /etc/krc/krc.yaml && \
    chmod 755 /krc && \
    chmod 777 -R /etc/krc/krcdb
EXPOSE 8080
ENTRYPOINT [ "/krc", "-config", "/etc/krc/krc.yaml" ]
