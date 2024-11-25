FROM ubuntu:latest
LABEL authors="christophe2bu"

WORKDIR /oem-utils
EXPOSE 8883
EXPOSE 1883
COPY oem-converter-linux ./
COPY config.yaml ./
CMD ["/oem-utils/oem-converter-linux"]