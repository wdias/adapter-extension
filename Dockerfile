FROM python:3.7-slim-buster

WORKDIR /src
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--timeout=10", "--workers=4", "web.app:app"]

RUN pip3 install \
  gunicorn \
  flask \
  requests \
  webargs==4.1.2 \
  sqlalchemy==1.3.0b1 \
  https://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-8.0.22.zip#md5=75f7d8f9c769846a2a7777bc84ddfb65 \
  redis

COPY . /src
RUN cd /src && python3 setup.py develop
