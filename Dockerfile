FROM bluelens/bl-classify-base:latest
MAINTAINER bluehackmaster <master@bluehack.net>

USER root

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

#RUN pip install -r requirements.txt

CMD ["python", "main.py"]
