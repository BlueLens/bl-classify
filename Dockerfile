FROM bluelens/python:3.6
MAINTAINER bluehackmaster <master@bluehack.net>

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
