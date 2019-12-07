FROM python:3.6

ENV PYTHONUNBUFFERED=0

ENV HOME_APP=/opt/app
RUN mkdir -p ${HOME_APP}
COPY . ${HOME_APP}
WORKDIR ${HOME_APP}

RUN pip install -r requirements.txt

CMD ['python', '-m', 'kafka_twitter_producer.main']