FROM python:2.7

ADD . /src

#RUN apt-get update && apt-get install -y build-essential \
#                                         python-dev \
#                                         python-pip

RUN pip install -r /src/requirements.txt

CMD ["python", "/src/rss.py"]
