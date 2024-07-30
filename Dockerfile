FROM quay.io/keboola/docker-custom-python:latest

COPY . /code/
WORKDIR /data/

RUN pip install -r /code/requirements.txt
RUN python -m pip install requests

CMD ["python", "-u", "/code/main.py"]
