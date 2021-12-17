FROM python:3.9

WORKDIR /workdir

COPY requirements.txt /workdir

ENV TZ=Asia/Shanghai

RUN pip install -r requirements.txt

CMD ["python", "notexist.py"]