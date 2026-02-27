FROM python:3.12-alpine

RUN apk add --no-cache bash coreutils findutils tzdata

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY stat.py      /app/stat.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV TZ=Europe/Paris
ENTRYPOINT ["/app/entrypoint.sh"]
