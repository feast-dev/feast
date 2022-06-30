FROM python:3.8

WORKDIR /usr/src/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY . .
EXPOSE 8080

CMD ["./entrypoint.sh"]
