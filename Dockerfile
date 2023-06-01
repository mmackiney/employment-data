FROM python:3.9

COPY . /app
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

RUN pip install requests
RUN pip install dbt-snowflake

ENTRYPOINT ["./entrypoint.sh"]
