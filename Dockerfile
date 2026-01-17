FROM python:3.12

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install -U pip

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src

RUN pip install -e .

CMD ["python", "-c", "print('Container ready')"]
