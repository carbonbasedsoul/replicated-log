FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ ./src/

RUN pip install uv
RUN uv sync

CMD ["uv", "run", "src/master.py"]
