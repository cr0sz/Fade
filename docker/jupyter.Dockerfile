# docker/jupyter.Dockerfile
# Slim JupyterLab image for the backtest playground.
# Based on python:3.11-slim (not jupyter/* which are several GB).
# mem_limit: 1g — only started explicitly via `--profile analysis`.

FROM python:3.11-slim

# System dependencies: only what psycopg2 needs at runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
 && rm -rf /var/lib/apt/lists/*

# Python packages — pinned loosely but all known-compatible
RUN pip install --no-cache-dir \
        jupyterlab==4.2.5 \
        pandas==2.2.2 \
        matplotlib==3.9.2 \
        sqlalchemy==2.0.36 \
        psycopg2-binary==2.9.9 \
        numpy==1.26.4 \
        scipy==1.13.1 \
        ipykernel==6.29.5

# Non-root user matching the notebooks mount ownership
RUN useradd -m -u 1000 work
USER work
WORKDIR /home/work

# Notebooks are mounted at /home/work/notebooks
RUN mkdir -p /home/work/notebooks

EXPOSE 8888

# Token is injected via JUPYTER_TOKEN env var (set in .env).
# --no-browser + --ip=0.0.0.0 so it's reachable from the host via port binding.
CMD ["sh", "-c", \
     "jupyter lab --no-browser --ip=0.0.0.0 --port=8888 \
      --notebook-dir=/home/work/notebooks \
      --ServerApp.token=${JUPYTER_TOKEN:-} \
      --ServerApp.password='' \
      --ServerApp.allow_origin='*'"]
