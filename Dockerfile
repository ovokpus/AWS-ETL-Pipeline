FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY local_emulator ./local_emulator
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit
COPY assets ./assets
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

# Streamlit loads dashboard/app.py with the script directory on sys.path.
# PYTHONPATH keeps `import dashboard` and `import local_emulator` working.
ENV LAKE_DIR=/data/lake \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    HOME=/tmp

EXPOSE 8501

# The entrypoint chowns the lake and drops to LOCAL_UID (default 1000).
ENTRYPOINT ["/entrypoint.sh"]
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
