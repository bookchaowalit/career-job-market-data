FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    API_HOST=0.0.0.0 \
    LAKE_READ_MODE=parquet \
    LAKE_READ_FALLBACK=error

WORKDIR /app

COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src

# The pinned data_lake runtime lists and reads s3:// (R2) objects through the AWS CLI.
RUN python -m pip install --no-cache-dir --disable-pip-version-check \
    ".[lake]" "awscli>=1.33,<2" \
    && useradd --system --uid 10001 --create-home jobdata
# Image hardening: take Debian security fixes newer than the base tag, then drop the base
# image's pip (the app runs from its installed environment; pip's vendored urllib3/msgpack/
# setuptools copies are what the Trivy image scan flags).
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && /usr/local/bin/python3 -m pip uninstall -y --disable-pip-version-check pip
USER jobdata

EXPOSE 8109
CMD ["python", "-m", "book_job_data.api"]
