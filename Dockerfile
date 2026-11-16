FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Dependencies first, so code changes don't reinstall them.
COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" \
      > /tmp/requirements.txt \
 && pip install -r /tmp/requirements.txt

COPY src ./src
COPY contracts ./contracts
COPY transform ./transform
COPY dashboard ./dashboard
COPY .streamlit ./.streamlit
COPY docs ./docs
RUN pip install --no-deps . \
 && useradd --create-home --uid 10001 app \
 && mkdir -p /secrets /state /sftp \
 && chown -R app /app/transform /secrets /state /sftp

ENV STOREOPS_CONTRACTS_DIR=/app/contracts STOREOPS_DBT_PROJECT_DIR=/app/transform
USER app
CMD ["storeops", "--help"]
