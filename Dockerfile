FROM python:3.12-slim-bookworm

# Copy uv binary from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG GITHUB_TOKEN

# Install Java and required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    default-jdk-headless \
    wget \
    git \
    openssh-client \
    sshpass \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables for headless mode
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN useradd -m -u 1000 dagster

RUN mkdir -p /opt/dagster/dagster_home && \
    chown -R dagster:dagster /opt/dagster

WORKDIR /home/dagster/app

COPY --chown=dagster:dagster pyproject.toml /home/dagster/app/pyproject.toml
COPY --chown=dagster:dagster src /home/dagster/app/src
COPY --chown=dagster:dagster tests /home/dagster/app/tests

RUN if [ -n "$GITHUB_TOKEN" ]; then \
    git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi

RUN uv pip install --system -e .

RUN java -version && \
    python -c "import mdp_common; from pathlib import Path; drivers = Path(mdp_common.__file__).parent / 'drivers'; print(f'✅ mdp_common drivers found: {list(drivers.glob(\"*.jar\"))}')"

# Switch to non-root user
# USER dagster

ENV DAGSTER_HOME=/opt/dagster/dagster_home

EXPOSE 4000

CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "mdp_jade_piece.definitions"]
