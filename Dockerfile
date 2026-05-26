FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    git \
    curl \
    ca-certificates \
    python3-venv \
    procps \
    python3-tk \
    tk \
    tcl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/local/bin/minio && \
    chmod +x /usr/local/bin/minio && \
    curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

WORKDIR /opt

RUN if [ -d /opt/CCBD_Project/.git ]; then \
        cd /opt/CCBD_Project && git pull; \
    else \
        git clone https://github.com/TanguyGodat/CCBD_Project.git /opt/CCBD_Project; \
    fi

WORKDIR /opt/CCBD_Project
RUN python3 -m venv .venv
ENV PATH="/opt/CCBD_Project/.venv/bin:${PATH}"

RUN pip install --upgrade pip && pip install -r requirements.txt

RUN mkdir -p /data /var/log/minio /root/.mc /opt/CCBD_Project/results /opt/CCBD_Project/benchdownloads /opt/CCBD_Project/data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9000 9001

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
