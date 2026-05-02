"""
Pipeline entry point.

The evaluation system invokes:
  docker run ... python pipeline/run_all.py

With --network=none, Java cannot resolve the container hostname for InetAddress.getLocalHost().
We point the JVM at a minimal hosts file under /tmp (writable) before Spark starts the JVM.
"""

from __future__ import annotations

import logging
import os
import socket


def _configure_jvm_hosts_for_isolated_network() -> None:
    host = socket.gethostname()
    hosts_path = "/tmp/spark_hosts"
    try:
        with open(hosts_path, "w", encoding="utf-8") as f:
            f.write("127.0.0.1 localhost\n")
            f.write(f"127.0.0.1 {host}\n")
        extra = f"-Djdk.net.hosts.file={hosts_path}"
        prev = os.environ.get("_JAVA_OPTIONS", "").strip()
        os.environ["_JAVA_OPTIONS"] = f"{prev} {extra}".strip() if prev else extra
    except OSError:
        pass


_configure_jvm_hosts_for_isolated_network()

import time

from pipeline.common import load_pipeline_config
from pipeline.dq_report import write_dq_report
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion
from pipeline.transform import run_transformation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    pipeline_start_time = time.time()
    run_ingestion()
    run_transformation()
    run_provisioning()

    write_dq_report(spark=None, config=load_pipeline_config(), pipeline_start_time=pipeline_start_time)

    stream_dir = "/data/stream"
    if os.path.isdir(stream_dir) and os.listdir(stream_dir):
        logger.info("Stream directory found — running Stage 3 stream ingestion")
        run_stream_ingestion()
    else:
        logger.info("No stream directory — skipping Stage 3 stream ingestion")
