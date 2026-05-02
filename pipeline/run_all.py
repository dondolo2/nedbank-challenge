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

from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.transform import run_transformation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    run_ingestion()
    run_transformation()
    run_provisioning()
