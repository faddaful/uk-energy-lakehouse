"""On-demand Dagster job: starts the Streamlit dashboard as a detached
background process. Not an asset: a dashboard is a process to launch,
not data to materialise, so it sits outside the asset graph as its own
job, triggered by hand from the Dagster UI (reachable over Tailscale,
same as the dashboard itself once it's up).

Detached (start_new_session=True) so the process outlives this op: the op
only has to start the server, not sit there supervising it for as long as
you want the dashboard up.
"""

import socket
import subprocess
import sys
from pathlib import Path

from dagster import OpExecutionContext, job, op

DASHBOARD_PORT = 8501
REPO_ROOT = Path(__file__).resolve().parents[3]


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("localhost", port)) == 0


@op
def start_streamlit_dashboard(context: OpExecutionContext) -> None:
    if _port_in_use(DASHBOARD_PORT):
        context.log.info(f"Dashboard already running on port {DASHBOARD_PORT}; nothing to do.")
        return

    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "apps/streamlit/dashboard.py",
            "--server.address",
            "0.0.0.0",
            "--server.port",
            str(DASHBOARD_PORT),
            "--server.headless",
            "true",
        ],
        cwd=REPO_ROOT,
        start_new_session=True,
    )
    context.log.info(f"Started dashboard on port {DASHBOARD_PORT}, same Tailscale IP as this Dagster UI.")


@job(description="Start the Streamlit dashboard on demand, if it isn't already running.")
def streamlit_dashboard_job() -> None:
    start_streamlit_dashboard()
