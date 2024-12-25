import os
import subprocess
import sys


def start_app():
    script_path = os.path.join(os.path.dirname(__file__), "stock_watcher.py")
    command = [sys.executable, "-m", "streamlit", "run", script_path]
    subprocess.run(command, check=True)


if __name__ == "__main__":
    start_app()
