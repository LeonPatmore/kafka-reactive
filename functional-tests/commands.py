
import subprocess


def run(command: str) -> int:
    process = subprocess.Popen(command.split())
    return process.pid
