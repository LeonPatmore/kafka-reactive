import subprocess


def run(command: str) -> int:
    with open("stdout.txt", "wb") as out, open("stderr.txt", "wb") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err, shell=True)
    return process.pid
