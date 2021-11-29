import subprocess


def run(command: str) -> subprocess.Popen:
    with open("stdout.txt", "wb") as out, open("stderr.txt", "wb") as err:
        process = subprocess.Popen(command, stdout=out, stderr=err, shell=True)
    return process


if __name__ == "__main__":
    run("../kafka-lib/gradlew -b ../../kafka-lib/build.gradle.kts test --tests *TestConsumer*")