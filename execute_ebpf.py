"""eBPF raw data extraction and profiling on target nodes."""

import argparse
import datetime
import io
import logging
import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from time import sleep
from typing import Any, Dict, List, Optional, Sequence, Union

import boto3
import psutil
from mypy_boto3_s3.client import S3Client

EBPF_PATH = "my_bcc_path"  # path to ebpf bcc repository
BUCKET = "my_s3_bucket"
KEY = "raw_key"
WORKLOADS: Dict[str, List[str]] = {
    "DISKIO": ["my_diskio_gcc_path", "&"],  # path to DiskIO_gcc
    "MATRIX": [
        "my_matmul_gcc_path",
        "-l",
        "2",
        "-m",
        "3000",
        "-p",
        "1500",
        "-n",
        "1500",
        "&",
    ],  # path to MatrixMultiplication_gcc
    "IDLE": [],
}


@dataclass
class EBPFCommand:
    """Class to store default arguments and option to profile with PID."""

    default_args: List[str]
    pid_arg: bool


class ThreadedExecution:
    """Executes scripts on separate thread and save output to filepath
    with timeout functionality."""

    def __init__(self, cmd: Sequence[str]) -> None:
        """Initializes ThreadingExecution class."""
        self.cmd = cmd
        self.process: Union[subprocess.Popen, None] = None
        self.output = io.StringIO()

    def run(
        self,
        execution_timeout: int = 45,
        graceful_exit_timeout: int = 5,
        **popen_kwargs: Any,
    ) -> io.StringIO:
        """Run script, handle termination and error checking."""
        join_timeout = 2  # timeout to join thread upon execution time interruption

        def target() -> None:
            """Runs the script using subprocess library."""
            logging.info(f"Thread for {self.cmd} started")
            self.process = subprocess.Popen(
                self.cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env={"PYTHONUNBUFFERED": "1"},
                **popen_kwargs,
            )  # disable buffering of stdout to update upon timeout

            self.output.write(self.process.stdout.read().decode("utf-8"))
            self.output.seek(0)
            self.process.communicate()  # sets return code after process termination

            logging.info(f"Thread for {self.cmd} finished")

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(execution_timeout)  # waits for thread to timeout

        if thread.is_alive():
            logging.info("Terminating process")
            os.kill(self.process.pid, signal.SIGINT)  # graceful exit
            sleep(graceful_exit_timeout)
            self.process.terminate()  # forceful termination after 5 seconds

            thread.join(join_timeout)  # joins thread after timeout
            logging.info(
                f"Execution successfully terminated after {execution_timeout}s for cmd: {self.cmd}"
            )
            return self.output

        if self.process and self.process.returncode > 0:
            raise subprocess.CalledProcessError(self.process.returncode, self.cmd)

        logging.info(f"Execution successful for command: {self.cmd}")
        return self.output


def generate_filepath(node: str, script_name: str) -> str:
    """Generate filepath for storage in S3."""
    now = datetime.datetime.now().astimezone()
    date_str = now.strftime("%m-%d-%Y")
    return KEY + date_str + "/" + node + "_" + script_name + ".txt"


def ebpf_execution(
    boto_client: S3Client, node: str, program_cmd: Optional[List[str]] = None
) -> None:
    """Profiling for eBPF and optional programs simultaneously on end systems."""
    if program_cmd is None:
        program_cmd = []

    for script_name, ebpf_cmd in ebpf_args.items():
        print("Executing script " + script_name)
        result_path = generate_filepath(node, script_name)

        cmd = [
            "python",
            EBPF_PATH + script_name + ".py",
        ] + ebpf_cmd.default_args  # construct command

        program_pid = None
        try:
            if program_cmd != []:  # if another program / workload command is passed in
                program = subprocess.Popen(program_cmd)
                program_pid = program.pid

                if ebpf_cmd.pid_arg:  # add to ebpf command if able to profile by PID
                    cmd.extend(["-p", str(program_pid)])

            execution = ThreadedExecution(cmd)
            output = execution.run()  # run profiling

            if program_pid is not None and psutil.pid_exists(
                program_pid
            ):  # if workload still running, kill
                os.kill(program_pid, signal.SIGKILL)

            boto_client.put_object(Bucket=BUCKET, Key=result_path, Body=output.read())
        except Exception as ex:
            print(ex)


ebpf_args = {
    "execsnoop": EBPFCommand(["-x", "-T", "-U"], False),
    "syscount": EBPFCommand(
        ["-L", "-d", "25"], True
    ),  # activate option to profile by PID if process running
    "runqlat": EBPFCommand(["25", "1"], False),  # has PID argument, but returns no data
    "biosnoop": EBPFCommand([], False),
    "biolatency": EBPFCommand(["25", "1", "-j"], False),
}


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="eBPF Execution for specified input Node Name"
    )
    parser.add_argument(
        "-n",
        dest="node_name",
        type=str,
        required=True,
        help="desired name of target node",
    )
    parser.add_argument(
        "-w",
        dest="workload",
        type=str,
        required=True,
        help="type of workload to run alongside profiling",
    )

    args = parser.parse_args()
    node_name = args.node_name
    workload = args.workload

    if workload not in WORKLOADS:
        raise KeyError(
            "Must pass in valid Workload from the following: ", list(WORKLOADS.keys())
        )

    s3 = boto3.client("s3")
    ebpf_execution(s3, node_name, WORKLOADS[workload])
