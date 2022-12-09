"""Transformation functionality for individual eBPF scripts."""

import ast
import re
from typing import Dict, List, Union


def transform_ebpf(script_name: str, raw_object: object, node_name: str) -> Dict:
    """Master function that returns transformed data to lambda."""
    if script_name == "execsnoop":
        return transform_execsnoop(raw_object, node_name)
    if script_name == "syscount":
        return transform_syscount(raw_object)
    if script_name == "runqlat":
        return transform_runqlat(raw_object)
    if script_name == "biosnoop":
        return transform_biosnoop(raw_object)
    if script_name == "biolatency":
        return transform_biolatency(raw_object)
    raise Exception("Invalid script name")


def transform_execsnoop(raw_object: object, node: str) -> Dict[str, List[str]]:
    """Pull data into data frame for execsnoop transformation."""
    file_iter = raw_object["Body"].iter_lines()
    line = next(file_iter).decode("utf-8")

    elements = re.split(
        r"\s+", line.strip(), flags=re.UNICODE
    )  # splits line into list, items separated by spaces
    mapping = dict(enumerate(elements))
    data: Dict[str, List[str]] = {element: [] for element in elements}

    for line in file_iter:
        elements = re.split(r"\s+", line.decode("utf-8").strip(), flags=re.UNICODE)
        for ele_idx, element in enumerate(elements):
            if (
                ele_idx == len(mapping) - 1
            ):  # append all fields under "ARGS column" into one column in data frame
                data[mapping[ele_idx]].append(" ".join(elements[ele_idx:]))
                break

            data[mapping[ele_idx]].append(element)

    data["node"] = [node] * len(
        data[list(data.keys())[0]]
    )  # adds column with node name
    return data


def transform_syscount(raw_object: object) -> Dict[str, Union[List[str], List[float]]]:
    """Pull syscalls and avg latencies for syscount transformation."""
    file_iter = raw_object["Body"].iter_lines()

    calls = []
    avg_latencies = []

    for line in file_iter:
        elements = re.split(r"\s+", line.decode("utf-8").strip(), flags=re.UNICODE)
        if len(elements) == 3 and re.match(
            r"^-?\d+(?:\.\d+)$", elements[-1]
        ):  # checks if last element in line is valid string representation of float
            calls.append(elements[0])
            avg_latencies.append(float(elements[-1]) / float(elements[1]))
    return {"calls": calls, "latencies": avg_latencies}


def transform_biolatency(raw_object: object) -> Dict[str, Union[List[str], List[int]]]:
    """Extract bin interval strings and counts for biolatency transformation."""
    raw_output = raw_object["Body"].read().decode("utf-8")

    bins: List[str] = []
    counts: List[int] = []

    if raw_output:
        data = ast.literal_eval(raw_output.strip())["data"]
        for entry in data:
            bins.append(
                str(entry["interval-start"]) + "->" + str(entry["interval-end"])
            )
            counts.append(entry["count"])

    return {"bins": bins, "counts": counts}


def transform_biosnoop(raw_object: object) -> Dict[str, float]:
    """Get avg read and write latency for biosnoop transformation."""
    file_iter = raw_object["Body"].iter_lines()

    read_count: int = 0
    write_count: int = 0

    read_lat: float = 0
    write_lat: float = 0

    next(file_iter)  # skip column headers
    for line in file_iter:
        elements = re.split(r"\s+", line.decode("utf-8").strip(), flags=re.UNICODE)

        if elements[4] == "W":
            write_count += 1
            write_lat += float(elements[-1])
        else:
            read_count += 1
            read_lat += float(elements[-1])

    return {
        "write_avg_latency": (write_lat / write_count) if write_count != 0 else 0,
        "read_avg_latency": (read_lat / read_count) if read_count != 0 else 0,
    }


def transform_runqlat(raw_object: object) -> Dict[str, Union[List[str], List[int]]]:
    """Get interval strings and counts for runqlat transformation."""
    file_iter = raw_object["Body"].iter_lines()
    intervals = []
    counts = []

    next(file_iter)  # skip "Tracing ..." message
    for line in file_iter:
        elements = re.split(r"\s+", line.decode("utf-8").strip(), flags=re.UNICODE)

        if len(elements) > 4 and elements[0] != "usecs":  # checks if line contains data
            intervals.append(" ".join(elements[0:3]))
            counts.append(int(elements[4]))

    return {"bins": intervals, "counts": counts}
