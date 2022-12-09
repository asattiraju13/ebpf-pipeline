"""Functionality to combine transformed outputs from individual nodes."""

from collections import defaultdict
from typing import Dict, List, Union

import numpy as np
import numpy.typing as npt
import pandas as pd


def combine_ebpf(
    result_data: List[Dict], script_name: str
) -> Union[pd.DataFrame, Dict]:
    """Master function that returns combined data to lambda."""
    if script_name == "execsnoop":
        return combine_execsnoop(result_data)
    if script_name == "syscount":
        return combine_syscount(result_data)
    if script_name in ("biolatency", "runqlat"):
        return combine_heatmap(result_data)
    if script_name == "biosnoop":
        return combine_biosnoop(result_data)
    raise Exception("Invalid script name")


def combine_execsnoop(
    result_data: List[Dict[str, Union[str, Dict[str, List[str]]]]]
) -> pd.DataFrame:
    """Combines execsnoop data frames into one data frame."""
    data_frame = pd.DataFrame()
    for res in result_data:
        node_data_frame = pd.DataFrame.from_dict(res["data"])
        data_frame = pd.concat([data_frame, node_data_frame], ignore_index=True)
    return data_frame


def combine_syscount(
    result_data: List[Dict[str, Union[str, Dict[str, Union[List[str], List[float]]]]]]
) -> Dict[str, Dict[str, Union[List[str], List[float]]]]:
    """Combines syscount results into one dictionary."""
    data: Dict[str, Dict[str, Union[List[str], List[float]]]] = defaultdict(
        lambda: defaultdict(lambda: [])
    )
    for res in result_data:
        calls, latencies = res["data"]["calls"], res["data"]["latencies"]
        for i, call in enumerate(calls):
            data[call]["node"].append(res["node"])
            data[call]["latencies"].append(latencies[i])
    return data


def combine_heatmap(
    result_data: List[Dict[str, Union[str, Dict[str, Union[List[str], List[int]]]]]]
) -> Dict[str, Union[List[str], npt.NDArray[np.intc]]]:
    """Combines runqlat or biolatency results into 2D heatmap."""
    x_ticks: List[str] = []
    y_ticks: List[str] = []
    node_data: List[List[int]] = []
    for res in result_data:
        x_ticks.append(res["node"])
        if len(y_ticks) < len(
            res["data"]["bins"]
        ):  # sets y_ticks to largest distribution recorded for heatmap y-axis
            y_ticks = res["data"]["bins"]
        node_data.append(res["data"]["counts"])

    data = np.zeros((len(y_ticks), len(x_ticks)))
    for idx, dist in enumerate(node_data):
        dist_arr = np.pad(  # 0-pads smaller distributions to fit heatmap y-axis length
            np.array(dist),
            (0, len(y_ticks) - len(dist)),
            "constant",
            constant_values=(0),
        )
        data[:, idx] = dist_arr[::-1]
    return {"x_ticks": x_ticks, "y_ticks": y_ticks, "data": data.astype(int)}


def combine_biosnoop(
    result_data: List[Dict[str, Union[str, Dict[str, float]]]]
) -> Dict[str, Union[List[str], Dict[str, List[float]]]]:
    """Combines biosnoop results into one dictionary."""
    data: Dict[str, List[float]] = defaultdict(lambda: [])
    nodes: List[str] = []
    for res in result_data:
        data["read_avg_latency"].append(res["data"]["read_avg_latency"])
        data["write_avg_latency"].append(res["data"]["write_avg_latency"])
        nodes.append(res["node"])
    return {"nodes": nodes, "data": data}
