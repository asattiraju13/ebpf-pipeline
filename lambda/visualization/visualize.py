"""Plotly graphing functions."""

from copy import deepcopy
from typing import Dict, List, Union

import numpy as np
import numpy.typing as npt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def visualize_ebpf(data: Dict[str, Union[pd.DataFrame, Dict]], filepath: str) -> None:
    """Master function to construct entire plot and assign subplots for each script."""
    fig = make_subplots(
        rows=4,
        cols=2,
        horizontal_spacing=0.15,
        vertical_spacing=0.1,
        specs=[
            [{"colspan": 2, "type": "table"}, None],
            [{"colspan": 2}, None],
            [{"colspan": 2}, None],
            [{}, {}],
        ],
        subplot_titles=(
            "Running Processes on Nodes",
            "Average Latencies of Read / Write Disk I/O Operations across Nodes",
            "Average Latencies of System Calls across Nodes",
            "Distribution of Run Queue Latency across Nodes (Profiled for 25s)",
            "Distribution of Block I/O Latency across Nodes (Profiled for 25s)",
        ),
    )

    fig.add_trace(visualize_execsnoop(data["execsnoop"]), row=1, col=1)

    for trace in visualize_biosnoop(data["biosnoop"]):
        fig.add_trace(trace, row=2, col=1)

    for trace in visualize_syscount(data["syscount"]):
        fig.add_trace(trace, row=3, col=1)

    fig.add_trace(
        visualize_heatmap(
            data["biolatency"], colorbar=dict(len=0.18, x=0.425, y=0.085)
        ),
        row=4,
        col=1,
    )

    fig.add_trace(
        visualize_heatmap(data["runqlat"], colorbar=dict(len=0.18, x=1, y=0.085)),
        row=4,
        col=2,
    )

    fig.update_layout(
        height=2000,
        showlegend=False,
        title_text="Performance Comparisons across Nodes",
    )

    # Update xaxis properties
    fig.update_xaxes(title_text="Node names", row=2, col=1)
    fig.update_xaxes(title_text="Node names", row=3, col=1)
    fig.update_xaxes(title_text="Node names", row=4, col=1)
    fig.update_xaxes(title_text="Node names", row=4, col=2)

    # Update yaxis properties
    fig.update_yaxes(title_text="Latency (ms)", row=2, col=1)
    fig.update_yaxes(title_text="Latency (us)", row=3, col=1)
    fig.update_yaxes(title_text="Latency Intervals (us)", row=4, col=1)
    fig.update_yaxes(title_text="Latency Intervals (us)", row=4, col=2)

    fig.write_html(filepath)


def visualize_execsnoop(data: pd.DataFrame) -> go.Table:
    """Table visualization for execsnoop."""
    trace = go.Table(
        header=dict(
            values=list(data.columns),
            fill_color="paleturquoise",
            align="left",
        ),
        cells=dict(
            values=[data[col] for col in data.columns],
            fill_color="lavender",
            align="left",
        ),
    )
    return trace


def visualize_heatmap(
    data: Dict[str, Union[List[str], npt.NDArray[np.intc]]], colorbar: Dict[str, float]
) -> go.Heatmap:
    """Heatmap visualization for runqlat and biolatency."""
    heatmap = data["data"][::-1]
    hover = deepcopy(heatmap)
    hover = hover.astype(int).astype(str)  # hover data
    for i, hover_dat in enumerate(hover):
        for j, dat in enumerate(hover_dat):
            hover[i, j] = "Count: " + dat

    trace = go.Heatmap(
        z=heatmap,
        x=data["x_ticks"],
        y=data["y_ticks"],
        colorbar=colorbar,
        hoverinfo="text",
        hovertext=hover,
    )
    return trace


def visualize_biosnoop(
    data: Dict[str, Union[List[str], Dict[str, List[float]]]]
) -> List[go.Bar]:
    """Barchart visualization for biosnoop."""
    avg_read_latency = []
    for dat in data["data"]["read_avg_latency"]:
        avg_read_latency.append("Read Latency (ms): " + str(dat))
    avg_write_latency = []
    for dat in data["data"]["write_avg_latency"]:
        avg_write_latency.append("Write Latency (ms): " + str(dat))

    traces = [
        go.Bar(
            name="Read",
            x=data["nodes"],
            y=data["data"]["read_avg_latency"],
            hoverinfo="text",
            hovertext=avg_read_latency,
        ),
        go.Bar(
            name="Write",
            x=data["nodes"],
            y=data["data"]["write_avg_latency"],
            hoverinfo="text",
            hovertext=avg_write_latency,
        ),
    ]
    return traces


def visualize_syscount(
    data: Dict[str, Dict[str, Union[List[str], List[float]]]]
) -> List[go.Box]:
    """Boxplot visualization for syscount."""
    traces = []
    for call in data.keys():
        hover = []  # store hover data
        for idx, node in enumerate(data[call]["node"]):
            hover.append(
                "Node: "
                + str(node)
                + "<br>"
                + "Avg latency: "
                + str(data[call]["latencies"][idx])
            )
        trace = go.Box(
            y=data[call]["latencies"],
            boxpoints="all",
            name=call,
            hoverinfo="text",
            hovertext=hover,
        )
        traces.append(trace)
    return traces
