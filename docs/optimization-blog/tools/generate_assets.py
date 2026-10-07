#!/usr/bin/env python3
"""Validate retained observations and regenerate deterministic SVGs and a table."""

from collections import defaultdict
from html import escape
import json
import math
from pathlib import Path
import statistics

BLOG = Path(__file__).resolve().parents[1]
FIGURES = BLOG / "figures"
LABELS = {"table": "Robust table", "parser-word": "First-word reuse",
          "temperature": "Temperature word", "pool": "Buffer reuse", "simd": "Pooled AVX2"}
BLUE = "#245b91"
GREEN = "#23734e"


def svg_start(width, height, title, description):
    return [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
            f'<title id="title">{escape(title)}</title>', f'<desc id="desc">{escape(description)}</desc>',
            '<rect width="100%" height="100%" fill="#ffffff"/>',
            '<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#18252f;font-size:13px}.muted{fill:#52616b;font-size:12px}.title{font-size:21px;font-weight:bold}.grid{stroke:#dfe5e8;stroke-width:1}</style>']


def text(parts, x, y, label, css="", anchor="start"):
    parts.append(f'<text x="{x:.2f}" y="{y:.2f}" class="{css}" text-anchor="{anchor}">{escape(str(label))}</text>')


def line(parts, x1, y1, x2, y2, color="#dfe5e8", width=1):
    parts.append(f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" stroke="{color}" stroke-width="{width}"/>')


def write_svg(name, parts):
    (FIGURES / name).write_text("\n".join(parts + ["</svg>"]) + "\n")


def observations(window, variant):
    return [r["seconds"] for b in window["confirmation_blocks"] if b["variant"] == variant for r in b["runs"]]


def reduction(window):
    a = statistics.mean(observations(window, window["control"]))
    b = statistics.mean(observations(window, window["candidate"]))
    return 100 * (1 - b / a)


def validate(data):
    assert len(data["windows"]) == 14
    total = 0
    for w in data["windows"]:
        a, b = w["control"], w["candidate"]
        assert [block["variant"] for block in w["confirmation_blocks"]] == [a, b, b, a]
        assert w["null_gate"]["pass"]
        threshold = w["null_gate"]["threshold_percent"]
        assert w["null_gate"]["max_abs_drift_percent"] <= threshold
        assert all(abs(value) <= threshold for value in w["order_drift_percent"].values())
        expected_hash = data["corpora"][w["corpus"]]["oracle_sha256"]
        for block in w["confirmation_blocks"]:
            assert len(block["runs"]) == 5 and block["warmups_excluded"] == 2
            for run in block["runs"]:
                assert run["seconds"] > 0
                assert run["complete_process_seconds"] >= run["seconds"]
                assert run["returncode"] == 0 and run["exact"]
                assert run["output_sha256"] == expected_hash
                total += 1
        for variant in [a, b]:
            times = observations(w, variant)
            summary = w["summary"][variant]
            assert len(times) == summary["n"] == 10
            assert math.isclose(statistics.mean(times), summary["mean_seconds"], abs_tol=1e-12)
            assert math.isclose(statistics.median(times), summary["median_seconds"], abs_tol=1e-12)
            cv = 100 * statistics.stdev(times) / statistics.mean(times)
            assert math.isclose(cv, summary["cv_percent"], abs_tol=1e-9)
        assert len(w["source_report_sha256"]) == len(w["quiet_report_sha256"]) == 64
    assert total == 280
    return total


def overview(windows):
    width, height = 1080, 160 + 36 * len(windows)
    parts = svg_start(width, height, "Measured runtime reductions",
                      "Every accepted Linux confirmation window is shown separately. Bars start at zero and show same-window runtime reduction; they are not cumulative. All windows include ten measured runs per variant.")
    text(parts, 24, 34, "Accepted changes: same-window runtime reduction", "title")
    text(parts, 24, 58, "Ryzen 7 5800X · 1 billion rows · 10 observations per variant · repeats kept separately", "muted")
    left, right, top = 360, 980, 100
    for tick in range(0, 51, 10):
        x = left + (right - left) * tick / 50
        line(parts, x, top - 10, x, height - 62)
        text(parts, x, height - 41, f"{tick}%", "muted", "middle")
    for i, w in enumerate(windows):
        y = top + i * 36
        number = w["window"].split("-")[-1]
        label = f'{LABELS[w["experiment"]]} · {"10K" if w["corpus"] == "10k" else "standard"} / {number}'
        text(parts, left - 12, y + 9, label, anchor="end")
        value = reduction(w)
        length = (right - left) * value / 50
        color = GREEN if w["corpus"] == "10k" else BLUE
        parts.append(f'<rect x="{left}" y="{y - 6}" width="{length:.3f}" height="22" rx="2" fill="{color}"/>')
        text(parts, left + length + 9, y + 9, f"{value:.3f}%")
    text(parts, 24, height - 12, "Blue: standard · Green: 10K · Each row has its own baseline. These gains cannot be added or multiplied.", "muted")
    write_svg("runtime-reductions.svg", parts)


def runtime_figure(experiment, windows):
    width, height = 1080, 116 + 154 * len(windows)
    parts = svg_start(width, height, LABELS[experiment] + " observations",
                      "Dots are all individual measured process runtimes; vertical marks show arithmetic means. Each panel uses its own labeled zoomed horizontal axis in seconds. No outliers are removed.")
    text(parts, 24, 34, LABELS[experiment] + ": every confirmation observation", "title")
    text(parts, 24, 58, "Dots: measured runs · Vertical mark: arithmetic mean · Each panel has its own zoomed time axis", "muted")
    left, right = 240, 800
    for panel, w in enumerate(windows):
        y = 95 + panel * 154
        variants = [w["control"], w["candidate"]]
        combined = sum((observations(w, variant) for variant in variants), [])
        lower, upper = min(combined), max(combined)
        padding = max((upper - lower) * 0.16, 0.008)
        lower, upper = lower - padding, upper + padding
        scale = lambda value: left + (value - lower) / (upper - lower) * (right - left)
        corpus = "10K" if w["corpus"] == "10k" else "Standard"
        number = w["window"].split("-")[-1]
        text(parts, 24, y, f"{corpus} · window {number}")
        text(parts, 850, y, f"{reduction(w):.3f}% less time")
        for tick_index in range(5):
            tick = lower + (upper - lower) * tick_index / 4
            x = scale(tick)
            line(parts, x, y + 13, x, y + 100)
            text(parts, x, y + 116, f"{tick:.3f}", "muted", "middle")
        for row, variant in enumerate(variants):
            center = y + 35 + row * 43
            text(parts, left - 16, center + 4, "Baseline" if row == 0 else "Candidate", anchor="end")
            color = BLUE if row == 0 else GREEN
            times = observations(w, variant)
            for index, value in enumerate(times):
                offset = (index % 5 - 2) * 3.8
                parts.append(f'<circle cx="{scale(value):.3f}" cy="{center + offset:.3f}" r="3.7" fill="{color}" fill-opacity="0.78"><title>{escape(variant)}: {value:.9f} seconds</title></circle>')
            average = statistics.mean(times)
            line(parts, scale(average), center - 14, scale(average), center + 14, color, 3)
            text(parts, 850, center + 4, f"mean {average:.6f} s")
        text(parts, 24, y + 136, f'Fresh null max drift: {w["null_gate"]["max_abs_drift_percent"]:.3f}% · 2% declared gate · host/order checks passed', "muted")
        line(parts, 24, y + 144, 1040, y + 144)
    text(parts, 24, height - 12, "Horizontal scale is seconds and does not start at zero; compare within each panel. n=10 per variant; no trimming.", "muted")
    write_svg(experiment + "-runtimes.svg", parts)


def memory_figure(memory):
    width, height = 1080, 340
    parts = svg_start(width, height, "Buffer reuse reduces allocation volume",
                      "Instrumented diagnostic runs show total allocation in decimal gigabytes. The horizontal axis is logarithmic, from 0.01 to 20 gigabytes; bar lengths are not linear ratios.")
    text(parts, 24, 34, "Buffer reuse: total allocation volume", "title")
    text(parts, 24, 58, "Instrumented diagnostic runs · decimal GB · logarithmic horizontal axis", "muted")
    left, right = 240, 850
    lower, upper = 0.01, 20
    scale = lambda value: left + math.log10(value / lower) / math.log10(upper / lower) * (right - left)
    for tick in [0.01, 0.1, 1, 10, 20]:
        x = scale(tick)
        line(parts, x, 82, x, 270)
        text(parts, x, 293, f"{tick:g}", "muted", "middle")
    for index, key in enumerate(["standard/baseline", "standard/pool", "10k/baseline", "10k/pool"]):
        y = 101 + index * 43
        value = memory["variants"][key]["total_alloc_bytes"] / 1e9
        text(parts, left - 16, y + 7, key.replace("/", " · "), anchor="end")
        color = GREEN if key.endswith("pool") else BLUE
        parts.append(f'<rect x="{left}" y="{y - 9}" width="{scale(value) - left:.3f}" height="25" rx="2" fill="{color}"/>')
        text(parts, scale(value) + 10, y + 7, f"{value:.6f} GB")
    text(parts, 24, 326, "Total allocated over a run is distinct from live heap and RSS. Timing comparisons use separate uninstrumented runs.", "muted")
    write_svg("pool-allocations.svg", parts)


def measurement_table(windows):
    rows = ["# Recomputed acceptance measurements", "",
            "Each mean uses all ten `seconds` observations per version, with two excluded warmups per block. Null and order results pass the declared 2% bound. Host validity comes from the original reports.", "",
            "| Change | Corpus / window | Baseline mean (s) | Candidate mean (s) | Runtime reduction | Null max drift | Baseline / candidate order drift |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- |"]
    for w in windows:
        a, b = w["control"], w["candidate"]
        ma, mb = statistics.mean(observations(w, a)), statistics.mean(observations(w, b))
        drift = w["order_drift_percent"]
        label = ("10K" if w["corpus"] == "10k" else "Standard") + " / " + w["window"].split("-")[-1]
        rows.append(f'| {LABELS[w["experiment"]]} | {label} | {ma:.6f} | {mb:.6f} | {reduction(w):.3f}% | {w["null_gate"]["max_abs_drift_percent"]:.3f}% | {drift[a]:+.3f}% / {drift[b]:+.3f}% |')
    rows += ["", "[Raw observations](timings.json) retain the source records. [Part 2](../02-measuring-improvements.md) explains the method. Each row is a separate comparison, not part of a cumulative gain.", ""]
    (BLOG / "data/measurements.md").write_text("\n".join(rows))


def main():
    data = json.loads((BLOG / "data/timings.json").read_text())
    memory = json.loads((BLOG / "data/memory.json").read_text())
    total = validate(data)
    FIGURES.mkdir(exist_ok=True)
    groups = defaultdict(list)
    for window in data["windows"]:
        groups[window["experiment"]].append(window)
    overview(data["windows"])
    for experiment, windows in groups.items():
        runtime_figure(experiment, windows)
    memory_figure(memory)
    measurement_table(data["windows"])
    print(f"Verified {total} observations in {len(data['windows'])} windows; generated 7 SVG figures and the measurement table.")


if __name__ == "__main__":
    main()
