import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from math import pi
from pathlib import Path

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
summary_path = base_dir / "reports" / "tritone_multi_artist_report.xlsx"
output_dir = base_dir / "reports" / "figures_advanced"
output_dir.mkdir(parents=True, exist_ok=True)

# === Load summary ===
df = pd.read_excel(summary_path, sheet_name="Summary")

# === Color palette ===
colors = ["#00b894", "#0984e3", "#6c5ce7", "#fab1a0"]

# --------------------------------------------------
# STACKED BAR: Matched vs Unmatched
# --------------------------------------------------
df["Unmatched"] = df["Total Tracks"] - df["Matched"]
fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(df["Artist"], df["Matched"], color="#74b9ff", label="Matched")
ax.bar(df["Artist"], df["Unmatched"], bottom=df["Matched"], color="#dfe6e9", label="Unmatched")
ax.set_title("Matched vs Unmatched Works per Artist", fontsize=14, pad=10)
ax.set_xlabel("Artist")
ax.set_ylabel("Track Count")
ax.legend()
for i, val in enumerate(df["Total Tracks"]):
    ax.text(i, val + 10, f"{val}", ha="center", fontsize=9)
plt.tight_layout()
stacked_path = output_dir / "stacked_bar_matched_unmatched.png"
plt.savefig(stacked_path, dpi=400, bbox_inches="tight")
plt.close()

# --------------------------------------------------
# BUBBLE CHART: Total vs Coverage
# --------------------------------------------------
fig, ax = plt.subplots(figsize=(6, 6))
sizes = (df["Matched"] / df["Total Tracks"]) * 2500  # bubble size scaled
scatter = ax.scatter(df["Total Tracks"], df["Coverage_%"], s=sizes, c=df["Coverage_%"], cmap="plasma", alpha=0.7, edgecolors="black")
for i, row in df.iterrows():
    ax.text(row["Total Tracks"], row["Coverage_%"] + 2, row["Artist"], ha="center", fontsize=9, weight="medium")
ax.set_title("Catalog Size vs Coverage % (Bubble = Relative Match Volume)", pad=12)
ax.set_xlabel("Total Tracks")
ax.set_ylabel("Coverage (%)")
plt.colorbar(scatter, label="Coverage %")
plt.tight_layout()
bubble_path = output_dir / "bubble_catalog_vs_coverage.png"
plt.savefig(bubble_path, dpi=400, bbox_inches="tight")
plt.close()

# --------------------------------------------------
# RADAR CHART: Multi-Metric Comparison
# --------------------------------------------------
metrics = ["Total Tracks", "Matched", "Coverage_%"]
num_vars = len(metrics)
angles = [n / float(num_vars) * 2 * pi for n in range(num_vars)]
angles += angles[:1]  # close loop

fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
for i, (_, row) in enumerate(df.iterrows()):
    values = [row[m] for m in metrics]
    values += values[:1]
    ax.plot(angles, values, label=row["Artist"], linewidth=2)
    ax.fill(angles, values, alpha=0.15)
ax.set_xticks(angles[:-1])
ax.set_xticklabels(metrics)
ax.set_title("Radar Chart: Comparative Metrics per Artist", pad=15)
ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1))
plt.tight_layout()
radar_path = output_dir / "radar_multi_metric.png"
plt.savefig(radar_path, dpi=400, bbox_inches="tight")
plt.close()

# --------------------------------------------------
print("Advanced summary visuals generated successfully!")
print(f"Stacked Bar: {stacked_path}")
print(f"Bubble Chart: {bubble_path}")
print(f"Radar Chart: {radar_path}")
