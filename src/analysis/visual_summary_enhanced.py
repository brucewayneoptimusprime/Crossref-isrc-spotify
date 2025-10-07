import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle
from pathlib import Path

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
summary_path = base_dir / "reports" / "tritone_multi_artist_report.xlsx"
output_dir = base_dir / "reports" / "figures_enhanced"
output_dir.mkdir(parents=True, exist_ok=True)

# === Load Data ===
df = pd.read_excel(summary_path, sheet_name="Summary")

# === Style Setup ===
plt.style.use("ggplot")
plt.rcParams.update({
    "figure.facecolor": "#f8f9fa",
    "axes.facecolor": "#ffffff",
    "axes.edgecolor": "#e0e0e0",
    "axes.grid": True,
    "grid.alpha": 0.4,
    "grid.linestyle": "--",
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})

# === Gradient Bar Chart ===
fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.bar(df["Artist"], df["Coverage_%"], color="#556ee6", edgecolor="#222", linewidth=0.6)

# gradient fill simulation
for bar in bars:
    bar.set_facecolor("none")
    x, y = bar.get_xy()
    w, h = bar.get_width(), bar.get_height()
    grad = np.linspace(0.3, 1, 256).reshape(256, 1)
    ax.imshow(grad, extent=[x, x + w, 0, h],
              aspect="auto", origin="lower", cmap="plasma", alpha=0.8, clip_path=bar, clip_on=True)
    ax.add_patch(bar)

# titles and labels
ax.set_title("Unclaimed Rights Coverage per Artist", pad=15, fontweight="bold")
ax.set_xlabel("Artist")
ax.set_ylabel("Coverage (%)")
ax.set_ylim(0, max(df["Coverage_%"]) * 1.25)

# annotate
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, h + 2,
            f"{h:.1f}%", ha="center", va="bottom",
            fontsize=10, color="#111", fontweight="medium")

plt.tight_layout()
coverage_chart_path = output_dir / "coverage_comparison_enhanced.png"
plt.savefig(coverage_chart_path, dpi=400, bbox_inches="tight")
plt.close()

# === Donut Charts (Matched vs Unmatched) ===
for _, row in df.iterrows():
    artist = row["Artist"]
    matched, total = row["Matched"], row["Total Tracks"]
    unmatched = total - matched

    fig, ax = plt.subplots(figsize=(4.2, 4.2))
    wedges, texts, autotexts = ax.pie(
        [matched, unmatched],
        labels=["Matched", "Unmatched"],
        autopct="%1.1f%%",
        startangle=120,
        colors=["#00b894", "#dfe6e9"],
        textprops={"color": "black"},
        pctdistance=0.8
    )

    # draw center hole (donut effect)
    centre_circle = Circle((0, 0), 0.55, fc="white")
    ax.add_artist(centre_circle)

    ax.set_title(f"{artist}\nCatalog Composition", fontsize=12, fontweight="bold")
    plt.tight_layout()
    out_path = output_dir / f"{artist.replace(' ', '_')}_donut.png"
    plt.savefig(out_path, dpi=400, bbox_inches="tight")
    plt.close()

print("Enhanced visuals generated successfully!")
print(f"Gradient coverage chart: {coverage_chart_path}")
print(f"Donut charts saved to: {output_dir}")
