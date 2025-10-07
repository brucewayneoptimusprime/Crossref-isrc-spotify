import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# === Paths ===
base_dir = Path(r"C:\Tritone_Spotify\Data_analytics\tritone-assignment")
summary_path = base_dir / "reports" / "tritone_multi_artist_report.xlsx"
output_dir = base_dir / "reports" / "figures"
output_dir.mkdir(parents=True, exist_ok=True)

# === Load Summary Sheet ===
df = pd.read_excel(summary_path, sheet_name="Summary")

# === Coverage Bar Chart ===
plt.figure(figsize=(8, 5))
bars = plt.bar(df["Artist"], df["Coverage_%"], edgecolor="black")
plt.title("Coverage of Unclaimed Works per Artist", fontsize=14, pad=12)
plt.xlabel("Artist", fontsize=12)
plt.ylabel("Coverage (%)", fontsize=12)
plt.xticks(rotation=25)
plt.ylim(0, max(df["Coverage_%"]) * 1.2)

# annotate bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, height + 1, f"{height:.1f}%", ha="center", va="bottom", fontsize=10)

plt.tight_layout()
coverage_chart_path = output_dir / "coverage_comparison.png"
plt.savefig(coverage_chart_path, dpi=300)
plt.close()

# === Composition Pie Chart (Matched vs Unmatched) ===
for _, row in df.iterrows():
    artist = row["Artist"]
    matched, total = row["Matched"], row["Total Tracks"]
    unmatched = total - matched
    plt.figure(figsize=(4.5, 4.5))
    plt.pie(
        [matched, unmatched],
        labels=[f"Matched ({matched})", f"Unmatched ({unmatched})"],
        autopct="%1.1f%%",
        startangle=120,
        colors=["#4CAF50", "#E0E0E0"],
        wedgeprops={"edgecolor": "white"},
    )
    plt.title(f"{artist} — Catalog Composition", fontsize=11)
    plt.tight_layout()
    out_path = output_dir / f"{artist.replace(' ', '_')}_composition.png"
    plt.savefig(out_path, dpi=300)
    plt.close()

print("Charts generated successfully!")
print(f"Coverage chart: {coverage_chart_path}")
print(f"Individual composition pies saved to: {output_dir}")
