import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import os

# Let's create a sample dataframe
data = {
    "property_id": [1, 1, 1, 1, 2, 2, 2, 2],
    "month_end": [
        "2023-05-01",
        "2023-06-01",
        "2023-07-01",
        "2023-08-01",
        "2023-05-01",
        "2023-06-01",
        "2023-07-01",
        "2023-08-01",
    ],
    "total_revenue": [
        1250000,
        2000000,
        1850000,
        1950000,
        1800000,
        1850000,
        1900000,
        2100000,
    ],
}

os.makedirs("charts", exist_ok=True)

df = pd.DataFrame(data)
df["month_end"] = pd.to_datetime(df["month_end"])  # converting to datetime


def draw_charts(df):
    color = (0.9677975592919913, 0.44127456009157356, 0.5358103155058701)

    # Process each property
    for property_id in df["property_id"].unique():
        # Get the data for this property
        property_data = df[df["property_id"] == property_id].sort_values(by="month_end")

        # Create a line plot
        plt.figure(figsize=(8, 6))
        ax = sns.lineplot(
            x="month_end",
            y="total_revenue",
            data=property_data,
            marker="o",
            sort=False,
            color=color,
        )

        # Add labels to the points
        for line in ax.lines:
            for x_value, y_value in zip(line.get_xdata(), line.get_ydata()):
                label = f"${y_value / 100:,.0f}"
                ax.text(
                    x_value,
                    y_value + 0.05 * (ax.get_ylim()[1] - ax.get_ylim()[0]),
                    label,
                    color="white",
                    weight="bold",
                    ha="center",
                    bbox=dict(
                        facecolor=color, edgecolor=color, boxstyle="round,pad=0.5"
                    ),
                )

        # Set the title
        plt.title("Revenue")

        # Set x-axis and y-axis labels
        ax.set_xlabel("")
        ax.set_ylabel("")

        # Format y-axis labels as dollars
        formatter = ticker.FuncFormatter(lambda x, pos: f"${x / 100:,.0f}")
        ax.yaxis.set_major_formatter(formatter)

        # Format x-axis labels as month, year
        ax.xaxis.set_major_locator(mdates.MonthLocator())  # Set major ticks on months
        # ax.xaxis.set_minor_locator(mdates.WeekdayLocator())  # Set minor ticks on weekdays
        ax.xaxis.set_major_formatter(
            mdates.DateFormatter("%B, %Y")
        )  # Format labels as 'Month, Year'

        # Increase y-axis limits to add space
        y_min, y_max = ax.get_ylim()
        ax.set_ylim(y_min - 0.2 * (y_max - y_min), y_max + 0.2 * (y_max - y_min))

        # Remove gridlines
        ax.grid(False)
        sns.despine()

        # Increase left margin for more spacing
        plt.subplots_adjust(left=0.15)

        # Show the plot
        # plt.show()
        plt.savefig(f"charts/revenue_property_{property_id}.png")


draw_charts(df)
