import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import io


def draw_line_chart(property_data, chart_type):
    # Define color for the chart
    color = (0.9677975592919913, 0.44127456009157356, 0.5358103155058701)

    # Define settings for each chart type
    chart_settings = {
        "total_revenue": {
            "title": "Revenue",
            "label_format": lambda x: f"${x/100:,.0f}",
            "y_format": lambda x: f"${x/100:,.0f}",
            "ylim_max": float("inf"),
        },
        "occupancy_rate": {
            "title": "Occupancy Rate",
            "label_format": lambda x: f"{x * 100:.0f}%",
            "y_format": lambda x: f"{x * 100:.0f}%",
            "ylim_max": 1,
        },
        "stars": {
            "title": "Star Ratings",
            "label_format": lambda x: f"{x:.2f}",
            "y_format": lambda x: f"{x:.2f}",
            "ylim_max": 5,
        },
    }

    settings = chart_settings[chart_type]

    # Create a line plot
    plt.figure(figsize=(8, 6))
    ax = sns.lineplot(
        x="month_end",
        y=chart_type,
        data=property_data,
        marker="o",
        sort=False,
        color=color,
    )

    # Add labels to the points
    for line in ax.lines:
        for x_value, y_value in zip(line.get_xdata(), line.get_ydata()):
            label = settings["label_format"](y_value)
            ax.text(
                x_value,
                y_value + 0.05 * (ax.get_ylim()[1] - ax.get_ylim()[0]),
                label,
                color="white",
                weight="bold",
                ha="center",
                bbox=dict(facecolor=color, edgecolor=color, boxstyle="round,pad=0.5"),
            )

    # Format y-axis labels
    formatter = ticker.FuncFormatter(lambda x, pos: settings["y_format"](x))
    ax.yaxis.set_major_formatter(formatter)

    # Increase y-axis limits to add space
    y_min, y_max = ax.get_ylim()
    y_min_new = y_min - 0.2 * (y_max - y_min)
    y_max_new = y_max + 0.2 * (y_max - y_min)
    ax.set_ylim(y_min_new, min(y_max_new, settings["ylim_max"]))

    # Set the title
    plt.title("Revenue")

    # Set x-axis and y-axis labels
    ax.set_xlabel("")
    ax.set_ylabel("")

    # Format x-axis labels as month, year
    ax.xaxis.set_major_locator(mdates.MonthLocator())  # Set major ticks on months
    ax.xaxis.set_major_formatter(
        mdates.DateFormatter("%B, %Y")
    )  # Format labels as 'Month, Year'

    # Remove gridlines
    ax.grid(False)
    sns.despine()

    # Increase left margin for more spacing
    plt.subplots_adjust(left=0.15)

    # Save the plot
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)
    return buffer
