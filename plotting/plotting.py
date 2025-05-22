import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv('logs/analyser/report.csv')

# Show top rows
# display_dataframe_to_user(name="Report Data Preview", dataframe=df)

# 1. Plot mean_msg_loss vs pub_qos for different delays.
pivot_loss = df.pivot_table(
    index='delay', columns='pub_qos', values='mean_msg_loss', aggfunc='mean'
)
plt.figure()
pivot_loss.plot(marker='o')
plt.title("Mean Message Loss % vs Delay by Publisher QoS")
plt.xlabel("Delay (ms)")
plt.ylabel("Mean Loss %")
plt.tight_layout()
plt.show()

# 2. Heatmap of mean_gap_ms across pub_qos and delay
heatmap_data = df.pivot_table(index='pub_qos', columns='delay', values='mean_gap_ms', aggfunc='mean')
plt.figure()
plt.imshow(heatmap_data, aspect='auto')
plt.colorbar(label='Mean Gap (ms)')
plt.xticks(ticks=range(len(heatmap_data.columns)), labels=heatmap_data.columns)
plt.yticks(ticks=range(len(heatmap_data.index)), labels=heatmap_data.index)
plt.title("Heatmap of Mean Inter-message Gap")
plt.xlabel("Delay (ms)")
plt.ylabel("Publisher QoS")
plt.tight_layout()
plt.show()

# 3. Duplicate rate distribution histogram
plt.figure()
df['duplication_rate'].hist(bins=20)
plt.title("Distribution of Mean Duplicate % Across Tests")
plt.xlabel("Mean Duplicate %")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

# 4. Scatter of mean_msg_loss vs mean_gap_ms
plt.figure()
plt.scatter(df['mean_msg_loss'], df['mean_gap_ms'])
plt.title("Mean Loss % vs Mean Gap (ms)")
plt.xlabel("Mean Loss %")
plt.ylabel("Mean Gap (ms)")
plt.tight_layout()
plt.show()
