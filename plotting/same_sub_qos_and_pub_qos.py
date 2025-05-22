import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv('logs/analyser/report.csv')
df = df[df['sub_qos'] == df['pub_qos']]
print(df)
# Show top rows
# display_dataframe_to_user(name="Report Data Preview", dataframe=df)

plt.scatter(list(range(df.shape[0])), df['mean_msg_loss'])
plt.show()


# # 1. Plot mean_msg_loss vs pub_qos for different delays.
# pivot_loss = df.pivot_table(
#     index='delay', columns='pub_qos', values='mean_msg_loss', aggfunc='mean'
# )
# plt.figure()
# pivot_loss.plot(marker='o')
# plt.title("Mean Message Loss % vs Delay by Publisher QoS")
# plt.xlabel("Delay (ms)")
# plt.ylabel("Mean Loss %")
# plt.tight_layout()
# plt.show()
