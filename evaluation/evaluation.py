#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""."""

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set(style='whitegrid', palette='deep')

df = pd.read_csv('data5.csv')
df['throughput'] = df['total_events'].div(df['time'])
df['per_tuple_cost'] = 1.0 / df['throughput']

# df_means = df.groupby(['window_size', 'slide'], as_index=False)
# df_means = df_means[['throughput', 'per_tuple_cost']].mean()

# plot = sns.lineplot(data=df, x='time', y='total_events', hue='window_size')
# plot.set(xscale='log')
# plt.show()

plot = sns.lineplot(data=df, x='time', y='throughput', hue='window_size')
plot.set(xscale='log')
# plot.figure.savefig('throughput_over_time.png')
plt.show()

plot = sns.lineplot(data=df, x='window_size', y='throughput', hue='slide')
plot.set(ylim=(0, 25000), xscale='log')
# plot.figure.savefig('throughput.png')
plt.show()

plot = sns.lineplot(data=df, x='window_size', y='per_tuple_cost', hue='slide')
plot.set(ylim=(0, 0.0002), xscale='log')
# plot.figure.savefig('per_tuple_cost.png')
plt.show()
