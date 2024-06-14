"""Plot 2x2 Slow controls json data blobs"""
"""Author: Sindhujha Kumaran, s.kumaran@uci.edu"""

import sys
import json
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import pandas as pd

def read_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

def extract_variables(data, exclude_keys):
    if data and isinstance(data[0], dict):
        keys = [key for key in data[0].keys() if key not in exclude_keys and not isinstance(data[0][key], (str, bool))]
        if 'tags' in keys:
            keys.remove('tags')
        return keys
    else:
        return []

def create_dataframe(data):
    df = pd.json_normalize(data)
    df['time'] = pd.to_datetime(df['time'].str[:-6])
    return df

def plot_subplots(fig_index, num_plots, max_subplots_per_canvas, variables, df, axs, tag_columns=None):
    start_index = fig_index * max_subplots_per_canvas
    end_index = min((fig_index + 1) * max_subplots_per_canvas, num_plots)

    for i, var_index in enumerate(range(start_index, end_index)): #subplot per variable
        var = variables[var_index]
        ax = axs[i]

        if tag_columns:
            for tag_values, group in df.groupby(tag_columns):
                group_sorted = group.sort_values(by='time')
                group_sorted = group_sorted.dropna(subset=[var])
                if not group_sorted.empty:
                    ax.plot(group_sorted['time'], group_sorted[var], marker='.', linestyle='-', label=str(tag_values))
            ax.legend()
        else:
            df_filtered = df.dropna(subset=[var])
            ax.plot(df_filtered['time'], df_filtered[var], marker='.', linestyle='-')

        ax.set_ylabel(var, fontsize=12)

        if i < len(axs) - 1:
            ax.set_xticks([])
            ax.set_xlabel('')

    axs[-1].set_xlabel('Time', fontsize=12)

def plot_data(df, variables, tag_columns):
    max_subplots_per_canvas = 4 # 4 subplots per canvas
    num_plots = len(variables)
    num_figures = (num_plots + max_subplots_per_canvas - 1) // max_subplots_per_canvas

    #group by tag columns if present
    if tag_columns:
        sorted_groups = df.groupby(tag_columns).size().sort_values(ascending=False).index #if there are more than 10 sorted groups divide the plots further
        chunks = [sorted_groups[i:i + 10] for i in range(0, len(sorted_groups), 10)]

        for chunk in chunks: #for every group of 10
            chunk_df = df[df[tag_columns].apply(tuple, axis=1).isin(chunk)]
            chunk_variables = extract_variables(chunk_df.to_dict(orient='records'), ['time', 'tags'])

            for fig_index in range(num_figures):
                fig, axs = plt.subplots(min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas), 1,
                                        figsize=(8, 7 * min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas)))
                if min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas) == 1:
                    axs = [axs]

                plot_subplots(fig_index, num_plots, max_subplots_per_canvas, chunk_variables, chunk_df, axs, tag_columns)

                plt.tight_layout()
                plt.show()

    else:
        for fig_index in range(num_figures):
            fig, axs = plt.subplots(min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas), 1,
                                    figsize=(8, 7 * min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas)))
            if min(max_subplots_per_canvas, num_plots - fig_index * max_subplots_per_canvas) == 1:
                axs = [axs]

            plot_subplots(fig_index, num_plots, max_subplots_per_canvas, variables, df, axs, tag_columns)

            plt.tight_layout()
            plt.show()


def main(json_filename):
    data = read_json_file(json_filename)
    df = create_dataframe(data)
    tag_columns = [col for col in df.columns if col.startswith('tags.')]
    variables = extract_variables(data, ['time', 'tags'])

    plot_data(df, variables, tag_columns)

if __name__ == "__main__":
    json_filename = sys.argv[1]
    main(json_filename)
