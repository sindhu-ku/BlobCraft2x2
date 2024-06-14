import sys
import json
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import os

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
    plot_index = 0  # Track the actual number of plots
    num_axs = len(axs)

    for var_index in range(start_index, end_index):
        var = variables[var_index]
        ax = axs[plot_index]
        plot_index += 1

        #group by tag columns if present
        if tag_columns:
            for tag_values, group in df.groupby(tag_columns):
                group_sorted = group.sort_values(by='time')
                #group_sorted = group_sorted.dropna(subset=[var])  # Drop NaN values for the current variable
                if not group_sorted.empty:
                    ax.plot(group_sorted['time'], group_sorted[var], marker='.', linestyle='-', label=str(tag_values))
            ax.legend(fontsize=12 + (10*(num_axs-1)))
        else:
            df_filtered = df.dropna(subset=[var])
            ax.plot(df_filtered['time'], df_filtered[var], marker='.', linestyle='-')

        ax.set_ylabel(var, fontsize=14 + (10*(num_axs-1)))

        ax.tick_params(axis='x', labelsize=12 +(10*(num_axs-1)))
        ax.tick_params(axis='y', labelsize=12 +(10*(num_axs-1)))

        if plot_index < len(axs):
            ax.set_xticks([])
            ax.set_xlabel('')

    if plot_index > 0:
        axs[plot_index - 1].set_xlabel('Time',fontsize=14 + (10*(num_axs-1)))

def plot_data(df, variables, tag_columns, json_filename):

    base_filename = os.path.basename(json_filename)
    base_filename = base_filename.rsplit('_', 2)[0]
    max_subplots_per_canvas = 4  # 4 subplots per canvas
    num_plots = len(variables)

    variables_with_data = [var for var in variables if not df.dropna(subset=[var]).empty]
    print(f"Plotting {variables_with_data} from {base_filename}")
    num_plots_with_data = len(variables_with_data)
    num_figures = (num_plots_with_data + max_subplots_per_canvas - 1) // max_subplots_per_canvas


    #group by tag columns if present
    if tag_columns:
        sorted_groups = df.groupby(tag_columns).size().sort_values(ascending=False).index
        chunks = [sorted_groups[i:i + 10] for i in range(0, len(sorted_groups), 10)]

        for chunk_num, chunk in enumerate(chunks): #for every group of 10 if there two many tags
            chunk_df = df[df[tag_columns].apply(tuple, axis=1).isin(chunk)]
            chunk_variables = extract_variables(chunk_df.to_dict(orient='records'), ['time', 'tags'])

            for fig_index in range(num_figures):
                num_active_plots = min(max_subplots_per_canvas, num_plots_with_data - fig_index * max_subplots_per_canvas)
                fig, axs = plt.subplots(num_active_plots, 1, figsize=(12*num_active_plots, 7 * num_active_plots))
                if num_active_plots == 1:
                    axs = [axs]

                plot_subplots(fig_index, num_plots_with_data, max_subplots_per_canvas, chunk_variables, chunk_df, axs, tag_columns)

                plt.tight_layout()
                plt.savefig(f"Plots/{base_filename}_{chunk_num}_subplot_{fig_index + 1}.png")
                #plt.show()
    else:
        for fig_index in range(num_figures):
            num_active_plots = min(max_subplots_per_canvas, num_plots_with_data - fig_index * max_subplots_per_canvas)
            fig, axs = plt.subplots(num_active_plots, 1, figsize=(12*num_active_plots, 7 * num_active_plots))
            if num_active_plots == 1:
                axs = [axs]

            plot_subplots(fig_index, num_plots_with_data, max_subplots_per_canvas, variables_with_data, df, axs, tag_columns)

            plt.tight_layout()
            plt.savefig(f"Plots/{base_filename}_subplot_{fig_index + 1}.png")
            #plt.show()

def main(json_filename):
    data = read_json_file(json_filename)
    df = create_dataframe(data)
    tag_columns = [col for col in df.columns if col.startswith('tags.')]
    variables = extract_variables(data, ['time', 'tags'])

    plot_data(df, variables, tag_columns, json_filename)

if __name__ == "__main__":
    json_filename = sys.argv[1]
    main(json_filename)
