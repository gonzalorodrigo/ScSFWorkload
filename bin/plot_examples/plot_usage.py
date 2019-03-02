""" Plots analysis on the workflow variables for experiments with different
workflow types and different %of workflow core hours in the workload.

Resuls are plotted as barchars that show how much the vas deviate in
single and multi from aware.
"""
import matplotlib

from orchestration import get_central_db
from plot import (extract_usage, join_rows,
                  gen_trace_ids_exps, get_list_rows,
                  plot_multi_bars, produce_plot_config)


# remote use no Display
matplotlib.use('Agg')


db_obj = get_central_db()

pre_base_trace_id_percent = 2549
base_trace_id_percent = 2459


time_labels_long = ["", "1%", "", 
                    "", "5%", "",
                    "", "10%", "",
                    "", "25%", "",
                    "", "50%", "",
                    "", "75%", "",
                    "", "100%", ""]
manifest_label=["floodP", "longW", "wideL",
                "cybers", "sipht", "montage"]

head_file_name="percent"
""" WHEN we set the trace_id to be the grouped one, we need to use the mean=True
so the code takes the mean and not the median.
put the next when we use the grouped.

mean_values = True

"""
mean_values=False


total_trace_id_rows=  join_rows(
                        gen_trace_ids_exps(pre_base_trace_id_percent,   
                                      inverse=False,
                                      group_size=3,
                                      group_jump=18, block_count=6,
                                      group_count=2,
                                      base_exp_group=None),
                         gen_trace_ids_exps(base_trace_id_percent,
                                      inverse=False,
                                      group_size=3,
                                      group_jump=18, block_count=6,
                                      group_count=5,
                                      base_exp_group=None)
                          )


diffs_results = extract_usage(db_obj, total_trace_id_rows, factor=100.0, 
                              mean=mean_values)
diffs_results = get_list_rows(diffs_results, ["utilization",
                                              "corrected_utilization",
                                              "utilization"])
colors, hatches, legend = produce_plot_config(db_obj, total_trace_id_rows)

result_type="utilization"
target_dir="percent"

grouping_types = [["bar", "bar", "bar"],
                  ["bar", "bar", "bar"],
                  ["bar", "bar", "bar"],
                  ["bar", "bar", "bar"],
                  ["bar", "bar", "bar"],
                  ["bar", "bar", "bar"],
                  ["bar", "bar", "bar"]
                  ]

head_file_name="wf_percent-b{0}".format(base_trace_id_percent)
title = "Utilization, percent scenarios"
y_limits=(0,100)


plot_multi_bars(
        name=title,
        file_name=target_dir+"/{0}-{1}-bars.png".format(head_file_name,
                                                           result_type),
        title=title,
        exp_rows=diffs_results,
        y_axis_labels=manifest_label,
        x_axis_labels=time_labels_long,
        y_axis_general_label=result_type,
        type_rows=grouping_types,
        colors=colors,
        hatches=hatches,
        y_limits=y_limits,
        y_log_scale=False,
        legend=legend,
        y_tick_count=3,
        y_tick_count_alt=3,
        ncols=2,
        subtitle="% workflow workload",
        do_auto_label=False
        ) 



