"""  Plots analysis on the workflow variables for experiments with different
workflow types and different %of workflow core hours in the workload.

Results are plotted as boxplots for the variables in eahc exp. 


"""
from orchestration import get_central_db
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from plot import (plot_multi_exp_boxplot, produce_plot_config, extract_results,
                  gen_trace_ids_exps, get_args)


# remote use no Display
import matplotlib
matplotlib.use('Agg')


db_obj = get_central_db()

base_trace_id_percent, lim = get_args(2459, True)
print "Base Exp", base_trace_id_percent
print "Using analysis of limited workflows:", lim

edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}

trace_id_rows = []
base_exp=170
exp=ExperimentDefinition()
exp.load(db_obj, base_exp)
core_seconds_edges=exp.get_machine().get_core_seconds_edges()



trace_id_rows= gen_trace_ids_exps(base_trace_id_percent,
                                      inverse=False,
                                      group_jump=18, block_count=6,
                                      base_exp_group=None)


time_labels = ["", "10%", "", "", "25%", "", 
               "", "50%", "", "", "75%", "", 
               "",  "100%", ""]
manifest_label=["floodP", "longW", "wideL",
                "cybers", "sipht", "montage"]


y_limits_dic={"[0,48] core.h": (1, 1000),
      "(48, 960] core.h":(1,100),
      "(960, inf.) core.h":(1,20)}

target_dir="percent"

grouping=[ 3,3,3,3,3]

colors, hatches, legend = produce_plot_config(db_obj, trace_id_rows)

head_file_name="wf_percent-box{0}".format(base_trace_id_percent)



for (name, result_type) in zip(["turnaround time (h.)", "wait time(h.)",
                                "runtime (h.)", "stretch factor"],
                          ["wf_turnaround", "wf_waittime", 
                           "wf_runtime", "wf_stretch_factor"]): 

    if lim:
        result_type="lim_{0}".format(result_type)
    print "Loading: {0}".format(name)
    factor=1.0/3600.0
    if result_type in ("wf_stretch_factor", "lim_wf_stretch_factor"):
        factor=None
    edge_plot_results = extract_results(db_obj, trace_id_rows,
                                        result_type, factor=factor,
                                        second_pass=lim)
#     for res_row  in edge_plot_results:
#         print [ x._get("median") for x in res_row]
    
    title="Workflows {0}".format(name)
    y_limits=None
    print "Plotting figure"
    plot_multi_exp_boxplot(
        name=title,
        file_name=target_dir+"/{0}-{1}-boxplot.png".format(head_file_name,
                                                           result_type),
        title=title,
        exp_rows=edge_plot_results,
        y_axis_labels=manifest_label,
        x_axis_labels=time_labels,
        y_axis_general_label=name,
        grouping=grouping,
        colors=colors,
        hatches=hatches,
        y_limits=y_limits,
        y_log_scale=False,
        legend=legend,
        percent_diff=True,
        y_tick_count=3,
        y_tick_count_alt=3,
        y_axis_label_alt="difference"   
        ) 

    