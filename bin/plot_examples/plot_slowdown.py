""" Does a boxplot chat comparison of the first seed experiments with wideLong, 
longWide, and floodplain workflows. Including, 2wf/h, 6wf/h, 60wf/h. It covers
wfs runtime, turnaround, wait time, stretch factor. It covers three workflow
manipulation techniques: wf aware backfilling, single job, and dependencies.

Analysis focuses only on the job slow down divided in thre job ranges:
- 0-48 c.h
- 48-990 c.h
- 906- c.h

"""


from orchestration import get_central_db
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from plot import (plot_multi_exp_boxplot, produce_plot_config, extract_results,
                  gen_trace_ids_exps)


# remote use no Display
import matplotlib
matplotlib.use('Agg')


db_obj = get_central_db()


edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}

file_name_edges = {0: "small", 48*3600:"medium", 
            960*3600:"large"}

trace_id_rows = []
base_exp=3189
base_trace_id=4166

exp=ExperimentDefinition()
exp.load(db_obj, base_exp)
core_seconds_edges=exp.get_machine().get_core_seconds_edges()



trace_id_rows= gen_trace_ids_exps(base_trace_id, base_exp,
                                      group_size=3,
                                      group_count=5,
                                      block_count=6,
                                      group_jump=18)
 

time_labels = ["", "", "10%", "", "", "25%", "", 
               "", "50%", "", "", "75%", "", 
               "",  "100%", ""]
manifest_label=["floodP", "longW", "wideL",
                "cybers", "sipht", "montage"]

result_type="jobs_slowdown"

y_limits_dic={"[0,48] core.h": (1, 1000),
      "(48, 960] core.h":(1,100),
      "(960, inf.) core.h":(1,20)}

target_dir="percent"

grouping=[1,3,3,3,3,3]

colors, hatches, legend = produce_plot_config(db_obj, trace_id_rows)

name="Slowdown"

for edge in core_seconds_edges:
    edge_result_type=ResultTrace.get_result_type_edge(edge,result_type)
    print "Loading "+edge_result_type
    edge_plot_results = extract_results(db_obj, trace_id_rows,
                                        edge_result_type)
    edge_formated=edge_keys[edge]
    title="Jobs slowdow: {0}".format(edge_formated)
    y_limits=y_limits_dic[edge_formated]
    print "Plotting figure"
    plot_multi_exp_boxplot(
        name=title,
        file_name=target_dir+"/percent-slow_down_jobs-{0}.png".format(
                                                        file_name_edges[edge]),
        title=title,
        exp_rows=edge_plot_results,
        y_axis_labels=manifest_label,
        x_axis_labels=time_labels,
        y_axis_general_label=name,
        grouping=grouping,
        colors=colors,
        hatches=hatches,
        y_limits=y_limits,
        y_log_scale=True,
        legend=legend,
        y_tick_count=3,
        y_tick_count_alt=3,
        grouping_alt=grouping,
        percent_diff=True
        ) 

    