""" It does an analysis on the slowdown for the experiments with workflows
submitted by period and % of submitted core hours.

For each block of experiments (workflow and submission policy) it presents
the boxlpot of the non workflow slowdown and barchart of the deviation of
an experiment from it.  

"""

from orchestration import get_central_db
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from plot import (plot_multi_exp_boxplot, produce_plot_config, extract_results,
                  gen_trace_ids_exps, calculate_diffs,plot_multi_bars,
                  join_rows)

# remote use no Display
import matplotlib
matplotlib.use('Agg')


db_obj = get_central_db()


edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}

file_name_edges = {0: "small", 48*3600:"medium", 
            960*3600:"large"}


base_exp=2116

exp=ExperimentDefinition()
exp.load(db_obj, base_exp)
core_seconds_edges=exp.get_machine().get_core_seconds_edges()


"""We only take the realistic workflows for this one, that is why the trace_id
is weird."""
base_trace_id=2126
"""
We retrieve first the floodplain, longWait, WaitLong
then we retrieve the cybershake, sipht, montage.
"""

                

block_count=1
group_count=5
floorp_trace_id_percent=2459
cybers_trace_id_percent=2468
montage_trace_id_percent=2474
compare_trace_id_rows=[]
colors_trace_id_rows=[]

block_count=1
for base_trace_id in [floorp_trace_id_percent, cybers_trace_id_percent,
                      montage_trace_id_percent]:
    
    t_compare_trace_id_rows=[]
    t_compare_trace_id_rows=join_rows(t_compare_trace_id_rows,
                                    gen_trace_ids_exps(base_trace_id+90,
                                      inverse=False,group_count=2,
                                      group_jump=18, block_count=block_count,
                                      base_exp_group=base_exp))
    t_compare_trace_id_rows= join_rows(t_compare_trace_id_rows,
                                 gen_trace_ids_exps(base_trace_id,
                                      inverse=False,group_count=group_count-1,
                                      group_jump=18, block_count=block_count,
                                      base_exp_group=base_exp))

    compare_trace_id_rows+=t_compare_trace_id_rows

    t_colors_trace_id_rows = []
    t_colors_trace_id_rows=join_rows(t_colors_trace_id_rows, 
                                  gen_trace_ids_exps(base_trace_id+90,
                                      inverse=False,group_count=2,
                                      group_jump=18, block_count=block_count,
                                      base_exp=None))
    
    t_colors_trace_id_rows= join_rows(t_colors_trace_id_rows,
                                    gen_trace_ids_exps(base_trace_id,
                                      inverse=False,group_count=group_count-1,
                                      group_jump=18, block_count=block_count,
                                      base_exp=None)
                                    )
    colors_trace_id_rows+=t_colors_trace_id_rows
manifest_label=[
                "floorp", "cybers", "montage"
                ]

# block_count=3
# group_count=5

# pre_base_trace_id_percent=2549
# base_trace_id_percent = 2468
# compare_trace_id_rows=(gen_trace_ids_exps(pre_base_trace_id_percent,
#                                       inverse=False,group_count=2,
#                                       group_jump=18, block_count=block_count,
#                                       base_exp_group=base_exp))
# compare_trace_id_rows= join_rows(compare_trace_id_rows,
#                                  gen_trace_ids_exps(base_trace_id_percent,
#                                       inverse=False,group_count=group_count-1,
#                                       group_jump=18, block_count=block_count,
#                                       base_exp_group=base_exp))
# 
# 
# 
# colors_trace_id_rows=(gen_trace_ids_exps(base_trace_id,
#                                       inverse=False,group_count=group_count,
#                                       group_jump=18, block_count=block_count,
#                                       base_exp=None))
# 
# colors_trace_id_rows= join_rows(colors_trace_id_rows,
#                                 gen_trace_ids_exps(base_trace_id_percent,
#                                       inverse=False,group_count=group_count-1,
#                                       group_jump=18, block_count=block_count,
#                                       base_exp=None)
#                                 )
# manifest_label=[
#                 "cybers", "sipht", "montage"
#                 ]


print "IDs", compare_trace_id_rows
# 
# print colors_trace_id_rows

time_labels = [
               "", "1%", "",
               "", "5%", "",  
               "", "10%", "",
               "", "25%", "",
               "", "50%", "",
               "", "75%", ""]





base_exp_trace_ids= [[base_exp] for i in manifest_label]

result_type="jobs_slowdown"

y_limits_dic={"[0,48] core.h": (0.5, 10),
      "(48, 960] core.h":(0.5,10),
      "(960, inf.) core.h":(0.5,10)}

y_limits_title={"[0,48] core.h": "Small jobs",
      "(48, 960] core.h": "Medium jobs",
      "(960, inf.) core.h": "Large jobs"}

target_dir="slowdown"

grouping=[
          ["bar", "bar", "bar"],["bar", "bar", "bar"],
          ["bar", "bar", "bar"],["bar", "bar", "bar"],
          ["bar", "bar", "bar"],["bar", "bar", "bar"]
        ]

colors, hatches, legend = produce_plot_config(db_obj, colors_trace_id_rows)

name="Slowdown"

for edge in core_seconds_edges:
    edge_result_type=ResultTrace.get_result_type_edge(edge,result_type)
    print "Loading "+edge_result_type
    diff_source_results = extract_results(db_obj, compare_trace_id_rows,
                                          edge_result_type,
                                          fill_none=True)
    print "VALUES:", diff_source_results
    diffs_results = calculate_diffs(diff_source_results, base_index=0, 
                                    group_count=4,
                                    speedup=True)
    print "DIFFS:", diffs_results

    plot_results=diffs_results
   
  
    edge_formated=edge_keys[edge]
    title="Workflows impact on slowdown: {0}".format(
                                        y_limits_title[edge_formated])
    y_limits=y_limits_dic[edge_formated]
    print "Plotting figure"
    
    plot_multi_bars(
               name=title,
               file_name=target_dir+"/compare-slow_down_jobs-{0}.png".format(
                                                    file_name_edges[edge]),
               title=title, 
               exp_rows=diffs_results,
               type_rows=grouping,
               y_axis_labels=manifest_label,
               x_axis_labels=time_labels,
               y_axis_general_label=None,
               colors=colors,
               hatches=hatches,
               y_limits=y_limits,
               y_log_scale=True,
               legend=legend,
               y_tick_count=None,
               do_auto_label=False,
               ref_line=1.0)
    

    