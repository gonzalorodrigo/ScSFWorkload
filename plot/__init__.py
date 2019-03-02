"""
Library to produce specific plots using the nerscsplot library.
"""
from commonLib.nerscPlot import (paintHistogramMulti, paintBoxPlotGeneral,
                                 paintBarsHistogram)
import getopt
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import sys
from array import array
from numpy import ndarray, arange, asarray
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from stats import  NumericStats
from matplotlib.cbook import unique


def get_args(default_trace_id=1, lim=False):
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:ln",
                                   ["id=", "lim", "nolim"])
    except getopt.GetoptError:
        print 'test.py [-i <trace_id>] [-l]'
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-i", "--id"):
            default_trace_id=int(arg)
        elif opt in ("-l", "--lim"):
            lim=True
        elif opt in ("-n", "--nolim"):
            lim=False
    return default_trace_id, lim
 
def profile(data, name, file_name, x_axis_label, x_log_scale=False):
    """
    Produces a png with a histogram and CDF of the numeric values in data.
    Args:
    - data: list of values to analyze.
    - name: string with the title to write on the figure.
    - file_name: string with the file system route where to place the out file.
       No need to include an extension, png will be appended.
    - x_axis_label: Label to ilsutrate the x_axis of the histogram.
    - x_log_scale: if True, x axis uses log scale.
    """
    data_dic = {name:data}
    
    paintHistogramMulti(name, data_dic, bins=100,
                        graphFileName=file_name,
                        labelX=x_axis_label,
                        xLogScale=x_log_scale,
                        labelY="Number Of Jobs")
    
def profile_compare(log_data, trace_data, name, file_name, x_axis_label,
                    x_log_scale=False, filterCut=0):
    """
    Produces a histogram and boxplot comparing two series of values that
    represent a random variable of the original workload data and the same
    random variable of the derived synthetic workload. 
    Args:
    - log_data: list of values of a random variable of a real workload.
    - trace_data: list of values of a random variable of a synthetic workload.
    - name: string with the title to write on the figure.
    - file_name: string with the file system route where to place the out file.
       No need to include an extension, png will be appended. It will be
       used for th histogram file, the boxplot file will have the same name
       with "-boxplot" added.
    - x_axis_label: Label to ilsutrate the x_axis of the histogram.
    - x_log_scale: if True, x axis uses log scale.
    - filterCut: if set to 0, it has no effect. Otherwise, filgerCut will be
       the upper bound of values shown in the xaxis of the histogram.
    """
    
    data_dic = {"original jobs":log_data,
                "synthetic jobs":trace_data}
    
    if x_log_scale:
        paintHistogramMulti(name, data_dic, bins=100,
                        graphFileName=file_name,
                        labelX=x_axis_label,
                        xLogScale=x_log_scale,
                        labelY="Number Of Jobs",
                        xLim=filterCut)
    else:
        paintHistogramMulti(name, data_dic, bins=100,
                        graphFileName=file_name,
                        labelX=x_axis_label,
                        xLogScale=x_log_scale,
                        labelY="Number Of Jobs",
                        filterCut=filterCut)
    
    paintBoxPlotGeneral(name, data_dic, labelY=x_axis_label, 
                        yLogScale=True,
                        graphFileName=file_name+"-boxplot")

def histogram_cdf(edges, hist, name, file_name, x_axis_label, 
                  y_axis_label, target_folder="",
                  hists_order=None, 
                  do_cdf=False,
                  x_log_scale=False,
                  y_log_scale=False,
                  cdf_y_log_scale=False,
                  min_max=None,
                  cdf_min_max=None):
    """Plots histograms using their edges and bins content and stores the
    resulting bitmap it in a png file. All histograms must have the same edges.
    It is also capable of plotting the CDF of the histograms. 
    Args:
    - edges: list of numbers representing the edges of all the histograms.
    - hist: list of bin values or dictionary of lists. Each element in the
        dictionary represents a histogram. Its key is its name, its value
        is a list of ordered numbers, representing the number of elements in
        each bin (or share).
    - name: string with the name of the plot. Will be used for the title of the
        plot.
    - file_name: file system route pointing to the file to save the graph to.
        ".png" will be appended to the name.
    - x_axis_label: string with the label for x-axis.
    - y_axis_label: string with the label for y-axis.
    - target_folder: string with a file-system folder route where output file
        should be stored.
    - hists_order: list of stings with the names series present in
        histograms_value_dictionary. Series will be plotted in the order
        hist in this list.
    - do_cdf: if True, the cumulative distribution function will be plotted for
        each histogram.
    - x_log_scale: if True, x-axis will use log scale.
    - y_log_scale: if True, y-axis for the histograms will use log scale.
    - cdf_y_log_scale: if True, the y-axis for the CDF lines will use log scale.
    - min_max: If set to a two element tuple of the shape (None, None),
        (x1,None), (None, x2), (x1, x2). If first Element is not None it will
        be used as the minimum value in the y axis for the histograms. If
        second is not None, it will be used as the maximum value.
    - cdf_min_max: If set to a two element tuple of the shape (None, None),
        (x1,None), (None, x2), (x1, x2). If first Element is not None it will
        be used as the minimum value in the y axis for the CDFs. If
        second is not None, it will be used as the maximum value.
    """  

    if type(hist) is not dict:
        hist = {"":hist}
    if hists_order is not None:
        if set(hists_order)!=set(hist.keys()):
            raise ValueError("hists_order list of keys must have the same keys"
                             " as hist dictionary")
    else:
        hists_order=sorted(hist.keys())
    paintBarsHistogram(name, hists_order, edges, hist,
                       target_folder=target_folder,
                       file_name=file_name,
                       labelX=x_axis_label, labelY=y_axis_label,
                       cdf=do_cdf, 
                       x_log_scale=x_log_scale,
                       y_log_scale=y_log_scale,
                       cdf_y_log_scale=cdf_y_log_scale,
                       min_max=min_max,
                       cdf_min_max=cdf_min_max)

def create_legend(ax,legend):
    """Creates a legend for a plot. It is placed over the top of the axis, in
    a wide distribution.
    Args:
    - ax: matplotlib axis on which the legend is placed.
    - legend: list of pairs ("series name", "color name) to be used to
        construct the legend. List order matches order of on-screen legend,  
    """
    handles=[]
    for key in legend:
        hatch=None
        if len(key)>3:
            hatch=key[3]
        handles.append(mpatches.Patch(facecolor=key[1], label=key[0],
                                      edgecolor="black",
                                      hatch=hatch))
    bbox = ax.get_window_extent()
    correct=0
    if bbox.height<100:
        correct=0.1    
    ax.legend(handles=handles, fontsize=10,
               bbox_to_anchor=(0.0, 1.00-correct, 1., 0.00), loc=3,
           ncol=len(legend), mode="expand", borderaxespad=0.0,
           frameon=False)

def do_list_like(the_list, ref_list, force=False):
    """ if the_list is not a list of lists, it returns a list with n copies
    of the_list, where n is len(ref_list)."""
    if force or (the_list is not None and not type(the_list[0]) is list):
        return [the_list for x in range(len(ref_list))]
    else:
        return the_list
    

def join_rows(row_list_1, row_list_2):
    """Returns a list of list, in which element is the concatenation of the 
    elements in the same position of row_list1 and row_list."""
    
    if not row_list_1:
        return row_list_2
    if not row_list_2:
        return row_list_1
    row_list = []
    for (row1, row2) in zip(row_list_1, row_list_2):
        row_list.append(row1+row2)
    return row_list

def calculate_diffs(result_list, base_index=0, group_count=3, percent=True,
                    groups=None, speedup=False, field="median"):
    """ Calculate the absolute or relative arithmetic distance between groups
    of values. Each list in result list is sliced in ordered groups of results
    of size group_count. In each group difference is calculated between the
    element in position base_index in the group and the rest. 
    Args:
    - result_list: list of lists of NumericStats objects. 
    - base_index: position of the reference resul in each result group.
    - group_count: number of elements in each group.
    - percent: if True the distance calculated is relative,
        if False is absolute.
    - groups: list of numbers. It overrides group_count. It contains a list
        of the sizes of the groups in the result_list. e.g. [2, 3], means that
        the first group has two elements, and the second three elements.
    - speedup: if True, the relative distance is calculated using the non
        base_index element as the base of the comparison.
    """
    diffs =[]
    if groups:
        for row in result_list:
            index=0
            diffs_row = []
            diffs.append(diffs_row)
            for group in groups:
                base_res=row[index+base_index]
                base_median=base_res._get(field)
                for j in range(group):
                    res_median=row[index+j]._get(field)
                    if speedup:
                        if base_median==0:
                            diff_value=0
                        else:
                            diff_value=res_median/base_median
                    else:
                        diff_value=res_median-base_median
                        if percent and base_res!=0:
                            if base_median==0:
                                base_median=1
                            diff_value=float(diff_value)/float(base_median)
                    if j!=base_index:
                        diffs_row.append(diff_value)
                index+=group                    
    else:
        for row in result_list:
            diffs_row = []
            diffs.append(diffs_row)
            for i in range(0, len(row), group_count):
                base_res=row[i+base_index]
                base_median=base_res._get(field)
                for j in range(group_count):
                    res_median=row[i+j]._get(field)
                    diff_value=res_median-base_median
                    if speedup:
                        if base_median==0:
                            diff_value=0
                        else:
                            diff_value=res_median/base_median
                    else:
                        if percent and base_res!=0:
                            if diff_value!=0:
                                if base_median==0:
                                    base_median=1
                                diff_value=float(diff_value)/float(base_median)
                    if j!=base_index:
                        diffs_row.append(diff_value)
        
    return diffs

def adjust_number_ticks(ax, tick_count, log_scale=False, extra=None):
    """ Adjusts the y-axis of ax to show only tick_count labels."""
    my_t=ax.get_yticks()
    y_lim = (float(str(my_t[0])), float(str(my_t[-1])))
    print "INTERNAL", y_lim
    step = float(max(y_lim)-min(y_lim))/(tick_count-1)
    step=float(str(step))
    upper_limit=float(str(max(y_lim)+step))
    lower_limit=float(str(min(y_lim)))
    
    ticks = arange(lower_limit, upper_limit,step)
    if extra is not None:
        ticks=sorted(list(ticks)+[float(extra)])
    if log_scale:
        ticks=_log_down(ticks)
    ax.set_yticks(ticks)
    
    
def _log_down(num):
    from numpy import log10, power, floor
    power_l = log10(num)
    return power(10, sorted(list(set(floor(power_l)))))

def remove_ids(list_of_rows, group_size=3, list_of_pos_to_remove=[0]):
    new_list_of_rows=[]
    for row in list_of_rows:
        new_row=[]
        new_list_of_rows.append(new_row)
        index=0
        for (index, elem) in zip(range(len(row)), row):
            if not index%group_size in list_of_pos_to_remove:
                new_row.append(elem)
    return new_list_of_rows
                
                
def replace(trace_id_rows, original, replacement):
    new_trace_id_rows=[]
    for row in trace_id_rows:
        new_row=[]
        new_trace_id_rows.append(new_row)
        for item in row:
            if item in original:
                index=original.index(item)
                new_row.append(replacement[index])
            else:
                new_row.append(item)
    
    return new_trace_id_rows

def gen_trace_ids_exps(base_id, base_exp=None, group_size=3, group_count=5,
                  block_count=6, group_jump=18,inverse=False,
                  base_exp_group=None,skip=0):
    """ Generates the list of trace_ids to load and plot. Returns a list of
    lists of trace_id.
    Args:
    - base_id: first trace_id in the first list of lists.
    - base_exp: if set, base_exp is added at the beginning. of each list.
    - group_size: size of the group of experiments.
    - group_count: number of groups per block of experiments.
    - block_count: number of lists in the returned list of lists.
    - group_jump: number trace_ids to jump from one group to the next.
    - inverse: if True, the first group is at the end of each row list.
    - base_ex_group: if set, base_ex_group is added at the begining of each
        group.
    - skip: number of trace_ids to jump between blocks, 
    
    Returns:
        a list of block_count lists. Each list may be started by base_exp if
        set. Each list is composed by group_count groups of group_size size.
        If base_exp_group, is set, it is added to each group.
    """
    trace_id_rows_colors = []
    for block_i in range(block_count):
        trace_id_row = []
        trace_id_rows_colors.append(trace_id_row)
        if base_exp is not None:
            trace_id_row.append(base_exp)
        group_index_list=range(group_count)
        if inverse:
            group_index_list=reversed(group_index_list)

        for group_i in group_index_list:
            if base_exp_group is not None:
                trace_id_row.append(base_exp_group)
            for exp_i in range(group_size):
                trace_id=(base_id
                          + (group_size+skip)*block_i
                          + (group_jump)*group_i
                          + exp_i)
                trace_id_row.append(trace_id)
    return trace_id_rows_colors

def plot_multi_exp_boxplot(name, file_name, title, 
                   exp_rows,
                   y_axis_labels,
                   x_axis_labels,
                   y_axis_general_label=None,
                   grouping=None,
                   colors=None,
                   hatches=None,
                   aspect_ratio=None,
                   y_limits=None,
                   y_log_scale=False,
                   legend=None,
                   percent_diff=False,
                   base_diff=0,
                   group_count=3,
                   grouping_alt=None,
                   precalc_diffs=None,
                   y_tick_count=None,
                   y_tick_count_alt=None,
                   y_axis_label_alt=None):
    """Plots a matrix of comboboxes os series that are defined by two variables
    with multiple values. Series in the same row share the same value for the
    first variable. Series in the same column share the saame value for the
    second.
    Args:
    - name: Matplot lib name string.
    - file_name: string containing file system route pointing to a file where
        the plot will be stored.
    - exp_rows: input data to be used to produce the plot expressed as a list of
        lists of NumericStats objects. the results are plotted as rows of,
        results, each row a list item contained in the first list. All rows
        must contain the same number of NumericStats objects. 
    - x_axis_label: list of strings to label each "column" of combo boxes.
        Must have the same dimension as the number of columns in exp_rows.
        Printed at the bottom of the plot.
    - y_axis_lables: list of strings to label each "row" of combo boxes in
        the plot.
    - grouping: list of numbers that control how the layout wihtin rows. Each
        number in the list indicates how many are back to back, a 0 indicates
        an extra space. SUM(grouping) must be equal to the second dimension
        of exp__rows. e.g. with a row has 10 elements, [1, 5, 0, 4] will
        present: one combo-box, a space, five back-to-back combo-boxes, two
        spaces and four combo-boxes back to back.
    - colors:  list of text colors corresponding to the background filling of
        each column of combo-boxes. If list of list, each element correspond
        to each its same positioned combox box.
    - hatches: list of matplotlib hatches (e.g. '-', '/', '\') corresponding
        to each column of combo-boxes. If list of list, each element correspond
        to each its same positioned combox box.
    - aspect_ratio: if set to a number, it represents the width/height of the
        final plot.
    - y_limits: if set to a list of integer pairs (min, max), it will apply
        each list element as a limit for the series in a row of results.
    - y_log_scale: if True all y_scale in all comboxboxes will use logaritmic
        scale.
    - legend: if set, the plot will contain a legebd based on the list of pairs
        ("series name", "color name). List order matches order of on-screen
        legend, Color names will be mapped on matplor lib colors.  
    -  percent_diff: If True, the figure includes barcharts showing the
        difference between one of the values in each group and the rest.
    - base_diff: position of the difference reference in each group.
    - group_count: Size of the comparison groups.
    - grouping_alt: List of comparisons present per group. 
    - precalc_diffs: list of list of floats. Tt set, the differences are not
        generated, and the values are used as differences.
    - y_tick_count: If set to a number, it sets the number of labels used in
        the left y_axis.
    - y_tick_count_alt: If set to a number, it sets the number of labels used in
        the right y_axis.,
    - y_axis_label_alt: String value used as legend for the right y-axis.
    """
    num_rows=len(exp_rows)
    fig, axes = plt.subplots(nrows=num_rows, ncols=1)
    if not (type(axes) is ndarray):
        axes=[axes] 
    colors=do_list_like(colors, exp_rows)
    hatches=do_list_like(hatches, exp_rows)
    if percent_diff:
        if precalc_diffs is None:
            diffs_results = calculate_diffs(exp_rows, base_index=base_diff, 
                                    group_count=group_count,
                                    groups=grouping_alt)
        else:
            diffs_results = precalc_diffs
    else:
        diffs_results  = do_list_like([0], exp_rows)
    extra_spacing=0
    if percent_diff:
        extra_spacing=group_count-1
    
    label_ax=axes[len(axes)/2]
    for (ax,results_row,y_axis_label, color_item, hatches_item, diffs) in zip(
                                             axes,
                                             exp_rows,
                                             y_axis_labels,
                                             colors,
                                             hatches,
                                             diffs_results):
        median, p25, p75, min_val, max_val = _get_boxplot_data(results_row)
        if ax==axes[0]:
            ax.set_title(title)
        the_labels=None
        if ax==axes[-1]:
            the_labels=x_axis_labels
            if legend:
                create_legend(ax,legend)
        else:
            the_labels=["" for x in results_row]
        if y_axis_general_label: 
            if ax==label_ax:
                y_axis_label="{0}\n{1}".format(y_axis_general_label,
                                               y_axis_label)
            else:
                y_axis_label="{0}".format(y_axis_label)
            ax.get_yaxis().set_label_coords(-0.06,0.5)
        positions, widths, alt_positions, alt_width = (
            _add_precalc_boxplot(ax,median, p25, p75, min_val, max_val,
                            grouping=grouping,
                            colors=color_item,
                            hatches=hatches_item,
                            labels=the_labels,
                            y_axis_label=y_axis_label,
                            y_limits=y_limits,
                            y_log_scale=y_log_scale,
                            extra_spacing=extra_spacing))
        if grouping and percent_diff:
            the_y_label_alt=None
            if ax==label_ax:
                the_y_label_alt=y_axis_label_alt
                
                """ OOJOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO """
            if grouping[0]!=group_count:
                color_item=color_item[1:]
                hatches_item=hatches_item[1:]
            _add_diffs(ax, diffs, alt_positions, alt_width,
                       colors=_extract_pos(color_item, base_diff,
                                           extra_spacing+1),
                       hatches=_extract_pos(hatches_item, base_diff,
                                            extra_spacing+1),
                       y_tick_count=y_tick_count_alt,
                       y_label=the_y_label_alt)
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale)
            
        
            

    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        axes[0].set_title(title)
    
    fig.savefig(file_name, bbox_inches='tight')
    
def flatten_list(the_list):
    """ Takes a lists of lists and puts all their elements in a list."""
    return [item for sublist in the_list for item in sublist]

def extract_type(data_list, type_list, select_type):
    """Returns a sublist of data_list. An element es added to the returning list
    if the element of the same position in type_list is "select_type".
    Args:
        - data_list: List of element
        - type_list: List of string types. Same size as data_list
        - select_list: type of the elements to be returns.
    """
    new_data=[]
    type_list=flatten_list(type_list)
    for (the_data, the_type) in zip(data_list, type_list):
        if the_type==select_type:
            new_data.append(the_data)
    return new_data

def plot_multi_boxplot_bars(name, file_name, title, 
                   exp_rows,
                   type_rows,
                   y_axis_labels,
                   x_axis_labels,
                   y_axis_general_label=None,
                   colors=None,
                   hatches=None,
                   aspect_ratio=None,
                   y_limits=None,
                   y_log_scale=False,
                   legend=None,
                   y_tick_count=None,
                   y_tick_count_alt=None,
                   y_axis_label_alt=None):
    """
    Similar to plot_multi_boxplot, but the it can paint experiments as
    boxplot or barchart. The main arguments:
    - exp_rows: list of lists of numericStats
    - type_rows: list of lists of strings. Each sub list is a group of
        results that will be plotted without spaces between. Sublists are
        lists of strings signaling which method to plot the result. If
        string is "bar", it is a barchart showing the result median, if "box", 
        it is a boxplot of the result. 
    """
    num_rows=len(exp_rows)
    fig, axes = plt.subplots(nrows=num_rows, ncols=1)
    if not (type(axes) is ndarray):
        axes=[axes] 
    colors=do_list_like(colors, exp_rows)
    hatches=do_list_like(hatches, exp_rows)
    type_rows=do_list_like(type_rows, exp_rows, force=True)
    
    label_ax=axes[len(axes)/2]
    
    for (ax,results_row, type_grouping, y_axis_label, color_item, 
         hatches_item) in zip(axes,
                             exp_rows,
                             type_rows,
                             y_axis_labels,
                             colors,
                             hatches):
        if ax==axes[0]:
            ax.set_title(title)
        
        if y_axis_general_label: 
            if ax==label_ax:
                y_axis_label="{0}\n{1}".format(y_axis_general_label,
                                               y_axis_label)
            else:
                y_axis_label="{0}".format(y_axis_label)
            ax.get_yaxis().set_label_coords(-0.06,0.5)
        boxplot_results=extract_type(results_row, type_grouping, "box")
        bar_results=extract_type(results_row, type_grouping, "bar")
        positions_dic, widths = _cal_positions_hybrid(type_grouping)
        if boxplot_results:
            the_labels=None
            if ax==axes[-1]:
                the_labels=extract_type(x_axis_labels, type_grouping,"box")
                if legend:
                    create_legend(ax,legend)
            else:
                the_labels=["" for x in boxplot_results]
            median, p25, p75, min_val, max_val = _get_boxplot_data(
                                                                boxplot_results)
            _add_precalc_boxplot(ax,median, p25, p75, min_val, max_val,
                            x_position=positions_dic["box"],
                            x_widths=widths,
                        colors=extract_type(color_item, type_grouping,"box"),
                        hatches=extract_type(hatches_item, type_grouping,"box"),
                            labels=the_labels,
                            y_axis_label=y_axis_label,
                            y_limits=y_limits,
                            y_log_scale=y_log_scale)
        if bar_results:
            the_y_label_alt=None
            if ax==label_ax:
                the_y_label_alt=y_axis_label_alt
            if ax==axes[-1]:
                the_labels=extract_type(x_axis_labels, type_grouping,"bar")
            else:
                the_labels=["" for x in bar_results]
            
            _add_diffs(ax, bar_results, positions_dic["bar"], widths,
                       colors=extract_type(color_item, type_grouping,"bar"),
                       hatches=extract_type(hatches_item, type_grouping,"bar"),
                       y_tick_count=y_tick_count_alt,
                       y_label=the_y_label_alt,
                       x_labels=the_labels)
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale)

    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        axes[0].set_title(title)
    
    fig.savefig(file_name, bbox_inches='tight')
    
def plot_multi_bars(name, file_name, title, 
                   exp_rows,
                   type_rows,
                   y_axis_labels,
                   x_axis_labels,
                   y_axis_general_label=None,
                   colors=None,
                   hatches=None,
                   aspect_ratio=None,
                   y_limits=None,
                   y_log_scale=False,
                   legend=None,
                   y_tick_count=None,
                   y_tick_count_alt=None,
                   y_axis_label_alt=None,
                   ncols=1,
                   subtitle=None,
                   ref_line=None,
                   do_auto_label=True):
    """ Plots the medians of a list of lists of results. It is similar to
    the plot_multi_boxplot, but it admits to group the blocks in rows and
    columns. Important arguments.
    - exp_rows: list of list of results which median is plotted. Each list
        of the list will be plotted in an individual subfigure.
    - type_rows: list of lists of strings to shop the grouping. It shows how
        the bars are gropued (leaving or not spaces between). It must contain
        the work "bar" for each individual result.
    """
    num_rows=len(exp_rows)
    fig, axes = plt.subplots(nrows=num_rows/ncols, ncols=ncols)
    print axes
    if ncols>1:
        axes=asarray(flatten_list(axes))
    print axes
    if not (type(axes) is ndarray):
        axes=[axes] 
    colors=do_list_like(colors, exp_rows)
    hatches=do_list_like(hatches, exp_rows)
    type_rows=do_list_like(type_rows, exp_rows, force=True)
    
    label_ax=axes[len(axes)/2-(ncols-1)]
    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if ncols>1:
        plt.tight_layout(pad=0)
        
    for (ax,results_row, type_grouping, y_axis_label, color_item, 
         hatches_item) in zip(axes,
                             exp_rows,
                             type_rows,
                             y_axis_labels,
                             colors,
                             hatches):

        if ax==axes[0]:
            ax.set_title(title)
        if len(axes)==1 or ax==axes[1] and ncols>1 and subtitle:
            ax.set_title(subtitle)
        if ref_line:
            ax.axhline(ref_line, linestyle="--")
        
        if y_axis_general_label: 
            if ax==label_ax:
                y_axis_label="{0}\n{1}".format(y_axis_general_label,
                                               y_axis_label)
            else:
                y_axis_label="{0}".format(y_axis_label)
            #ax.get_yaxis().set_label_coords(-0.07*ncols,0.5)
       
        bar_results=results_row
        positions_dic, widths = _cal_positions_hybrid(type_grouping)

        if ax==axes[-1] or ax==axes[-ncols]:
            the_labels=x_axis_labels
            if legend and ax==axes[-1]:
                create_legend(ax,legend)
        else:
            the_labels=["" for x in bar_results]
    

        _add_diffs(ax, bar_results, positions_dic["bar"], widths,
                   colors=extract_type(color_item, type_grouping,"bar"),
                   hatches=extract_type(hatches_item, type_grouping,"bar"),
                   y_tick_count=None,
                   y_label=y_axis_label,
                   x_labels=the_labels,
                   main_axis=True,
                   bigger_numbers=True,
                   do_auto_label=do_auto_label,
                   y_log_scale=y_log_scale,
                   y_limits=y_limits)
        if y_limits:
            ax.set_ylim(y_limits[0],y_limits[1])
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale, extra=ref_line)


    fig.savefig(file_name, bbox_inches='tight')

def _extract_pos(items, pos, size):
    new_items=[]
    for i in range(len(items)):
        if i%size!=pos:
            new_items.append(items[i])
    return new_items


def _get_boxplot_data(numeric_results_list):
    values=[[],[],[],[],[]]
    for result in numeric_results_list:
        a_value = result.get_values_boxplot()
        for (target, src) in zip(values, a_value):
            target.append(src)
    return values[0], values[1], values[2], values[3], values[4]  
    
def _autolabel(ax, rects, values,bigger_numbers=False, background=True,
               y_limits=None):
    extra_cad=""
    max_value=max(values)
    min_value=min(values)
    y_lims=ax.get_ylim()
    max_value=min(max_value, y_lims[1])
    min_value=max(min_value, y_lims[0])
    if y_limits is not None:
        if y_limits[0] is not None:
            min_value=max(min_value, y_limits[0])
        if y_limits[1] is not None:
            max_value=min(max_value, y_limits[1])
    distance = max_value-min_value
    mid_point=min_value+distance/2.0
    print "values", min_value, max_value, distance, mid_point
    va="bottom"
    margin=0.05
    h_margin=0.0
    for (rect, value) in zip(rects, values):
        if value<0.3 and value>-0.3:
            if mid_point>=0:
                height=distance*margin
            else:
                height=-distance*margin
                va="top"
        elif value>0:
            if abs(value)>distance/2:
                height=distance*margin
            else:
                height = value+distance*margin
        elif value<0:
            if abs(value)>distance/2: 
                height=-distance*margin
                va="top"
            else:
                height=value-distance*margin
        horiz_position=rect.get_x() + (rect.get_width()/2)*(1+h_margin)
        font_size="smaller"
        if bigger_numbers:
            font_size="large"
        bbox=None
        extraText=""
        if y_limits is not None:
            if y_limits[0] is not None:
                height=max(height, y_limits[0])
            if y_limits[1] is not None:
                height=min(height, y_limits[1])
        if background:
            bbox=dict(facecolor='lightgrey', pad=0,
                      edgecolor="lightgrey", alpha=0.5)
            extraText=" "
        myt=ax.text(horiz_position, 1.01*height,
                extraText+"{0:.2f}{1}".format(float(value), extra_cad),
                ha='center', va=va, rotation="vertical",
                fontsize=font_size,
                bbox=bbox)



def precalc_boxplot(name,file_name, median, p25, p75, min_val, max_val,
                    grouping=None,
                    aspect_ratio=None,
                    colors=None,
                    hatches=None,
                    labels=None,
                    y_axis_label=None,
                    title=None):
    """ Plots boxplots from their basic values, instead of the original list
    of values. If median, p25,p75,min_val, max_val are lists of the same
    dimension, it plots multiple ones."""
    fig = plt.figure(name)
    ax = fig.add_subplot(111)
    _add_precalc_boxplot(ax,median, p25, p75, min_val, max_val,
                        grouping=grouping,
                        colors=colors,
                        hatches=hatches,
                        labels=labels,
                        y_axis_label=y_axis_label)
    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        ax.set_title(title)
    fig.savefig(file_name, bbox_inches='tight')

def _add_diffs(ax, diff_values, positions, width,
               colors=None, hatches=None,
               y_tick_count=None, y_label=None,
               x_labels=None,
               main_axis=False,
               bigger_numbers=False,
               do_auto_label=True,
               y_log_scale=False,
               y_limits=None):
    if main_axis:
        ax_alt=ax
    else:
        ax_alt=ax.twinx()
    bplot = ax_alt.bar(positions, diff_values, width=width,
                       tick_label=x_labels,
                       log=y_log_scale)
    if y_label:
        ax_alt.set_ylabel(y_label)
    if colors:        
        for patch, color in zip(bplot, colors):
            if color:
                patch.set_facecolor(color)
    if hatches:
        for patch, hatch in zip(bplot, hatches):
            if hatch:
                patch.set_hatch(hatch)
    
    if y_tick_count:
        adjust_number_ticks(ax_alt, y_tick_count)
    if do_auto_label:
        _autolabel(ax_alt, bplot, diff_values,bigger_numbers=bigger_numbers,
                   y_limits=y_limits)
    

def _adjust_y_limits_margin(ax, values, margin=0.3):
    max_value=float(max(values))
    min_value=float(min(values))

    if max_value>0:
        max_value+=(max_value-min_value)*margin
    if min_value<0:
        min_value-=(max_value-min_value)*margin
    ax.set_ylim((min_value, max_value))
    
    
    


def _add_precalc_boxplot(ax, median, p25, p75, min_val, max_val,
                        x_position=None,
                        x_widths=None,
                        grouping=None,
                        colors=None,
                        hatches=None,
                        labels=None,
                        y_axis_label=None,
                        y_limits=None,
                        y_log_scale=False,
                        extra_spacing=0,
                        alt_grouping=None):
    """Adds boxplots to a matplotlib axis.""" 
    
    positions=None
    alt_positions=None
    widths=0.5
    alt_width=0.25

    if x_position and widths:
        positions=x_position
        widths = x_widths
    elif grouping:
        positions, widths, alt_positions, alt_width=_cal_positions_widths(
                                                grouping, 
                                                extra_spacing=extra_spacing,
                                                alt_grouping=alt_grouping)
    fake_data=_create_fake_data(median, p25,p75, min_val, max_val)
    bplot = ax.boxplot(fake_data, positions=positions, 
               widths=widths,patch_artist=True,
               labels=labels,
               whis=9999999999999)
    if colors:        
        for patch, color in zip(bplot['boxes'], colors):
            if color:
                patch.set_facecolor(color)
    if hatches:
        for patch, hatch in zip(bplot['boxes'], hatches):
            if hatch:
                patch.set_hatch(hatch)
    
    if y_axis_label:
        ax.set_ylabel(y_axis_label)
    if y_limits:
        ax.set_ylim(y_limits)
    if y_log_scale:
        ax.set_yscale("log")
    
    return positions, widths, alt_positions, alt_width

    
    
def _create_fake_data(median, p25, p75, min_val, max_val):
    if (type(p25) is list):
        fake_data=[]
        for (median_i, p25_i, p75_i,min_val_i, max_val_i) in zip(
            median, p25, p75, min_val, max_val):
            fake_data.append(
                _create_fake_data(median_i, p25_i, p75_i,min_val_i, max_val_i))
        return fake_data 
    else:
        return [min_val, p25,median,p75,max_val]
    
def _cal_positions_widths(grouping,extra_spacing=0, alt_grouping=None):
    if grouping is None:
        return None, 0.5
    if alt_grouping is None:
        alt_grouping=grouping
    total_bp=sum(grouping)
    total_blocks=total_bp+len(grouping)+1
    if extra_spacing:
        total_blocks+=len(grouping)*extra_spacing
    
    widths=total_blocks/float(total_blocks)
    space_width=float(widths)/2.0
    
    current_pos=1.0
    positions = []
    alt_positions=[]
    for (bp_group, alt_group) in zip(grouping, alt_grouping):
        for bp in range(bp_group):
            positions.append(current_pos)
            current_pos+=widths
        for i in range(min(extra_spacing, alt_group-1)):
            alt_positions.append(current_pos-space_width)
            current_pos+=space_width
        current_pos+=space_width
    return positions, widths, alt_positions, space_width

def _cal_positions_hybrid(grouping):
    flat_grouping = flatten_list(grouping)
    if grouping is None:
        return None, 0.5
    uniq_types=list(set(flat_grouping))
    positions_dic = {}
    for ut in uniq_types:
        positions_dic[ut] = []
   
#     total_bp=len(flat_grouping)
#     total_blocks=float(total_bp)+(float(len(grouping))-1)*0.5
# 
#     widths=total_blocks/float(total_blocks)
#     space_width=float(widths)/2
#     
#     if float(len(grouping))%2==0:
#         widths*=2
#         space_width*=2
#         current_pos=widths
#     else:
#         current_pos=0.0
    widths=1.0
    space_width=widths
    current_pos=0


    for (bp_group) in grouping:
        for bp in bp_group:
            positions_dic[bp].append(current_pos)
            current_pos+=widths
        current_pos+=space_width
    return positions_dic, widths
            
    

def extract_grouped_results(db_obj, trace_id_rows_colors, edges, result_type): 
    """Takes a list of lists of trace_is and produces a list of lists of results
    corresponding to them.
    Args:
    - db_obj: DBManager object connted to a db where the results will be pulled
        from.
    - trace_id_rows_colors: list of lists of integers as trace_ids of experiments.
    - edges: if set to [""], it does no effect, the function extracts results
        of the type result_type. If set to a list of items, results will be
        pulled for each element as: "g"+str(edge)+_str(result_type)
    - result_type: string indentifying which type of result are we pulling. It
        correspond to the type of the NumericStats stored in db_obj.
    Returns: a dictionary indexed by edges. Each element is a list of lists of
        same dimension of trace_id_rows_colors, each element a NumericStats object
        corresponding to the result of that component.
    """
    exp_rows={}
    for edge in edges:
        exp_rows[edge]=extract_results(db_obj, trace_id_rows_colors,
                                       ResultTrace.get_result_type_edge(edge,
                                                               result_type))
    return exp_rows
    
    
    exp_rows={}
    for edge in edges:
        exp_rows[edge]=[]
    for row in trace_id_rows_colors:
        these_rows={}
        for edge in edges:
            these_rows[edge]=[]
            exp_rows[edge].append(these_rows[edge])
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            for edge in edges:
                result=None
                if exp.is_it_ready_to_process():
                    if edge=="":
                        key = ResultTrace.get_result_type_edge(edge,
                                                               result_type)
                    else:
                        key=result_type
                    key+="_stats"
                    result = NumericStats()
                    result.load(db_obj, trace_id, key)
                else:
                    result = NumericStats()
                    result.calculate([0, 0, 0])
                these_rows[edge].append(result)
    return exp_rows

def get_list_rows(rows, field_list):
    new_rows=[]
    for row in rows:
        new_row = []
        new_rows.append(new_row)
        
        for (index,res) in zip(range(len(row)),row):
            field=field_list[index%len(field_list)]
            new_row.append(res._get(field))
    return new_rows

def extract_usage(db_obj, trace_id_rows, fill_none=True, factor=1.0, 
                  mean=False): 
    """Takes a list of lists of trace_is and produces a list of lists of results
    corresponding to them.
    Args:
    - db_obj: DBManager object connted to a db where the results will be pulled
        from.
    """
    exp_rows=[]  
    my=ResultTrace()
    res_type="usage"
    if mean:
        res_type="usage_mean"
    for row in trace_id_rows:
        new_row=[]
        exp_rows.append(new_row)
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            result = my._get_utilization_result()           
            if exp.is_analysis_done():
                result.load(db_obj, trace_id,res_type)
            else:
                result._set("utilization", 0)
                result._set("waste", 0)
                result._set("corrected_utilization", 0)
            result.apply_factor(factor)
            new_row.append(result)
    return exp_rows


def extract_results(db_obj, trace_id_rows_colors, result_type, factor=None,
                    fill_none=True, second_pass=False): 
    """Takes a list of lists of trace_is and produces a list of lists of results
    corresponding to them.
    Args:
    - db_obj: DBManager object connted to a db where the results will be pulled
        from.
    - trace_id_rows_colors: list of lists of integers as trace_ids of experiments.
     - result_type: string indentifying which type of result are we pulling. It
        correspond to the type of the NumericStats stored in db_obj.
    Returns:  a list of lists of
        same dimension of trace_id_rows_colors, each element a NumericStats object
        corresponding to the result of that component.
    """
    exp_rows=[]  
    for row in trace_id_rows_colors:
        new_row=[]
        exp_rows.append(new_row)
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            
            if exp.is_analysis_done(second_pass=second_pass):
                key=result_type+"_stats"
                result = NumericStats()
                result.load(db_obj, trace_id, key)
                if factor:
                    result.apply_factor(factor)
            else:
                result = NumericStats()
                result.calculate([0, 0, 0])
            if fill_none and result._get("median") is None:
                result = NumericStats()
                result.calculate([0, 0, 0])
            new_row.append(result)
    return exp_rows

def get_dic_val(dic, val):
    if val in dic.keys():
        return dic[val]
    return dic[""]

def produce_plot_config(db_obj, trace_id_rows_colors):
    """ Produces the coloring and hatches matrixes for a matrix style plot.
    For that it conencts to a dabase, and depending on the scheduling algorithm
    used in the experiment, it chooses a cooresponding coloring and hatches.
    Args:
    - db_obj: DBManager object connted to a db where the results will be pulled
        from.
    - trace_id_rows_colors: list of lists of integers as trace_ids of experiments.
    returns:
    - color_rows: list of list of matplotlib colors corresponding to each
        experiment subplot.
    - hatches_rows: list of lists of the hatches to be used in each experiment
        subplot.
    - legend: legend list of the format ("series names", "color"), listing the
        scheduling algorithms present in the experiments.
    """
    colors_dic = {"no":"white", "manifest":"lightgreen", "single":"lightblue",
                  "multi":"pink", "":"white"}
    
    hatches_dic = {"no":None, "manifest": "-", "single":"\\",
                  "multi":"/", "":None}
    
    detected_handling={}
    
    color_rows = []
    hatches_rows = []
    for row in trace_id_rows_colors:
        this_color_row=[]
        color_rows.append(this_color_row)
        this_hatches_row=[]
        hatches_rows.append(this_hatches_row)
        for trace_id in row:
            exp = ExperimentDefinition()
            exp.load(db_obj, trace_id)
            handling=exp.get_true_workflow_handling()
            detected_handling[handling]=1
            this_color_row.append(get_dic_val(colors_dic,
                                              handling))
            this_hatches_row.append(get_dic_val(hatches_dic,
                                                handling))
        
    legend=[("n/a","white", "no", None),
                ("aware","lightgreen", "manifest","-"),
                ("waste","lightblue", "single", "\\"),
                ("wait","pink", "multi", "/")]
    new_legend=[]
    for item in legend:
        if item[2] in detected_handling.keys():
            new_legend.append(item)
    
    return color_rows, hatches_rows, new_legend
            
     
     
        
    
    
    