from plot import extract_usage, calculate_diffs


def get_diff_results(db_obj, trace_id_rows, mean=False,
                     field = "corrected_utilization"):
    usage_rows=extract_usage(db_obj, trace_id_rows, factor=100.0, mean=mean)
#     print [[dd._get("corrected_utilization") for dd in row] 
#            for row in usage_rows]
#     
#     print usage_rows

    diffs_results = calculate_diffs(usage_rows, base_index=1, 
                                    group_count=2, percent=False,
                                    field=field)
    return diffs_results

def print_results(time_labels, manifest_label,diffs_results,
                  num_decimals=2):
    print(" & ".join(time_labels) + "\\\\")
    print("\\hline")
    for (man,row) in zip(manifest_label,diffs_results):
        print((" & ".join([man]+
                      [("${0:."+str(num_decimals)+"f}$").format(the_value) 
                        for the_value in row]) +
               "\\\\"))
    print("\\hline")