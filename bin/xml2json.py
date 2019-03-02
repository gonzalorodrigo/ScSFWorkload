from workflows import convert_xml_wf_to_json_manifest
import sys

"""
examples:
python xml2json.py xml/mont-degree8.xml xml/mont-degree8.json 0 mBackground mProjectPP

python xml2json.py xml/mont-degree8.xml xml/mont-degree8.json 480 mBackground mProjectPP

This scripts transforms the DAX workflow definitions to JSON manifest format.
It can group tasks that belong to the same stage that have the same name. An
overal or per stage limit of cores per stage can be set (applied to grouped
tasks).

"""

usage = ("python xml2json input_xml_file output_json_file [max_cores_per_task]"
        " [task_name_0] [task_name_1:max_cores_for_this_task]... [task_name_n]")

if len(sys.argv)<3:
    print "Usage: ",usage
    raise ValueError("Missing input xml and/our output json files");

xml_file=sys.argv[1]
json_file=sys.argv[2]
grouped_jobs=[]
max_cores=0
if (len(sys.argv)==4):
    print "Usage:",usage
    raise ValueError("Cannot provide max_cores value wihtout a list of job"
                    " names to fuse.")
if len(sys.argv)>3:
    max_cores=int(sys.argv[3])
    grouped_jobs=sys.argv[4:]
     
    
convert_xml_wf_to_json_manifest(xml_file, json_file,
                                max_cores=max_cores,
                                grouped_jobs=grouped_jobs)