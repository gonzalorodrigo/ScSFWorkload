#/bin/bash
#
# Script to compile list_trace as an stand alone.

if [ "$#" -ne 1 ]; then
    echo "Missing argument: route to the Slurm Worker package code"
    exit -1
fi

patch_file="sim_trace.patch"
source_dir="$1/slurm/contribs/simulator"

file_list=( "list_trace.c" "sim_trace.c" "sim_trace.h" )

echo "Copying files from ${source_dir}"
for file_name in "${file_list[@]}"
do
	echo "${source_dir}/${file_name}"
	cp "${source_dir}/${file_name}" .	
done

echo "Patching sim_trace.c"
patch sim_trace.c "$patch_file"

echo "Compiling"
make
