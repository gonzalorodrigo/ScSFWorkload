#!/bin/bash
# Script to launch all the tests

echo "These tests use a test database. Make sure that a local Test database"\
" exists and correct env vars are configured (read bin/sql_conf_env.sh)"

echo "Tests require a Slurm worker with IP 192.168.56.24 or at the "\
"TEST_VM_HOST IP"

test_list=("test_running" "test_running" "test_orchestration")

passed_count=0
failed_count=0





eval "test_list=\${$test_list[@]}"
echo ".......Testing ${test_list[@]}}"
for test_name in ${test_list[@]}; do
	test_output_file="${test_name}.test_result"
	command="python -m unittest ${test_name}"
	printf "Test ${test_output_file}: "
	result="FAILED. Re-run: $command"
	$command &> "${test_output_file}"
	if [ $? -eq 0 ]; then
		result="PASSED"
		passed_count=$[ ${passed_count}+1 ]
	else
		failed_count=$[ ${failed_count}+1 ]
	fi
	echo "${result}"
done


total_tests=$[ ${passed_count}+${failed_count} ]
echo ".......Summary......."
echo "Total tests: ${total_tests}."
echo "Tests PASSED: ${passed_count}"
echo "Tests FAILED: ${failed_count}"
if [ $failed_count -eq 0 ]; then
	echo "ALL TESTS PASSED!! OLÉ!!!"
	exit 0
else
	echo "SOME TESTS FAILED!! Keep trying..."
	exit 1
fi
