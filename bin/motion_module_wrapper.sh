#!/usr/bin/env bash

pegasus_working_dir=`pwd`

mkdir motion_output
tar -xzvf dataset.tar.gz

/coconet/build/Motion_Module $pegasus_working_dir/dataset $pegasus_working_dir/motion_output

exit_code=$?

if [ $exit_code -eq 0 ]; then
  tar -czvf motion_output.tar.gz motion_output
else
  exit $exit_code
fi
