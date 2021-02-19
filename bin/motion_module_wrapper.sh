#!/usr/bin/env bash

pegasus_working_dir=`pwd`

mkdir motion_output
tar -xzvf dataset.tar.gz

#cd /coconet
/coconet/build/Motion_Module $pegasus_working_dir/dataset $pegasus_working_dir/motion_output
#mv /coconet/output/* $pegasus_working_dir/motion_output

cd $pegasus_working_dir
tar -czvf motion_output.tar.gz motion_output
