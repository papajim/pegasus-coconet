#!/usr/bin/env bash

pegasus_working_dir=`pwd`

mkdir detection_output
tar -xzvf dataset.tar.gz

cd /coconet
./darknet detect $pegasus_working_dir/yolov3.cfg $pegasus_working_dir/yolov3.weights $pegasus_working_dir/dataset
mv /coconet/output/* $pegasus_working_dir/detection_output

cd $pegasus_working_dir
tar -czvf detection_output.tar.gz detection_output
