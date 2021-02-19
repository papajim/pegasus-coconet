#!/usr/bin/env python3

import os
from pathlib import Path
from argparse import ArgumentParser

from Pegasus.api import *

class CoconetWorkflow():
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_name = "coconet_workflow"
        self.wf_dir = Path(__file__).parent.resolve()
        return


    def write(self):
        self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write()
        return


    def create_pegasus_properties(self):
        self.props = Properties()
        return


    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = Site("local")\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),
                        
                        Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                            .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
                    )

        exec_site = Site(exec_site_name)\
                        .add_pegasus_profile(style="condor")\
                        .add_condor_profile(universe="vanilla")\
                        .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")

        self.sc.add_sites(local, exec_site)
        return


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()
        
        motion_container = Container("motion_container", Container.DOCKER, image=os.path.join(self.wf_dir, "containers/motion_container.tar"), image_site="condorpool")
        detection_container = Container("detection_container", Container.DOCKER, image=os.path.join(self.wf_dir, "containers/detection_container.tar"), image_site="condorpool")

        motion_module = Transformation("motion_module", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/motion_module_wrapper.sh"), is_stageable=True, container=motion_container)
        detection_module = Transformation("detection_module", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/detection_module_wrapper.sh"), is_stageable=True, container=detection_container)
        tracking_fusion_module = Transformation("tracking_fusion_module", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/tracking_fusion_module_wrapper.sh"), is_stageable=False)

        self.tc.add_containers(motion_container, detection_container)
        self.tc.add_transformations(motion_module, detection_module, tracking_fusion_module)
        return


    # --- Replica Catalog ----------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()\
                    .add_replica("local", "dataset.tar.gz", os.path.join(self.wf_dir, "input/dataset.tar.gz"))\
                    .add_replica("local", "yolov3.cfg", os.path.join(self.wf_dir, "input/yolov3.cfg"))\
                    .add_replica("local", "yolov3.weights", os.path.join(self.wf_dir, "input/yolov3.weights"))
        return


    # --- Submit Workflow ----------------------------------------------------------
    def submit_workflow(self):
        return

    
    # --- Create Workflow ----------------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)
        
        dataset = File("dataset.tar.gz")
        motion_output = File("motion_output.tar.gz")
        motion_module_job = Job("motion_module")\
                                .add_inputs(dataset)\
                                .add_outputs(motion_output, stage_out=True, register_replica=False)

        yolov3_cfg = File("yolov3.cfg")
        yolov3_weights = File("yolov3.weights")
        detection_output = File("detection_output.tar.gz")
        detection_module_job = Job("detection_module")\
                                .add_inputs(dataset, yolov3_cfg, yolov3_weights)\
                                .add_outputs(detection_output, stage_out=True, register_replica=False)

        self.wf.add_jobs(motion_module_job, detection_module_job)


if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Coconet Workflow")

    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")

    args = parser.parse_args()

    workflow = CoconetWorkflow(args.output)
    
    print("Creating execution sites...")
    workflow.create_sites_catalog()

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog()

    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    print("Creating coconet workflow dag...")
    workflow.create_workflow()

    workflow.write()
