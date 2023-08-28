# %%
from diagrams import Cluster, Diagram, Edge

# %%
from diagrams.aws.analytics import DataPipeline
from diagrams.aws.compute import Batch, Compute
from diagrams.aws.database import Database
from diagrams.aws.devtools import CommandLineInterface
from diagrams.aws.general import General

# %%
with Diagram("imports", direction="TB", show=False):

    with Cluster("resource modules"):
        ht = General("helper_tools")
        sb = General("submit")

    cli = General("cli")
    wf = General("workflows")
    pp = General("preprocess")

    cli << wf << pp << ht << sb
    cli << sb
    wf << ht
    pp << sb


# %%
graph_attr = {
    "layout": "dot",
    "compound": "true",
}

with Diagram("process", graph_attr=graph_attr, show=True):
    cli = CommandLineInterface("cli")
    with Cluster("submit"):
        sb = Batch("schedule_subj")
    with Cluster("sbatch_parent"):
        wf = Compute("workflows")
        with Cluster("run_preproc"):
            dl = Database("pull_rawdata")
            with Cluster("preprocess"):
                fs = Batch("RunFreeSurfer")
                fp = Batch("RunFmriprep")
                fl = Compute("fsl_preproc")
                with Cluster("helper_tools"):
                    ht = Batch("AfniFslMethods")
            ul = Database("push_derivatives")
    with Cluster("sbatch_child"):
        fschild = Compute("recon_all")
        fpchild = Compute("fmriprep")
        fslchild = DataPipeline("tmean-->scale")

    cli >> sb >> Edge(lhead="cluster_sbatch_parent") >> wf
    wf >> Edge(lhead="cluster_run_preproc") >> dl
    dl >> Edge(lhead="cluster_preprocess") >> fs
    fl >> Edge(lhead="cluster_helper_tools") >> ht
    fs >> fp >> fl
    ht >> Edge(ltail="cluster_preprocess") >> ul

    #
    fs >> fschild
    fp >> fpchild
    ht >> fslchild


# %%
