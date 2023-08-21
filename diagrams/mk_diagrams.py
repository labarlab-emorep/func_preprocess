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

with Diagram("process", graph_attr=graph_attr):
    cli = CommandLineInterface("cli")
    sb = Batch("schedule wf")

    with Cluster("sbatch"):
        wf = Compute("parent wf")

        with Cluster("child_pipe"):
            dl = Database("Download")
            fs = Compute("FreeSurfer")
            fp = Compute("fMRIPrep")
            fl = Compute("FSL preproc")
            ul = Database("Upload")

    cli >> sb >> Edge(lhead='cluster_sbatch') >> wf
    # wf >> dl
    wf >> Edge(lhead='cluster_child_pipe') >> dl
    dl >> fs >> fp >> fl >> ul

# %%
