import sys, os, threading
import execo, execo_g5k, execo_engine
from os.path import join as pjoin
from os.path import exists as pexists

from common import *

class g5k_prepare_bench_flops(execo_engine.Engine):

    def __init__(self):
        super(g5k_prepare_bench_flops, self).__init__()
        self.options_parser.set_usage("usage: %prog <comma separated list of clusters>")
        self.options_parser.set_description("precompile package for benching flops")
        self.options_parser.add_option("-o", dest = "oar_options", help = "oar reservation options", default = None)
        self.options_parser.add_option("-w", dest = "walltime", help = "walltime of compilation jobs", type = "string", default = "8:0:0")
        self.options_parser.add_argument("clusters", "comma separated list of clusters. ALL for all clusters")
        self.prepare_path = pjoin(self.engine_dir, "preparation")

    def run(self):
        if len(self.args) != 1:
            print "ERROR: missing argument"
            self.options_parser.print_help(file=sys.stderr)
            exit(1)
        try:
            os.makedirs(self.prepare_path)
        except:
            pass
        cluster_specs = set(self.args[0].split(","))
        if "ALL" in cluster_specs:
            cluster_specs.remove("ALL")
            cluster_specs.update(execo_g5k.get_g5k_clusters())
        clusters = list()
        for cluster_spec in cluster_specs:
            cluster, _, site = cluster_spec.partition("@")
            if not site:
                site = execo_g5k.get_cluster_site(cluster)
            clusters.append((cluster, site))
        clusters_todo = [ cluster for cluster in clusters if (not pexists(pjoin(self.prepare_path, prepared_archive("atlas", cluster[0])))
                                                              or not pexists(pjoin(self.prepare_path, prepared_archive("openmpi", cluster[0])))
                                                              or not pexists(pjoin(self.prepare_path, prepared_archive("hpl", cluster[0])))) ]
        execo_engine.logger.info("clusters = %s" % (clusters,))
        execo_engine.logger.info("cluster_todo = %s" % (clusters_todo,))
        threads = list()
        for cluster, site in clusters_todo:
            th = threading.Thread(target = self.compil_worker, args = (cluster, site))
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

    def compil_worker(self, cluster, site):
        jobid = None

        def worker_log(arg):
            execo_engine.logger.info("compil worker %s@%s - job %s: %s" % (cluster, site, jobid, arg))

        try:
            # submit job
            worker_log("submit oar job")
            submission = execo_g5k.OarSubmission(resources = "{'cluster=\"%s\"'}/nodes=1" % cluster,
                                                 walltime = self.options.walltime,
                                                 name = "compilworker",
                                                 additional_options = self.options.oar_options)
            ((jobid, _),) = execo_g5k.oarsub([(submission, site)])
            worker_log("job submitted")
            if not jobid:
                worker_log("aborting, job submission failed")
                return
            execo_g5k.wait_oar_job_start(jobid, site, prediction_callback = lambda ts: worker_log("job start prediction: %s" % (execo.format_date(ts),)))
            worker_log("job started - get job nodes")
            nodes = execo_g5k.get_oar_job_nodes(jobid, site)
            worker_log("nodes = %s" % (nodes,))

            def prepare_package(package):
                worker_log("preparing package %s" % package)
                worker_log("copy %s files to nodes" % package)
                preparation = execo.Put(nodes,
                                        local_files = ([ pjoin(self.engine_dir, packages[package]["archive"]),
                                                         pjoin(self.engine_dir, "node_prepare_" +  package) ] +
                                                       [ pjoin(self.prepare_path, prepared_archive(dep, cluster)) for dep in packages[package]["deps"] ]),
                                        remote_location = node_working_dir,
                                        create_dirs = True,
                                        connexion_params = execo_g5k.default_oarsh_oarcp_params,
                                        name = "copy files")
                preparation.run()
                if not preparation.ok():
                    worker_log("aborting, copy of %s files failed:\n%s" % (package, execo.Report([preparation]).to_string()))
                    return False
                worker_log("compile %s" % package)
                compil = execo.Remote(
                    "%s/node_prepare_%s %s/%s %s %s %s > %s/%s.stdout" % (node_working_dir,
                                                                          package,
                                                                          node_working_dir,
                                                                          packages[package]["archive"],
                                                                          packages[package]["extract_dir"],
                                                                          prepared_archive(package, cluster),
                                                                          " ".join([ prepared_archive(dep, cluster) for dep in packages[package]["deps"] ]),
                                                                          node_working_dir,
                                                                          prepared_archive(package, cluster)),
                    nodes,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "compilation")
                compil.run()
                if not compil.ok():
                    worker_log("%s compilation failed:\n%s" % (package, execo.Report([compil]).to_string()))
                worker_log("retrieve result of %s compilation" % package)
                remote_files = [ pjoin(node_working_dir, prepared_archive(package, cluster) + ".stdout") ]
                if compil.ok():
                    remote_files.append(pjoin(node_working_dir, prepared_archive(package, cluster)))
                retrieval = execo.Get(
                    nodes,
                    remote_files = remote_files,
                    local_location = self.prepare_path,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "get built package")
                retrieval.run()
                if not retrieval.ok():
                    try:
                        os.unlink(pjoin(self.prepare_path, prepared_archive(package, cluster)))
                    except:
                        pass
                    worker_log("aborting, retrieval of %s compilation results failed:\n%s" % (package, execo.Report([retrieval]).to_string()))
                    return False
                worker_log("finished compilation of package %s" % package)
                return True

            for package in ["atlas", "openmpi", "hpl"]:
                if not pexists(pjoin(self.prepare_path, prepared_archive(package, cluster))):
                    prepare_package(package)

        finally:
            if jobid:
                worker_log("delete oar job")
                execo_g5k.oardel([(jobid, site)])
                jobid = None
            worker_log("exit")
