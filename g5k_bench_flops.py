import sys, os, shutil, time, math, threading, time
import execo, execo_g5k, execo_engine
from execo_g5k.planning import g5k_charter_time
from os.path import join as pjoin
import pprint
from common import *

def g5k_crossed_charter_boundary(t):
    # - param: a unix timestamp
    # - returns True if a g5k charter time boundary has been crossed
    #   since the last call. returns False if not or if it is the
    #   first call
    if g5k_crossed_charter_boundary.previous_t == None:
        retval = False
    else:
        retval = g5k_charter_time(g5k_crossed_charter_boundary.previous_t) != g5k_charter_time(t)
    g5k_crossed_charter_boundary.previous_t = t
    return retval
g5k_crossed_charter_boundary.previous_t = None

class g5k_bench_flops(execo_engine.Engine):

    def __init__(self):
        super(g5k_bench_flops, self).__init__()
        self.options_parser.set_usage("usage: %prog <comma separated list of clusters>")
        self.options_parser.set_description("compile and install openmpi, atlas, xhpl, then run xhpl bench to get max flops for cluster nodes")
        self.options_parser.add_option("-o", dest = "oar_options", help = "oar reservation options", default = None)
        self.options_parser.add_option("-w", dest = "walltime", help = "walltime of bench jobs", type = "string", default = "5:0:0")
        self.options_parser.add_option("-r", dest = "max_workers", help = "maximum number of concurrent worker jobs per cluster", type = "int", default = 20)
        self.options_parser.add_option("-t", dest = "max_waiting", help = "maximum number of concurrent waiting jobs per cluster", type = "int", default = 2)
        self.options_parser.add_option("-s", dest = "schedule_delay", help = "delay between rescheduling worker jobs", type = "int", default = 10)
        self.options_parser.add_option("-n", dest = "num_replicas", help = "num xp replicas: how many repetition of bench runs", type ="int", default = 5)
        self.options_parser.add_option("-C", dest = "charter", help = "activate submission of best-effort jobs during time periods where g5k charter is applicable", action =  "store_true", default = False)
        self.options_parser.add_argument("clusters", "comma separated list of clusters")
        self.prepare_path = pjoin(self.engine_dir, "preparation")

    def run(self):
        if len(self.args) != 1:
            print "ERROR: wrong number of arguments"
            self.options_parser.print_help(file=sys.stderr)
            exit(1)
        clusters_threads = dict()
        for cluster in self.args[0].split(","):
            clusters_threads[(cluster, execo_g5k.get_cluster_site(cluster))] = list()
        sites = set([s for (c,s) in clusters_threads.keys()])
        execo_engine.logger.info("clusters = %s" % (clusters_threads.keys(),))
        execo_engine.logger.info("sites = %s" % (sites,))
        parameters = {
            "cluster": {},
            "blas": ["atlas"],
            "num_nodes": [1],
            "xhpl_nb": [ 64, 128, 256 ],
            "repl": range(0, self.options.num_replicas)
            }
        for (cluster, site) in clusters_threads.keys():
            attrs = execo_g5k.get_host_attributes(cluster + "-1")
            num_cores = attrs["architecture"]["smt_size"]
            free_mem = attrs["main_memory"]["ram_size"] - 300000000
            big_size = int(math.sqrt(free_mem/8.0)*0.8)
            parameters["cluster"][(cluster, site)] = {
                "num_cores": {
                    1: {
                        "xhpl_grid": [ (1, 1) ],
                        "xhpl_n": [big_size // num_cores],
                        },
                    num_cores: {
                        "xhpl_grid": [ (int(num_cores / p), p) for p in range(1, int(math.sqrt(num_cores)) + 1) ],
                        "xhpl_n": [big_size],
                        }
                    },
                "xhpl_pfact": [0, 1, 2],
                "xhpl_rfact": [0, 1, 2],
                }
        execo_engine.logger.info("parameters:\n" + pprint.pformat(parameters))
        execo_engine.logger.info("len(sweeps) = %i" % len(execo_engine.sweep(parameters)))
        self.sweeper = execo_engine.ParamSweeper(pjoin(self.result_dir, "parameters"),
                                                 execo_engine.sweep(parameters),
                                                 save_sweeps = True)
        num_total_workers = 0
        while len(self.sweeper.get_remaining()) > 0:
            t = execo.Timer()
            #execo_engine.logger.debug("schedule loop iteration start. sweeper: %s" % (self.sweeper,))
            # when passing from non-charter to charter time period, or
            # the reverse, kill all previously subitted jobs still in
            # waiting:
            if self.options.charter and g5k_crossed_charter_boundary(time.time()):
                for cluster, site in clusters_threads.keys():
                    for w in clusters_threads[(cluster, site)]:
                        if w.waiting: w.to_delete = True
            jobs_to_kill = []
            for cluster, site in clusters_threads.keys():
                for w in clusters_threads[(cluster, site)]:
                    if w.waiting and w.to_delete and w.jobid:
                        jobs_to_kill.append((w.jobid, site))
            if len(jobs_to_kill) > 0:
                execo_engine.logger.info("g5k user charter status change, killing %s" % (jobs_to_kill,))
                execo_g5k.oardel(jobs_to_kill)
            for cluster, site in clusters_threads.keys():
                clusters_threads[(cluster, site)] = [w for w in clusters_threads[(cluster, site)] if w.is_alive()]
                num_workers = len(clusters_threads[(cluster, site)])
                num_waiting = len([w for w in clusters_threads[(cluster, site)] if w.waiting])
                num_combs_remaining = len([comb for comb in self.sweeper.get_remaining() if comb["cluster"] == (cluster, site) ])
                num_new_workers = min(self.options.max_workers - num_workers,
                                      self.options.max_waiting - num_waiting,
                                      num_combs_remaining)
                if num_new_workers > 0:
                    execo_engine.logger.info("rescheduling on cluster %s@%s: num_workers = %s / num_waiting = %s / num_combs_remaining = %s / num_new_workers = %s" %
                                             (cluster, site,
                                              num_workers,
                                              num_waiting,
                                              num_combs_remaining,
                                              num_new_workers))
                    for worker_index in range(0, num_new_workers):
                        th = threading.Thread(target = self.worker, args = (cluster, site, num_total_workers,), name = "bench flops worker %i - cluster = %s@%s" % (num_total_workers, cluster, site))
                        th.waiting = True
                        th.jobid = None
                        th.to_delete = False
                        th.site = site
                        th.start()
                        num_total_workers += 1
                        clusters_threads[(cluster, site)].append(th)
                else:
                    execo_engine.logger.debug("rescheduling on cluster %s@%s: num_workers = %s / num_waiting = %s / num_combs_remaining = %s / num_new_workers = %s" %
                                              (cluster, site,
                                               num_workers,
                                               num_waiting,
                                               num_combs_remaining,
                                               num_new_workers))
            #execo_engine.logger.debug("schedule loop iteration end. took: %ss." % (t.elapsed(),))
            execo.sleep(self.options.schedule_delay)
        execo_engine.logger.info("no more combinations to explore. exit schedule loop")

    def worker(self, cluster, site, worker_index):
        jobid = None
        comb = None

        def worker_log(arg):
            execo_engine.logger.info("worker #%i %s@%s - job %s: %s" % (
                worker_index,
                cluster,
                site,
                jobid,
                arg))

        def update_nodes(action):
            good_nodes = [ process.host() for process in action.processes() if process.finished_ok() ]
            num_lost_nodes = len(action.processes()) - len(good_nodes)
            if num_lost_nodes > 0:
                worker_log("lost %i nodes" % num_lost_nodes)
            return good_nodes

        try:
            comb = self.sweeper.get_next(filtr = lambda r: filter(lambda comb: comb["cluster"] == (cluster, site), r))
            worker_log("new comb: %s" % (comb,))

            if comb:
                # submit job
                worker_log("submit oar job")
                submission = execo_g5k.OarSubmission(resources = "{'cluster=\"%s\"'}/nodes=%i" % (cluster, comb['num_nodes']),
                                                     walltime = self.options.walltime,
                                                     name = "flopsworker",
                                                     additional_options = self.options.oar_options)
                if self.options.charter and g5k_charter_time(time.time()):
                    submission.job_type = "besteffort"
                ((jobid, _),) = execo_g5k.oarsub([(submission, site)])
                if not jobid:
                    worker_log("aborting, job submission failed")
                    self.sweeper.cancel(comb)
                    return
                threading.current_thread().jobid = jobid
                worker_log("job submitted - wait job start")
                execo_g5k.wait_oar_job_start(jobid, site, prediction_callback = lambda ts: worker_log("job start prediction: %s" % (execo.format_date(ts),)))
                threading.current_thread().waiting = False
                worker_log("job started - get job nodes")
                nodes = execo_g5k.get_oar_job_nodes(jobid, site)
                worker_log("nodes = %s" % (nodes,))
                # generate configuration
                comb_dir = pjoin(self.result_dir, execo_engine.slugify(comb))
                try:
                    os.makedirs(comb_dir)
                except OSError:
                    pass # if directory already exists (from a previously interrupted run)
                worker_log("generate bench params in %s" % (comb_dir,))
                xhplconf = """HPLinpack benchmark input file
Innovative Computing Laboratory, University of Tennessee
HPL.out      output file name (if any)
8            device out (6=stdout,7=stderr,file)
1            # of problems sizes (N)
{ns}         Ns
1            # of NBs
{nbs}        NBs
0            PMAP process mapping (0=Row-,1=Column-major)
1            # of process grids (P x Q)
{grid_ps}    Ps
{grid_qs}    Qs
16.0         threshold
1            # of panel fact
{pfacts}     PFACTs (0=left, 1=Crout, 2=Right)
2            # of recursive stopping criterium
2 4          NBMINs (>= 1)
1            # of panels in recursion
2            NDIVs
1            # of recursive panel fact.
{rfacts}     RFACTs (0=left, 1=Crout, 2=Right)
1            # of broadcast
0            BCASTs (0=1rg,1=1rM,2=2rg,3=2rM,4=Lng,5=LnM)
1            # of lookahead depth
0            DEPTHs (>=0)
2            SWAP (0=bin-exch,1=long,2=mix)
64           swapping threshold
0            L1 in (0=transposed,1=no-transposed) form
0            U  in (0=transposed,1=no-transposed) form
1            Equilibration (0=no,1=yes)
8            memory alignment in double (> 0)
"""
                xhplconf = xhplconf.format(
                    ns = comb["xhpl_n"],
                    nbs = comb["xhpl_nb"],
                    grid_ps = comb["xhpl_grid"][0],
                    grid_qs = comb["xhpl_grid"][1],
                    pfacts = comb["xhpl_pfact"],
                    rfacts = comb["xhpl_rfact"])
                with open(pjoin(comb_dir,"HPL.dat"), "w") as f:
                    print >> f, xhplconf
                # prepare nodes
                worker_log("copy files to nodes")
                preparation = execo.Put(
                    nodes,
                    local_files = ([ pjoin(self.prepare_path, prepared_archive(package, comb["cluster"][0]))
                                     for package in [ "atlas", "openmpi", "hpl" ] ] +
                                   [ pjoin(self.engine_dir, "node_bench_flops"),
                                     pjoin(comb_dir, "HPL.dat") ]),
                    remote_location = node_working_dir,
                    create_dirs = True,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "copy files")
                preparation.run()
                if not preparation.ok():
                    if preparation.stats()['num_ok'] < comb['num_nodes']:
                        worker_log("aborting, copy of files failed on too much nodes:\n" + execo.Report([preparation]).to_string())
                        self.sweeper.cancel(comb)
                        return
                nodes = update_nodes(preparation)
                # run bench
                worker_log("run bench on nodes")
                bench = execo.Remote(
                    "cd %s ; ./node_bench_flops %s %s %s %s > stdout" % (
                        node_working_dir,
                        comb["blas"],
                        comb["num_cores"],
                        packages["hpl"]["extract_dir"],
                        " ".join([ prepared_archive(package, comb["cluster"][0])
                                   for package in [ "atlas", "openmpi", "hpl" ] ])),
                    nodes,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "bench nb=%i p*q=%i*%i %s" % (
                        comb["xhpl_nb"],
                        comb["xhpl_grid"][0],
                        comb["xhpl_grid"][1],
                        comb["blas"]))
                bench.run()
                failed = False
                if not bench.ok():
                    if bench.stats()['num_ok'] < comb['num_nodes']:
                        failed = True
                        worker_log("bench failed on too much nodes:\n" + execo.Report([bench]).to_string())
                        self.sweeper.cancel(comb)
                # retrieve stdout from all node
                worker_log("retrieve logs in %s" % (comb_dir,))
                retrieval1 = execo.Get(
                    nodes,
                    remote_files = [
                        "%s/%s/bin/Linux_PII_CBLAS/HPL.dat" % (node_working_dir, packages["hpl"]["extract_dir"]),
                        "%s/stdout" % node_working_dir],
                    local_location = pjoin(comb_dir, "{{{host}}}"),
                    create_dirs = True,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "get logs nb=%i p*q=%i*%i %s" % (
                        comb["xhpl_nb"],
                        comb["xhpl_grid"][0],
                        comb["xhpl_grid"][1],
                        comb["blas"]))
                retrieval1.run()
                if failed:
                    worker_log("bench failed, got the logs, aborting")
                    return
                # retrieve results
                nodes = update_nodes(bench)
                worker_log("retrieve results in %s" % (comb_dir,))
                retrieval2 = execo.Get(
                    nodes,
                    remote_files = [ "%s/%s/bin/Linux_PII_CBLAS/HPL.out" % (node_working_dir, packages["hpl"]["extract_dir"]) ],
                    local_location = pjoin(comb_dir, "{{{host}}}"),
                    create_dirs = True,
                    connexion_params = execo_g5k.default_oarsh_oarcp_params,
                    name = "get results nb=%i p*q=%i*%i %s" % (
                        comb["xhpl_nb"],
                        comb["xhpl_grid"][0],
                        comb["xhpl_grid"][1],
                        comb["blas"]))
                retrieval2.run()
                if not retrieval2.ok():
                    if retrieval2.stats()['num_ok'] < comb['num_nodes']:
                        [ os.unlink(p) for p in find_files(comb_dir, "-name", "HPL.out") ]
                        worker_log("aborting, results retrieval failed on too much nodes:\n" + execo.Report([retrieval2]).to_string())
                        self.sweeper.cancel(comb)
                        return
                worker_log("finished combination %s" % (comb,))
                self.sweeper.done(comb)
        finally:
            if jobid:
                worker_log("delete oar job")
                execo_g5k.oardel([(jobid, site)])
                jobid = None
            worker_log("exit")
