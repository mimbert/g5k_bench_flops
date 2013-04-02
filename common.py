import subprocess

packages = {
    "atlas": {
        "version": "3.10.0",
        "archive": "atlas3.10.0.tar.bz2",
        "extract_dir": "ATLAS",
        "deps": []
        },
    "openmpi": {
        "version": "1.6.3",
        "archive": "openmpi-1.6.3.tar.bz2",
        "extract_dir": "openmpi-1.6.3",
        "deps": []
        },
    "hpl": {
        "version": "2.1",
        "archive": "hpl-2.1.tar.gz",
        "extract_dir": "hpl-2.1",
        "deps": [ "atlas", "openmpi" ]
        }
    }

node_working_dir = "/tmp/benchflops"
preparation_dir = "preparation"

def prepared_archive(package, cluster):
    return "compiled-%s-%s-%s.tgz" % (package, packages[package]["version"], cluster)

def find_files(*args):
    """run find utility with given path(es) and parameters, return the result as a list"""
    find_args = "find " + " ".join([quote(arg) for arg in args])
    p = subprocess.Popen(find_args, shell = True,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    (stdout, stderr) = p.communicate()
    p.wait()
    return [ p for p in stdout.split("\n") if p ]
