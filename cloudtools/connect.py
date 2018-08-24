import sys
import os
from subprocess import Popen, check_call


def init_parser(parser):
    parser.add_argument('name', type=str, help='Cluster name.')
    parser.add_argument('service', type=str,
                        choices=['notebook', 'nb', 'spark-ui', 'ui', 'spark-ui1', 'ui1',
                                 'spark-ui2', 'ui2', 'spark-history', 'hist'],
                        help='Web service to launch.')
    parser.add_argument('--port', '-p', default='10000', type=str,
                        help='Local port to use for SSH tunnel to master node (default: %(default)s).')
    parser.add_argument('--zone', '-z', default='us-central1-b', type=str,
                        help='Compute zone for Dataproc cluster (default: %(default)s).')

def main(args):
    print("Connecting to cluster '{}'...".format(args.name))

    # shortcut mapping
    shortcut = {
        'ui': 'spark-ui',
        'ui1': 'spark-ui1',
        'ui2': 'spark-ui2',
        'hist': 'history',
        'nb': 'notebook'
    }

    service = args.service
    if service in shortcut:
        service = shortcut[service]

    # Dataproc port mapping
    dataproc_ports = {
        'spark-ui': 4040,
        'spark-ui1': 4041,
        'spark-ui2': 4042,
        'spark-history': 18080,
        'notebook': 8123
    }
    connect_port = dataproc_ports[service]

    # open SSH tunnel to master node
    cmd = [
        'gcloud',
        'compute',
        'ssh',
        '{}-m'.format(args.name),
        '--zone={}'.format(args.zone),
        '--ssh-flag=-D {}'.format(args.port),
        '--ssh-flag=-N',
        '--ssh-flag=-f',
        '--ssh-flag=-n'
    ]
    with open(os.devnull, 'w') as f:
        check_call(cmd, stdout=f, stderr=f)

    if sys.platform.startswith('linux'):
        names = ['chromium-browser', 'chromium', 'google-chrome']
        # attempt to find a chrome/chromium browser in the user's PATH
        for name in names:
            browser = which(name)
            if browser is not None:
                break
        if browser is None:
            raise Exception('could not find a chromium browser. searched for {}'.format(names))
    else:
        browser = r'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'

    # open Chrome/Chromium with SOCKS proxy configuration
    cmd = [
        browser,
        'http://localhost:{}'.format(connect_port),
        '--proxy-server=socks5://localhost:{}'.format(args.port),
        '--host-resolver-rules=MAP * 0.0.0.0 , EXCLUDE localhost',
        '--user-data-dir=/tmp/'
    ]
    with open(os.devnull, 'w') as f:
        Popen(cmd, stdout=f, stderr=f)


def which(cmd):
    """An almost verbatim copy of python 3.3 and later's shutil.which(), used to provide
    a compatible way to find an executable for python2.7

    Almost verbatim means that the windows specific portions have been removed, as have any
    tunable parameters"""
    # Check that a given file can be accessed with the correct mode.
    # Additionally check that `file` is not a directory, as on Windows
    # directories pass the os.access check.
    def _access_check(fn):
        mode = os.F_OK | os.X_OK
        return (os.path.exists(fn) and os.access(fn, mode)
                and not os.path.isdir(fn))

    # If we're given a path with a directory part, look it up directly rather
    # than referring to PATH directories. This includes checking relative to the
    # current directory, e.g. ./script
    if os.path.dirname(cmd):
        if _access_check(cmd):
            return cmd
        return None

    path = os.environ.get("PATH", os.defpath)
    if not path:
        return None
    path = path.split(os.pathsep)

    seen = set()
    for dir in path:
        normdir = os.path.normcase(dir)
        if normdir not in seen:
            seen.add(normdir)
            name = os.path.join(dir, cmd)
            if _access_check(name):
                return name
    return None
