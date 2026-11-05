#!/usr/bin/python3
"""Read-only Slurm overview. Python >=3.6; no third-party dependencies."""
import argparse
import builtins
import concurrent.futures
import csv
import io
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

VERSION = '2.1'
FIELD = re.compile(r'(?<!\S)([A-Za-z][A-Za-z0-9_/:]*)=(.*?)(?=\s+[A-Za-z][A-Za-z0-9_/:]*=|$)')
REMOTE = r'''LC_ALL=C; export LC_ALL
echo JOBLIST_MEM
awk '/^(MemTotal|MemAvailable):/ {print $1,$2}' /proc/meminfo
if command -v nvidia-smi >/dev/null 2>&1; then
 echo JOBLIST_GPU
 nvidia-smi --query-gpu=index,name,uuid,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null || echo JOBLIST_GPU_ERROR
 echo JOBLIST_PROCESSES
 nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits 2>/dev/null || echo JOBLIST_PROCESS_ERROR
else
 echo JOBLIST_NO_GPU_TOOL
fi
echo JOBLIST_DONE'''


def run(argv, timeout=8):
    p = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, start_new_session=True)
    try:
        out, err = p.communicate(timeout=timeout)
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        out, err = p.communicate()
        return 124, out, err


def fields(line):
    return dict(FIELD.findall(line))


def indices(value):
    if not value or value in ('N/A', '(null)', 'None'):
        return []
    result = set()
    for item in value.split(','):
        if re.match(r'^\d+$', item):
            result.add(int(item))
        elif re.match(r'^\d+-\d+$', item):
            first, last = map(int, item.split('-'))
            if 0 <= last - first <= 100000:
                result.update(range(first, last + 1))
    return sorted(result)


def gpu_resources(value):
    count, devices, has_indices = 0, set(), False
    for m in re.finditer(r'(?:^|,)gpu(?::([^:,()]+))?:(\d+)(?:\(([^)]*)\))?', value or ''):
        count += int(m.group(2))
        detail = m.group(3) or ''
        if detail.startswith('IDX:'):
            has_indices = True
            devices.update(indices(detail[4:]))
    return {'count': count, 'indices': sorted(devices), 'indices_known': has_indices}


def expand_hosts(spec):
    if not spec or spec in ('(null)', 'None'):
        return []
    if re.match(r'^[A-Za-z0-9_.-]+$', spec):
        return [spec]
    rc, out, err = run(['scontrol', 'show', 'hostnames', spec])
    if rc:
        raise RuntimeError('Cannot expand Slurm node list: ' + spec)
    return out.splitlines()


def parse_jobs(text):
    by_node = {}
    for line in text.splitlines():
        if not line.startswith('JobId='):
            continue
        f = fields(line)
        if f.get('JobState') not in ('RUNNING', 'COMPLETING', 'SUSPENDED'):
            continue
        number = f.get('JobId', '?')
        if f.get('ArrayJobId') and f.get('ArrayTaskId'):
            number = f['ArrayJobId'] + '_' + f['ArrayTaskId']
        job = {'id': number, 'user': f.get('UserId', '?').split('(')[0],
               'name': f.get('JobName', '?'), 'state': f.get('JobState'),
               'submit': f.get('SubmitTime', '?'), 'elapsed': f.get('RunTime', '?'),
               'cpus_total': int(f.get('NumCPUs', 0)), 'memory_requested': f.get('MinMemoryNode', f.get('MinMemoryCPU', '?'))}
        # Detailed resources are per-node; NumCPUs is the whole job allocation.
        detail = {}
        parts = re.split(r'(?<!\S)Nodes=', line)[1:]
        for part in parts:
            d = fields('Nodes=' + part)
            for host in expand_hosts(d.get('Nodes', '')):
                cpu_ids = indices(d.get('CPU_IDs', ''))
                detail[host] = {'cpus': len(cpu_ids) if cpu_ids else None,
                                'gpus': gpu_resources(d.get('GRES', ''))}
        hosts = expand_hosts(f.get('NodeList', ''))
        for host in hosts:
            item = dict(job)
            item.update(detail.get(host, {'cpus': job['cpus_total'] if len(hosts) == 1 else None,
                                          'gpus': {'count': None, 'indices': [], 'indices_known': False}}))
            by_node.setdefault(host, []).append(item)
    return by_node


def numeric(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_probe(text):
    result = {'memory': {}, 'gpus': [], 'processes': [], 'gpu_ok': False, 'process_ok': False}
    section = None
    for line in text.splitlines():
        if line.startswith('JOBLIST_'):
            section = line
            if line == 'JOBLIST_GPU':
                result['gpu_ok'] = True
            elif line == 'JOBLIST_PROCESSES':
                result['process_ok'] = True
            elif line == 'JOBLIST_GPU_ERROR':
                result['gpu_ok'] = False
            elif line == 'JOBLIST_PROCESS_ERROR':
                result['process_ok'] = False
            continue
        if section == 'JOBLIST_MEM':
            parts = line.split()
            if len(parts) == 2 and parts[0] in ('MemTotal:', 'MemAvailable:') and parts[1].isdigit():
                result['memory'][parts[0][:-1]] = int(parts[1]) / 1024.0
        elif section == 'JOBLIST_GPU':
            row = next(csv.reader([line]), [])
            if len(row) == 6 and row[0].strip().isdigit():
                result['gpus'].append({'index': int(row[0]), 'name': row[1].strip(), 'uuid': row[2].strip(),
                                      'util_percent': numeric(row[3]), 'memory_used_mib': numeric(row[4]),
                                      'memory_total_mib': numeric(row[5])})
        elif section == 'JOBLIST_PROCESSES':
            row = next(csv.reader([line]), [])
            if len(row) == 3 and row[1].strip().isdigit():
                result['processes'].append({'uuid': row[0].strip(), 'pid': int(row[1]), 'memory_mib': numeric(row[2])})
    return result


def probe(node, timeout, cache_seconds, refresh):
    if not re.match(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$', node):
        return {'error': 'Unsafe node name; probe skipped'}
    cache_path = None
    previous = None
    try:
        directory = os.path.join(os.path.expanduser('~'), '.cache', 'joblist-v2')
        os.makedirs(directory, mode=0o700, exist_ok=True)
        st = os.lstat(directory)
        if os.path.islink(directory) or st.st_uid != os.geteuid() or st.st_mode & 0o077:
            raise ValueError('Private cache directory required')
        cache_path = os.path.join(directory, socket.gethostname() + '-' + node + '.json')
        with open(cache_path) as f:
            previous = json.load(f)
        age = time.time() - previous['time']
        if not refresh and 0 <= age < cache_seconds:
            result = dict(previous)
            result['cache_age'] = age
            return result
    except (OSError, ValueError, KeyError, TypeError):
        pass
    rc, out, err = run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                        '-o', 'ConnectTimeout=' + str(max(1, int(timeout))),
                        '-o', 'ConnectionAttempts=1', '-o', 'ServerAliveInterval=1',
                        '-o', 'ServerAliveCountMax=1', node, REMOTE], timeout)
    result = {'time': time.time(), 'cache_age': 0, 'exit_code': rc}
    if rc == 0:
        result.update(parse_probe(out))
        if 'MemAvailable' not in result['memory']:
            result['error'] = 'Incomplete live memory response'
    else:
        result['error'] = 'Live SSH probe timed out' if rc == 124 else 'Live SSH probe failed (exit {})'.format(rc)
        if 'Host key verification failed' in err or 'host key is known' in err:
            result['error'] += '; host key must be confirmed locally'
    # Cache failures too: repeated display does not repeatedly hit a slow node.
    if cache_path:
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(prefix='.probe-', dir=directory)
            with os.fdopen(fd, 'w') as f:
                json.dump(result, f)
            os.replace(tmp, cache_path)
        except OSError:
            if tmp:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
    return result


def clean(value):
    return re.sub(r'[\x00-\x1f\x7f]', '?', str(value))


def gib(mib):
    return '{:.1f} GiB'.format(mib / 1024.0)


def color_enabled(mode):
    if mode != 'auto':
        return mode == 'always'
    return sys.stdout.isatty() and 'NO_COLOR' not in os.environ and os.environ.get('TERM') != 'dumb'


def colorize(line):
    colors = {'red': '1;31', 'green': '1;32', 'yellow': '1;33',
              'blue': '1;34', 'magenta': '1;35', 'cyan': '1;36', 'gray': '0;90'}

    def paint(value, name):
        return '\033[{}m{}\033[0m'.format(colors[name], value)

    def state_color(value):
        if any(word in value for word in ('DOWN', 'FAIL', 'INVALID', 'ERROR')):
            return 'red'
        if any(word in value for word in ('DRAIN', 'MIXED', 'SUSPENDED', 'COMPLETING')):
            return 'yellow'
        return 'green'

    if line.startswith('joblist '):
        return paint(line, 'cyan')
    if line.startswith('[Node:'):
        return re.sub(r'^\[Node: (.*?) \| State: (.*?)\]$',
                      lambda m: paint('[Node: ', 'cyan') + paint(m[1], 'green') +
                      paint(' | State: ', 'cyan') + paint(m[2], state_color(m[2])) + paint(']', 'cyan'), line)
    if line.startswith('NOTE:') or 'not tracked;' in line or 'Live data: Live ' in line:
        return paint(line, 'yellow')
    if any(word in line for word in ('Snapshot age:', 'not queried', 'No running/', 'Pending jobs are not listed',
                                     'allocation = Slurm reservations')):
        return paint(line, 'gray')
    line = re.sub(r'\b(UNALLOCATED|ALLOCATED)\b',
                  lambda m: paint(m[0], 'green' if m[0] == 'UNALLOCATED' else 'yellow'), line)
    line = re.sub(r'(Allocated )(\d+)', lambda m: m[1] + paint(m[2], 'red' if int(m[2]) else 'gray'), line)
    line = re.sub(r'(Unallocated )(\d+)', lambda m: m[1] + paint(m[2], 'green' if int(m[2]) else 'red'), line)
    line = re.sub(r'((?:Total |Physical |configured ))(\d+(?:\.\d+)?(?: GiB)?)',
                  lambda m: m[1] + paint(m[2], 'blue'), line)
    line = re.sub(r'(Available )(\d+(?:\.\d+)? GiB)', lambda m: m[1] + paint(m[2], 'green'), line)
    line = re.sub(r'(Non-available )(\d+(?:\.\d+)? GiB)', lambda m: m[1] + paint(m[2], 'red'), line)
    line = re.sub(r'(Util )(\d+(?:\.\d+)?%)',
                  lambda m: m[1] + paint(m[2], 'red' if float(m[2][:-1]) else 'green'), line)
    line = re.sub(r'((?:Nonzero utilization |Compute-process cards ))(\d+/\d+)',
                  lambda m: m[1] + paint(m[2], 'red' if int(m[2].split('/')[0]) else 'green'), line)
    line = re.sub(r'(Job:)(\S+)', lambda m: m[1] + paint(m[2], 'cyan'), line)
    line = re.sub(r'(User:)(\S+)', lambda m: m[1] + paint(m[2], 'magenta'), line)
    line = re.sub(r'(State:)(\S+)', lambda m: m[1] + paint(m[2], state_color(m[2])), line)
    line = re.sub(r'(GPUs on node:)(\d+)',
                  lambda m: m[1] + paint(m[2], 'yellow' if int(m[2]) else 'gray'), line)
    line = re.sub(r'^(\s*)(CPU:|MEM:|GPU Slurm:|GPU live:|GPU:)',
                  lambda m: m[1] + paint(m[2], 'cyan'), line)
    if 'unknown' in line:
        line = line.replace('unknown', paint('unknown', 'yellow'))
    return line


def display(data, color=False):
    def print(line=''):
        builtins.print(colorize(line) if color else line)

    print('joblist {} | {} | live cache {}s (use --refresh to bypass)'.format(
        VERSION, data['time'], data['cache_seconds']))
    print('CPU/GPU allocation = Slurm reservations; GPU utilization = sampled hardware activity.\n')
    if not data['memory_scheduling']:
        print('NOTE: Memory reservation/enforcement is not enabled in Slurm ({}).\n'.format(data['select_parameters']))
    for node in data['nodes']:
        f, live, jobs = node['slurm'], node['live'], node['jobs']
        print('[Node: {} | State: {}]'.format(clean(f['NodeName']), clean(f.get('State', '?'))))
        used, total = int(f.get('CPUAlloc', 0)), int(f.get('CPUTot', 0))
        print('  CPU: Allocated {} / Total {} | Unallocated {} | Load {}'.format(used, total, total-used, f.get('CPULoad', 'N/A')))
        if live.get('error'):
            print('  Live data: {} (not a Slurm health diagnosis)'.format(live['error']))
        if live.get('skipped'):
            print('  Live data: not queried (--slurm-only)')
        if live.get('cache_age', 0) > 0:
            print('  Snapshot age: {:.1f}s'.format(live['cache_age']))
        memory = live.get('memory', {})
        if 'MemTotal' in memory and 'MemAvailable' in memory:
            total_mem, available = memory['MemTotal'], memory['MemAvailable']
            print('  MEM: Available {} / Physical {} | Non-available {}'.format(gib(available), gib(total_mem), gib(total_mem-available)))
        else:
            print('  MEM: Live available unknown | Slurm configured {}'.format(gib(int(f.get('RealMemory', 0)))))
        total_gpu = gpu_resources(f.get('Gres', ''))['count']
        allocated = gpu_resources(f.get('GresUsed', '')) if 'GresUsed' in f else None
        live_gpus = live.get('gpus', [])
        if total_gpu or live_gpus:
            if allocated is not None:
                print('  GPU Slurm: Allocated {} / Total {} | Unallocated {}'.format(allocated['count'], total_gpu, max(0, total_gpu-allocated['count'])))
            else:
                print('  GPU Slurm: allocation unknown / Registered {}'.format(total_gpu))
            if live_gpus:
                busy = sum(1 for g in live_gpus if g['util_percent'] is not None and g['util_percent'] > 0)
                active = len(set(p['uuid'] for p in live.get('processes', [])))
                print('  GPU live: Nonzero utilization {}/{} | Compute-process cards {}'.format(
                    busy, len(live_gpus), '{}/{}'.format(active, len(live_gpus)) if live.get('process_ok') else 'unknown'))
                owners = {}
                for job in jobs:
                    for index in job['gpus']['indices']:
                        owners.setdefault(index, []).append(job['id'])
                for g in live_gpus:
                    index = g['index']
                    if allocated is not None and allocated['indices_known']:
                        status = 'ALLOCATED' if index in allocated['indices'] else 'UNALLOCATED'
                    else:
                        status = 'allocation unknown'
                    nproc = sum(1 for proc in live.get('processes', []) if proc['uuid'] == g['uuid'])
                    utilization = '{}%'.format(g['util_percent']) if g['util_percent'] is not None else 'N/A'
                    mem = '{}/{} MiB'.format(g['memory_used_mib'], g['memory_total_mib'])
                    print('    #{} {} | {} | Util {} | VRAM {} | Compute PIDs {} | Jobs {}'.format(
                        index, clean(g['name']), status, utilization, mem,
                        nproc if live.get('process_ok') else '?', ','.join(owners.get(index, [])) or '-'))
            else:
                print('  GPU live: unknown/not queried; allocation above remains current')
        if not jobs:
            print('    |-- No running/completing/suspended jobs')
        for job in jobs:
            gpu = job['gpus']
            desc = '{}'.format(gpu['count']) if gpu['count'] is not None else '?'
            if gpu['indices']:
                desc += ' [physical #' + ',#'.join(map(str, gpu['indices'])) + ']'
            job_line = '    |-- Job:{} | User:{} | State:{} | CPUs on node:{}'.format(
                clean(job['id']), clean(job['user']), job['state'], job['cpus'] if job['cpus'] is not None else '?')
            if total_gpu or live_gpus:
                job_line += ' | GPUs on node:{}'.format(desc)
            print(job_line)
            print('        Name:{} | Submitted:{} | Elapsed:{}'.format(clean(job['name']), clean(job['submit']), clean(job['elapsed'])))
        print()
    print('Pending jobs are not listed. Live snapshots and Slurm queries are not atomic.')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--slurm-only', action='store_true', help='Skip SSH; keep current Slurm CPU/GPU allocations')
    parser.add_argument('--timeout', type=float, default=3, help='Hard per-node live-probe timeout (seconds, default 3)')
    parser.add_argument('--cache-seconds', type=float, default=10, help='Live snapshot TTL (default 10; 0 disables reuse)')
    parser.add_argument('--refresh', action='store_true', help='Bypass live snapshot cache')
    parser.add_argument('--json', action='store_true', help='Machine-readable output')
    parser.add_argument('--color', choices=('auto', 'always', 'never'), default='auto',
                        help='Terminal colors (default auto; redirected/JSON output stays plain)')
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.timeout > 120 or args.cache_seconds < 0 or args.cache_seconds > 3600:
        parser.error('timeout must be in (0,120]; cache-seconds must be in [0,3600]')
    commands = [['scontrol', 'show', 'nodes', '-d', '-o'],
                ['scontrol', 'show', 'jobs', '-d', '-o'], ['scontrol', 'show', 'config']]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, commands))
    for cmd, result in zip(commands, results):
        if result[0]:
            raise RuntimeError('{} failed: {}'.format(' '.join(cmd), result[2].strip() or result[0]))
    nodes = [fields(l) for l in results[0][1].splitlines() if l.startswith('NodeName=')]
    nodes = [n for n in nodes if n.get('Partitions') not in (None, '', '(null)')]
    nodes.sort(key=lambda n: [int(x) if x.isdigit() else x for x in re.split(r'(\d+)', n['NodeName'])])
    jobs = parse_jobs(results[1][1])
    m = re.search(r'^SelectTypeParameters\s*=\s*(\S+)', results[2][1], re.M)
    select = m.group(1) if m else 'UNKNOWN'
    memory_scheduling = 'MEMORY' in select.upper()
    live = {}
    if args.slurm_only:
        live = {n['NodeName']: {'skipped': True} for n in nodes}
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(len(nodes), 16))) as pool:
            pending = {pool.submit(probe, n['NodeName'], args.timeout, args.cache_seconds, args.refresh): n['NodeName'] for n in nodes}
            for task in concurrent.futures.as_completed(pending):
                live[pending[task]] = task.result()
    data = {'version': VERSION, 'time': time.strftime('%Y-%m-%d %H:%M:%S %z'),
            'cache_seconds': args.cache_seconds, 'select_parameters': select, 'memory_scheduling': memory_scheduling,
            'nodes': [{'slurm': n, 'jobs': jobs.get(n['NodeName'], []), 'live': live[n['NodeName']]} for n in nodes]}
    if args.json:
        print(json.dumps(data))
    else:
        display(data, color_enabled(args.color))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print('joblist: {}'.format(error), file=sys.stderr)
        sys.exit(1)
