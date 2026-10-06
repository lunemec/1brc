#!/usr/bin/env python3
"""Check the real host and monitor background work around a benchmark.

CPU limits are expressed in cores (1.0 = one logical CPU kept fully busy).
This operational check complements, rather than replaces, a null control.
"""
import argparse
import ctypes
from datetime import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time


def ram_swap_devices(devices):
    if not devices:
        return False
    for device in devices:
        if not re.fullmatch(r'/dev/zram\d+', device):
            return False
        try:
            if (Path('/sys/block') / Path(device).name / 'backing_dev').read_text().strip() != 'none':
                return False
        except OSError:
            return False
    return True


def mac_snapshot():
    raw = subprocess.check_output(['ps', '-A', '-o', 'pid=,ppid=,lstart=,time=,comm='],
                                  text=True, env={**os.environ, 'LC_ALL': 'C'})
    processes = {}
    for line in raw.splitlines():
        fields = line.split(None, 8)
        pid, parent, clock, command = fields[0], fields[1], fields[7], fields[8]
        birth = datetime.strptime(' '.join(fields[2:7]), '%a %b %d %H:%M:%S %Y').timestamp()
        parts = clock.split(':')
        seconds = sum(float(part) * 60 ** i for i, part in enumerate(reversed(parts)))
        processes[int(pid)] = {'parent': int(parent), 'ticks': round(seconds * 100),
                               'start': birth, 'state': 'R', 'name': Path(command).name}
    if processes.get(1, {}).get('name') != 'launchd':
        raise RuntimeError('incomplete macOS host process view; run on the host')
    vm = subprocess.check_output(['vm_stat'], text=True)
    page_size = int(re.search(r'page size of (\d+)', vm)[1])
    values = {name: int(value) for name, value in re.findall(r'^([^:\n]+):\s*(\d+)', vm, re.M)}
    available = sum(values.get(name, 0) for name in
                    ('Pages free', 'Pages inactive', 'Pages speculative')) * page_size // 1024
    thermal = subprocess.check_output(['pmset', '-g', 'therm'], text=True, stderr=subprocess.STDOUT)
    limits = [int(x) for x in re.findall(r'CPU_(?:Speed|Scheduler)_Limit\s*=\s*(\d+)', thermal)]
    return {'time': time.monotonic(), 'wall_time': time.time(), 'processes': processes, 'cpu': None,
            'swap': values.get('Swapins', 0) + values.get('Swapouts', 0),
            'swap_in': values.get('Swapins', 0), 'swap_out': values.get('Swapouts', 0),
            'page_bytes': page_size,
            'available_kib': available, 'pressure': {}, 'temperatures_c': {},
            'thermal_limited': any(limit < 100 for limit in limits), 'pid1': 'launchd'}


def snapshot():
    if sys.platform == 'darwin':
        return mac_snapshot()
    if sys.platform != 'linux':
        raise RuntimeError('automatic host quietness checking requires Linux or macOS')
    init = Path('/proc/1/comm').read_text().strip()
    if init not in ('systemd', 'init'):
        raise RuntimeError(f'incomplete host process view (PID 1 is {init!r}); run on the host')
    processes = {}
    for path in Path('/proc').glob('[0-9]*/stat'):
        try:
            raw = path.read_text()
            fields = raw.rsplit(')', 1)[1].split()
            processes[int(path.parent.name)] = {
                'name': raw[raw.index('(') + 1:raw.rindex(')')],
                'parent': int(fields[1]), 'ticks': int(fields[11]) + int(fields[12]),
                'children_ticks': int(fields[13]) + int(fields[14]),
                'start': int(fields[19]), 'state': fields[0]}
        except PermissionError as exc:
            raise RuntimeError('incomplete host process view: unreadable process counters') from exc
        except (OSError, ValueError, IndexError):
            pass  # A process may exit between listing and reading it.
    cpu = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    memory = dict((line.split()[0].rstrip(':'), int(line.split()[1]))
                  for line in Path('/proc/meminfo').read_text().splitlines())
    pressure = {}
    for kind in ('cpu', 'io', 'memory'):
        path = Path('/proc/pressure') / kind
        if path.exists():
            pressure[kind] = {line.split()[0]: int(line.split()[-1].split('=')[1])
                              for line in path.read_text().splitlines()}
    temperatures = {}
    for hwmon in Path('/sys/class/hwmon').glob('hwmon*'):
        try:
            name = (hwmon / 'name').read_text().strip()
            for sensor in hwmon.glob('temp*_input'):
                temperatures[name + '/' + sensor.name] = int(sensor.read_text()) / 1000
        except OSError:
            pass
    swap_devices = [line.split()[0] for line in Path('/proc/swaps').read_text().splitlines()[1:]]
    return {'time': time.monotonic(), 'boot_time': time.clock_gettime(time.CLOCK_BOOTTIME),
            'processes': processes, 'cpu': cpu,
            'swap': int(vm['pswpin']) + int(vm['pswpout']),
            'swap_in': int(vm['pswpin']), 'swap_out': int(vm['pswpout']),
            'ram_swap_only': ram_swap_devices(swap_devices),
            'page_bytes': os.sysconf('SC_PAGE_SIZE'),
            'available_kib': memory['MemAvailable'], 'pressure': pressure,
            'temperatures_c': temperatures, 'pid1': init}


def descendants(processes, root):
    owned = {root}
    while True:
        more = {pid for pid, p in processes.items() if p['parent'] in owned}
        if more <= owned:
            return owned
        owned |= more


def same_timed_process(before, after, names):
    def active(sample):
        owned = descendants(sample['processes'], os.getpid())
        return {(pid, p['start']) for pid, p in sample['processes'].items()
                if pid in owned and p['name'] in names and p['state'] != 'Z'}
    return bool(active(before) & active(after))


def assess(before, after, root=None, max_cores=0.5, max_process=0.25, preparing=False, check_io=True):
    elapsed = after['time'] - before['time']
    hz = 100 if sys.platform == 'darwin' else os.sysconf('SC_CLK_TCK')
    owner = os.getpid() if root is not None else None
    excluded = descendants(before['processes'], owner) | descendants(after['processes'], owner)
    excluded.add(os.getpid())
    rows = []
    for pid, process in after['processes'].items():
        if pid in excluded or process['state'] == 'Z':
            continue
        previous = before['processes'].get(pid)
        if previous and previous['start'] == process['start']:
            ticks = process['ticks'] - previous['ticks']
        elif ((sys.platform == 'darwin' and process['start'] >= int(before['wall_time'])) or
              (sys.platform != 'darwin' and process['start'] / hz >= before.get('boot_time', before['time']))):
            ticks = process['ticks']  # Newly launched background process.
        else:
            continue
        cores = max(0, ticks) / hz / elapsed
        if cores:
            rows.append({'pid': pid, 'name': process['name'], 'cpu_cores': cores})
    process_background = sum(r['cpu_cores'] for r in rows)
    background = process_background
    if after['cpu'] is None:
        busy, iowait = background, 0  # macOS exposes per-process CPU; Linux also measures kernel CPU.
    else:
        delta = [b - a for a, b in zip(before['cpu'], after['cpu'])]
        busy = (sum(delta) - delta[3] - delta[4]) / hz / elapsed
        iowait = delta[4] / hz / elapsed
        if root is not None:
            # Waited-child counters roll into their ancestors, retaining CPU
            # consumed by benchmark subprocesses that exit between snapshots.
            def owned_ticks(sample):
                own = descendants(sample['processes'], owner)
                return sum(p['ticks'] + p.get('children_ticks', 0)
                           for pid, p in sample['processes'].items() if pid in own)
            owned_cpu = max(0, owned_ticks(after) - owned_ticks(before)) / hz / elapsed
            background = max(background, busy - owned_cpu)
    pressure = {kind: {state: (total - before['pressure'].get(kind, {}).get(state, total))
                              / elapsed / 10000
                       for state, total in states.items()}
                for kind, states in after['pressure'].items()}
    reasons = []
    if (busy if root is None else process_background) > max_cores:
        reasons.append('background CPU exceeds limit')
    elif root is not None and background > max_cores:
        reasons.append('unattributed host CPU exceeds limit')
    if rows and max(r['cpu_cores'] for r in rows) > max_process:
        reasons.append('individual background process exceeds limit')
    if not preparing and check_io and (iowait > 0.1 or pressure.get('io', {}).get('full', 0) > 0.5):
        reasons.append('I/O contention')
    if not preparing and pressure.get('memory', {}).get('full', 0) > 0.1:
        reasons.append('memory contention')
    if root is None and pressure.get('cpu', {}).get('some', 0) > 2:
        reasons.append('idle CPU contention')
    swap_bytes = (after['swap'] - before['swap']) * after.get('page_bytes', 4096)
    swap_in_pages = after.get('swap_in', 0) - before.get('swap_in', 0)
    swap_out_pages = after.get('swap_out', after['swap']) - before.get('swap_out', before['swap'])
    swap_out_bytes = swap_out_pages * after.get('page_bytes', 4096)
    ram_swap_only = bool(before.get('ram_swap_only') and after.get('ram_swap_only'))
    paging_limit_bytes = swap_out_bytes if ram_swap_only else swap_bytes
    if not preparing and paging_limit_bytes / elapsed > 64 * 1024:
        reasons.append('active swapping')
    if after['available_kib'] < 2 * 1024 * 1024:
        reasons.append('less than 2 GiB available memory')
    if after.get('thermal_limited'):
        reasons.append('macOS reports a thermal CPU limit')
    return {'seconds': elapsed, 'pass': not reasons, 'reasons': reasons,
            'busy_cpu_cores': busy, 'background_cpu_cores': background,
            'iowait_cpu_cores': iowait, 'pressure_percent': pressure,
            'swap_pages_delta': after['swap'] - before['swap'],
            'swap_bytes_per_second': swap_bytes / elapsed,
            'swap_in_pages_delta': swap_in_pages, 'swap_out_pages_delta': swap_out_pages,
            'swap_out_bytes_per_second': swap_out_bytes / elapsed,
            'ram_swap_only': ram_swap_only,
            'available_kib': after['available_kib'],
            'temperatures_c': after['temperatures_c'],
            'background_processes': sorted(rows, key=lambda r: -r['cpu_cores'])[:15]}


def remember_owned(sample, child, identities):
    # Our live wrapper is a trustworthy ancestor. A reaped command PID is not:
    # it may already identify an unrelated process after PID reuse.
    owned = descendants(sample['processes'], os.getpid())
    for pid, birth in identities.items():
        current = sample['processes'].get(pid)
        if current and current['start'] == birth:
            owned |= descendants(sample['processes'], pid)
    owned.discard(os.getpid())
    for pid in owned:
        if pid in sample['processes']:
            identities.setdefault(pid, sample['processes'][pid]['start'])


def reap(child):
    rows = []
    if sys.platform != 'linux':
        child.poll()
        return rows, True
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return rows, True
        if not pid:
            # A zombie leader can still have runtime threads tearing down.
            # WNOHANG=0 means children remain, not that they have completed.
            return rows, False
        code = os.waitstatus_to_exitcode(status)
        if pid == child.pid:
            child.returncode = code
        else:
            rows.append({'pid': pid, 'returncode': code})


def finish_owned(child, identities, terminate=False):
    """Wait/reap owned descendants; signal only a currently verified birth identity."""
    started = time.monotonic()
    rows = []
    while True:
        sample = snapshot()
        remember_owned(sample, child, identities)
        live = {pid: p for pid, p in sample['processes'].items()
                if pid in identities and p['start'] == identities[pid] and p['state'] != 'Z'}
        reaped, no_children = reap(child)
        rows.extend(reaped)
        if not live and no_children:
            child.wait()
            return rows
        elapsed = time.monotonic() - started
        if terminate or elapsed > 5:
            sig = signal.SIGKILL if elapsed > 2 else signal.SIGTERM
            def depth(pid):
                n = 0
                seen = set()
                while pid in live and pid not in seen:
                    seen.add(pid)
                    pid = live[pid]['parent']
                    n += 1
                return n
            for pid in sorted(live, key=depth, reverse=True):
                # Recheck immediately before each signal, after ancestors may exit.
                current = snapshot()['processes'].get(pid)
                if current and current['start'] == identities[pid]:
                    try:
                        os.kill(pid, sig)
                    except ProcessLookupError:
                        pass
        if elapsed > 15:
            raise RuntimeError('owned benchmark descendants did not exit after cleanup')
        time.sleep(0.05)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--report', type=Path, required=True)
    ap.add_argument('--seconds', type=float, default=10)
    ap.add_argument('--interval', type=float, default=2)
    ap.add_argument('--max-background-cores', type=float, default=0.5)
    ap.add_argument('--max-process-cores', type=float, default=0.25)
    ap.add_argument('--defer-io-until-ready', action='store_true',
                    help='harness signals completed cache preparation using BRC_QUIET_READY_FILE')
    ap.add_argument('--timed-process-names', help='comma-separated process names; apply IO gate to stable active-process intervals')
    ap.add_argument('command', nargs=argparse.REMAINDER)
    args = ap.parse_args()
    if args.seconds <= 0 or args.interval <= 0 or args.seconds > 60:
        ap.error('positive sample durations required; preflight must be at most 60 seconds')
    args.report.parent.mkdir(parents=True, exist_ok=True)
    report = {'schema': '1brc-host-quiet-v1', 'preflight_seconds': args.seconds,
              'max_background_cores': args.max_background_cores,
              'max_process_cores': args.max_process_cores,
              'paging_policy': '64KiB/s; RAM-only swap-ins recorded, assessed by CPU/pressure; swap-outs still reject',
              'timed_process_names': args.timed_process_names,
              'samples': []}
    def save():
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    child = None
    identities = {}
    ready = None
    adopted = []
    phase_directory = None
    try:
        before = snapshot()
        time.sleep(args.seconds)
        after = snapshot()
        report['preflight'] = assess(before, after, max_cores=args.max_background_cores,
                                      max_process=args.max_process_cores)
        save()
        if not report['preflight']['pass']:
            print('host quietness check failed:', ', '.join(report['preflight']['reasons']), flush=True)
            return 2
        print('host quietness check passed; report:', args.report, flush=True)
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        if not command:
            return 0
        if sys.platform == 'linux':
            libc = ctypes.CDLL(None, use_errno=True)
            if libc.prctl(36, 1, 0, 0, 0):  # PR_SET_CHILD_SUBREAPER
                raise OSError(ctypes.get_errno(), 'enable benchmark child subreaper')
        def interrupted(signum, frame):
            raise KeyboardInterrupt(f'signal {signum}')
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        phase_directory = tempfile.TemporaryDirectory(prefix='1brc-quiet-')
        ready = Path(phase_directory.name) / 'timing.ready'
        env = {**os.environ, 'BRC_QUIET_GUARD_PID': str(os.getpid()),
               'BRC_QUIET_READY_FILE': str(ready.resolve())}
        child = subprocess.Popen(command, env=env, start_new_session=True)
        before = snapshot()
        remember_owned(before, child, identities)
        timed_names = set(args.timed_process_names.split(',')) if args.timed_process_names else None
        def assess_interval(before, after, timing_ready):
            check_io = timed_names is None or same_timed_process(before, after, timed_names)
            result = assess(before, after, root=child.pid,
                            max_cores=args.max_background_cores,
                            max_process=args.max_process_cores,
                            preparing=not timing_ready, check_io=check_io)
            result['io_gate_applied'] = timing_ready and check_io
            return result
        while child.poll() is None:
            # Generic commands are monitored immediately. Harnesses mark the end
            # of cache warming so their own cold reads are not blamed on other work.
            timing_ready = not args.defer_io_until_ready or ready.exists()
            try:
                child.wait(timeout=args.interval)
            except subprocess.TimeoutExpired:
                pass
            after = snapshot()
            remember_owned(after, child, identities)
            sample = assess_interval(before, after, timing_ready)
            if sample['reasons'] == ['unattributed host CPU exceeds limit']:
                # Process counters are not an atomic tree snapshot. A child can
                # exit before its waited CPU appears in its parent's counters.
                # Re-read the same interval after that bookkeeping can settle.
                time.sleep(0.05)
                after = snapshot()
                remember_owned(after, child, identities)
                sample = assess_interval(before, after, timing_ready)
            sample['timing_ready'] = timing_ready
            sample['preparation_activity_only'] = not timing_ready and not sample['pass']
            report['samples'].append(sample)
            before = after
            if not sample['pass'] and timing_ready:
                report['valid_timing_window'] = False
                save()
                print('benchmark interrupted by background activity:', ', '.join(sample['reasons']), flush=True)
                report['adopted_children'] = finish_owned(child, identities, terminate=True)
                save()
                return 2
            if len(report['samples']) % 5 == 0:
                save()
        adopted = finish_owned(child, identities)
        report['adopted_children'] = adopted
        report['command_exit_code'] = child.returncode
        report['valid_timing_window'] = child.returncode == 0 and all(r['returncode'] == 0 for r in adopted)
        save()
        if child.returncode == 0 and not report['valid_timing_window']:
            return 2
        return child.returncode if child.returncode >= 0 else 128 - child.returncode
    except (OSError, RuntimeError, KeyboardInterrupt) as exc:
        report['error'] = str(exc)
        report['valid_timing_window'] = False
        save()
        if child is not None:
            for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                signal.signal(signum, signal.SIG_IGN)
            report['adopted_children'] = finish_owned(child, identities, terminate=True)
            save()
        print('host quietness check unavailable:', exc, file=sys.stderr)
        return 2
    finally:
        if ready is not None:
            ready.unlink(missing_ok=True)
        if phase_directory is not None:
            phase_directory.cleanup()


if __name__ == '__main__':
    sys.exit(main())
