import copy
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import benchmark_quiet as quiet


class QuietnessTests(unittest.TestCase):
    def test_benchmark_children_do_not_hide_background_work(self):
        def process(parent, ticks, start=0):
            return {'parent': parent, 'ticks': ticks, 'start': start,
                    'state': 'R', 'name': 'work'}
        before = {'time': 0, 'cpu': [0] * 8, 'swap': 0, 'pressure': {},
                  'available_kib': 4 * 1024 * 1024, 'temperatures_c': {},
                  'processes': {10: process(os.getpid(), 0), 11: process(10, 0), 20: process(1, 0)}}
        after = copy.deepcopy(before)
        after.update(time=10, cpu=[15010, 0, 0, 990, 0, 0, 0, 0])
        after['processes'][10]['ticks'] = 100
        after['processes'][11]['ticks'] = 14900
        after['processes'][20]['ticks'] = 10
        with patch('benchmark_quiet.os.sysconf', return_value=100):
            self.assertTrue(quiet.assess(before, after, root=10)['pass'])
            # A new independent workload must reject a window even while its benchmark is busy.
            after['processes'][30] = process(1, 600, start=500)
            self.assertFalse(quiet.assess(before, after, root=10)['pass'])
            del after['processes'][30]
            after['swap'] = 1000
            self.assertFalse(quiet.assess(before, after, root=10)['pass'])
            # Cache preparation may cause its own paging; timed runs may not.
            self.assertTrue(quiet.assess(before, after, root=10, preparing=True)['pass'])
            after['swap'] = 1
            self.assertTrue(quiet.assess(before, after, root=10)['pass'])
            # Waited-child CPU rolls into the parent when a short benchmark exits.
            after['processes'][10]['children_ticks'] = 14900
            del after['processes'][11]
            self.assertTrue(quiet.assess(before, after, root=10)['pass'])
            # Global CPU catches an unrelated burst which has already exited.
            after['cpu'][0] += 1000
            self.assertFalse(quiet.assess(before, after, root=10)['pass'])

    def test_sandbox_process_view_fails_closed(self):
        with patch('benchmark_quiet.sys.platform', 'linux'), \
                patch.object(Path, 'read_text', return_value='codex\n'):
            with self.assertRaisesRegex(RuntimeError, 'incomplete host process view'):
                quiet.snapshot()

    def test_ram_swap_requires_verified_absence_of_disk_backing(self):
        with patch.object(Path, 'read_text', return_value='none\n'):
            self.assertTrue(quiet.ram_swap_devices(['/dev/zram0']))
            self.assertFalse(quiet.ram_swap_devices(['/swapfile']))
        with patch.object(Path, 'read_text', return_value='/dev/nvme0n1\n'):
            self.assertFalse(quiet.ram_swap_devices(['/dev/zram0']))
        with patch.object(Path, 'read_text', side_effect=PermissionError):
            self.assertFalse(quiet.ram_swap_devices(['/dev/zram0']))

    def test_io_window_requires_same_owned_benchmark_process(self):
        before = {'processes': {101: {'parent': os.getpid(), 'start': 100,
                                      'name': 'split-go', 'state': 'R'}}}
        after = copy.deepcopy(before)
        self.assertTrue(quiet.same_timed_process(before, after, {'split-go'}))
        after['processes'][101]['start'] = 200
        self.assertFalse(quiet.same_timed_process(before, after, {'split-go'}))
        after = copy.deepcopy(before)
        after['processes'][101]['parent'] = 1
        self.assertFalse(quiet.same_timed_process(before, after, {'split-go'}))
        after = {'processes': {102: before['processes'][101]}}
        self.assertFalse(quiet.same_timed_process(before, after, {'split-go'}))

    def test_ram_swap_in_does_not_hide_pressure_or_swap_out(self):
        before = {'time': 0, 'cpu': [0] * 8, 'swap': 0, 'swap_in': 0, 'swap_out': 0,
                  'pressure': {'memory': {'full': 0}}, 'available_kib': 4 * 1024 * 1024,
                  'temperatures_c': {}, 'processes': {}, 'ram_swap_only': True}
        after = copy.deepcopy(before)
        after.update(time=10, cpu=[10, 0, 0, 15990, 0, 0, 0, 0], swap=1000, swap_in=1000)
        with patch('benchmark_quiet.os.sysconf', return_value=100):
            self.assertTrue(quiet.assess(before, after)['pass'])
            after['pressure'] = {'memory': {'full': 20000}}
            self.assertFalse(quiet.assess(before, after)['pass'])
            after['pressure'] = {}
            after['cpu'][0] = 1000
            self.assertFalse(quiet.assess(before, after)['pass'])
            after['cpu'][0] = 10
            after.update(swap=2000, swap_out=1000)
            self.assertFalse(quiet.assess(before, after)['pass'])
            after.update(swap=1000, swap_out=0, ram_swap_only=False)
            self.assertFalse(quiet.assess(before, after)['pass'])

    def test_zombie_leader_is_not_complete_until_waitable(self):
        child = unittest.mock.Mock(pid=4242)
        sample = {'processes': {4242: {'parent': 1, 'start': 100, 'state': 'Z'}}}
        with patch('benchmark_quiet.snapshot', return_value=sample), \
                patch('benchmark_quiet.reap', side_effect=[([], False), ([], True)]) as reap, \
                patch('benchmark_quiet.time.sleep'):
            quiet.finish_owned(child, {4242: 100})
        self.assertEqual(reap.call_count, 2)
        child.wait.assert_called_once()

    def test_reused_command_pid_is_not_claimed_as_owned(self):
        child = unittest.mock.Mock(pid=4242)
        sample = {'processes': {4242: {'parent': 1, 'start': 200},
                                4243: {'parent': 4242, 'start': 201}}}
        identities = {4242: 100}
        quiet.remember_owned(sample, child, identities)
        self.assertEqual(identities, {4242: 100})

    def test_mac_cpu_time_memory_and_thermal_limit(self):
        outputs = ['1 0 Mon Oct 5 12:00:00 2026 1:02.50 /sbin/launchd\n'
                   '22 1 Mon Oct 5 12:01:00 2026 00:03.25 /Applications/Work App\n',
                   'Mach Virtual Memory Statistics: (page size of 16384 bytes)\n'
                   'Pages free: 100000.\nPages inactive: 100000.\nSwapins: 2.\nSwapouts: 3.\n',
                   'CPU_Speed_Limit = 85\n']
        with patch('benchmark_quiet.subprocess.check_output', side_effect=outputs):
            sample = quiet.mac_snapshot()
        self.assertEqual(sample['processes'][22]['ticks'], 325)
        self.assertEqual(sample['swap'], 5)
        self.assertEqual(sample['available_kib'], 3200000)
        self.assertTrue(sample['thermal_limited'])


if __name__ == '__main__':
    unittest.main()
