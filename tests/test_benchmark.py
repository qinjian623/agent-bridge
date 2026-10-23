import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class BenchmarkTests(unittest.TestCase):
    def test_fixed_assertions_reject_broken_fixture_and_accept_reference(self):
        module = runpy.run_path(str(Path(__file__).parents[1] / "scripts/benchmark.py"))
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            module["fixture"](source)
            for case in module["CASES"].values():
                result = subprocess.run([sys.executable, "-B", "-c", case["test"]], cwd=source, capture_output=True)
                self.assertNotEqual(0, result.returncode)
            (source / "calc.py").write_text("""def normalize_label(text):
    return "-".join(text.lower().split())

def merge_intervals(intervals):
    result = []
    for start, end in sorted(intervals):
        if start > end:
            raise ValueError("reversed")
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result

def chunk_items(items, size):
    if type(size) is not int:
        raise TypeError("size")
    if size <= 0:
        raise ValueError("size")
    result, chunk = [], []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            result.append(chunk)
            chunk = []
    return result + ([chunk] if chunk else [])
""")
            for case in module["CASES"].values():
                result = subprocess.run([sys.executable, "-B", "-c", case["test"]], cwd=source, capture_output=True)
                self.assertEqual(0, result.returncode, result.stderr.decode())
