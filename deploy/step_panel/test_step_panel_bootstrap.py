import hashlib
import io
import pathlib
import shlex
import sys
import tempfile
import types
import unittest
from unittest import mock

from make_start_command import bootstrap_code, start_command


class OriginalStartup(Exception):
    pass


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.payload = b"from step_panel_test_sink import run\nrun()\n"
        self.sha = hashlib.sha256(self.payload).hexdigest()
        self.url = "https://raw.githubusercontent.com/burakatli05-ai/binance-momentum-bot/" + "a" * 40 + "/deploy/step_panel/step_panel_install.py"
        self.cache = self.root / ("step_panel_installer_" + self.sha + ".py")
        self.sink = mock.Mock()

    def execute(self, response=None, fetch_error=None, cache_error=False):
        modules = {
            "pathlib": types.SimpleNamespace(Path=lambda value: self.root / pathlib.Path(value).name),
            "step_panel_test_sink": types.SimpleNamespace(run=self.sink),
        }
        with mock.patch.dict(sys.modules, modules), mock.patch("urllib.request.urlopen", return_value=io.BytesIO(self.payload if response is None else response), side_effect=fetch_error) as fetch, mock.patch("os.execv", side_effect=OriginalStartup) as original:
            with mock.patch("tempfile.NamedTemporaryFile", side_effect=OSError("cache unavailable")) if cache_error else mock.patch("builtins.print"):
                try:
                    exec(bootstrap_code(self.url, self.sha), {})
                except OriginalStartup:
                    pass
        return fetch, original

    def test_verified_download_is_cached_and_executed(self):
        fetch, original = self.execute()
        self.assertEqual(self.cache.read_bytes(), self.payload)
        self.sink.assert_called_once_with()
        original.assert_not_called()
        fetch.assert_called_once()

    def test_verified_cache_works_without_network(self):
        self.cache.write_bytes(self.payload)
        fetch, original = self.execute(fetch_error=TimeoutError())
        fetch.assert_not_called()
        self.sink.assert_called_once_with()
        original.assert_not_called()

    def test_bad_hash_never_executes_download(self):
        _, original = self.execute(response=b"raise RuntimeError('untrusted')")
        self.sink.assert_not_called()
        original.assert_called_once_with(sys.executable, [sys.executable, "startup.py"])
        self.assertFalse(self.cache.exists())

    def test_network_failure_starts_original_image(self):
        _, original = self.execute(fetch_error=TimeoutError())
        self.sink.assert_not_called()
        original.assert_called_once_with(sys.executable, [sys.executable, "startup.py"])

    def test_cache_write_failure_keeps_verified_memory_payload(self):
        _, original = self.execute(cache_error=True)
        self.sink.assert_called_once_with()
        original.assert_not_called()

    def test_command_shell_roundtrip_and_immutable_url(self):
        self.assertEqual(shlex.split(start_command(self.url, self.sha)), ["python", "-c", bootstrap_code(self.url, self.sha)])
        with self.assertRaises(ValueError):
            start_command(self.url.replace("a" * 40, "main"), self.sha)


if __name__ == "__main__":
    unittest.main()
