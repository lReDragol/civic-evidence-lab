import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from runtime.task_scheduler import (
    build_schtasks_create_command,
    build_schtasks_query_command,
    build_wrapper_contents,
    daemon_log_path,
    install_task,
    write_wrapper,
)


class TaskSchedulerTests(unittest.TestCase):
    def test_build_wrapper_contents_uses_repo_root_python_and_no_preflight(self):
        repo_root = Path(r"F:\новости")
        python_exe = Path(r"C:\Python313\python.exe")

        contents = build_wrapper_contents(repo_root=repo_root, python_exe=python_exe, no_preflight=True)

        self.assertIn(f'cd /d "{repo_root}"', contents)
        self.assertIn(f'"{python_exe}" -m runtime.daemon --no-preflight', contents)
        self.assertIn(str(daemon_log_path(repo_root)), contents)

    def test_write_wrapper_creates_generated_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)

            wrapper_path = write_wrapper(repo_root=repo_root, python_exe=Path(sys.executable), no_preflight=False)

            self.assertTrue(wrapper_path.exists())
            text = wrapper_path.read_text(encoding="utf-8")
            self.assertIn("-m runtime.daemon", text)
            self.assertNotIn("--no-preflight", text)

    def test_create_command_uses_expected_trigger_and_wrapper(self):
        wrapper_path = Path(r"F:\новости\runtime\generated\daemon_task_wrapper.cmd")

        command = build_schtasks_create_command(
            task_name=r"CivicEvidenceLab\RuntimeDaemon",
            wrapper_path=wrapper_path,
            schedule="onlogon",
            user="Drago",
            force=True,
        )

        self.assertEqual(command[:4], ["schtasks", "/Create", "/TN", r"CivicEvidenceLab\RuntimeDaemon"])
        self.assertIn("/SC", command)
        self.assertIn("ONLOGON", command)
        self.assertIn("/RU", command)
        self.assertIn("Drago", command)
        self.assertIn('/TR', command)
        self.assertTrue(any(str(wrapper_path) in part for part in command))
        self.assertIn("/F", command)

    def test_query_command_is_verbose_list(self):
        self.assertEqual(
            build_schtasks_query_command(r"CivicEvidenceLab\RuntimeDaemon"),
            ["schtasks", "/Query", "/TN", r"CivicEvidenceLab\RuntimeDaemon", "/V", "/FO", "LIST"],
        )

    def test_install_task_falls_back_to_startup_folder_on_access_denied(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            repo_root.mkdir()
            appdata = Path(tmp) / "appdata"
            appdata.mkdir()

            completed = type(
                "Completed",
                (),
                {"returncode": 1, "stdout": "", "stderr": "ERROR: Access is denied."},
            )()

            with patch.dict("os.environ", {"APPDATA": str(appdata)}):
                with patch("runtime.task_scheduler._run", return_value=completed):
                    result = install_task(
                        task_name="CivicEvidenceLab Runtime Daemon",
                        repo_root=repo_root,
                        python_exe=Path(sys.executable),
                        schedule="onlogon",
                    )

            self.assertTrue(result["ok"])
            self.assertEqual(result["install_mode"], "startup_folder")
            self.assertTrue(Path(result["startup_launcher"]).exists())


if __name__ == "__main__":
    unittest.main()
