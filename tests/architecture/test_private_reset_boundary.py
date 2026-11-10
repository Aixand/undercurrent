"""Local maintenance probes do not establish a public Unix transport."""

import json
from pathlib import Path
import runpy
import unittest


ROOT = Path(__file__).resolve().parents[2]
GATE = runpy.run_path(str(ROOT / "tests/architecture/architecture_gate.py"))
ALLOWED = json.loads((ROOT / "tests/architecture/allowed-graph.json").read_text())
RESET = "crates/undercurrent-cli/src/reset.rs"


class PrivateResetBoundaryTests(unittest.TestCase):
    def model(self, package, path, symbol):
        model = GATE["base_model"](ALLOWED)
        model["unix_uses"].append({"package": package, "path": path, "symbol": symbol})
        return model

    def test_owned_reset_probe_and_its_listener_fixture_remain_private(self):
        for symbol in ["std::os::unix::net::UnixStream", "std::os::unix::net::UnixListener"]:
            with self.subTest(symbol=symbol):
                GATE["validate_model"](self.model("undercurrent-cli", RESET, symbol), ALLOWED)

    def test_other_client_modules_packages_and_socket_implementations_remain_forbidden(self):
        for package, path, symbol in [
            ("undercurrent-cli", "crates/undercurrent-cli/src/native.rs", "std::os::unix::net::UnixStream"),
            ("undercurrent-cli", "crates/undercurrent-cli/src/reset/transport.rs", "std::os::unix::net::UnixStream"),
            ("aixand-undercurrent", RESET, "std::os::unix::net::UnixStream"),
            ("undercurrent-cli", RESET, "tokio::net::UnixStream"),
            ("undercurrent-cli", RESET, "std::os::unix::net::UnixDatagram"),
        ]:
            with self.subTest(package=package, path=path, symbol=symbol):
                with self.assertRaises(GATE["Violation"]) as error:
                    GATE["validate_model"](self.model(package, path, symbol), ALLOWED)
                self.assertEqual(error.exception.rule, "ARCH_PUBLIC_UNIX_TRANSPORT")


if __name__ == "__main__":
    unittest.main()
