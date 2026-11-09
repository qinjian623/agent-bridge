import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from agent_bridge.config import Profile
from agent_bridge.discovery import ModelDirectory, NoRedirect, native_models, ollama_models
from agent_bridge.errors import BridgeError
from agent_bridge.runtimes import claude_environment


class OllamaEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.key_path = self.home / ".config/claude-ollama/api-key"
        self.key_path.parent.mkdir(parents=True)
        self.key_path.write_text("test-ollama-credential\n")
        self.key_path.chmod(0o600)
        self.profile = Profile("claude-ollama", "claude", "deepseek-v4.1-flash", provider="ollama-anthropic")
        self.home_patch = patch("agent_bridge.runtimes.Path.home", return_value=self.home)
        self.home_patch.start()
        self.addCleanup(self.home_patch.stop)
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_private_native_key_is_read_without_copying_it_and_official_credentials_are_replaced(self):
        with patch.dict(os.environ, {"ANTHROPIC_AUTH_TOKEN": "official-token", "ANTHROPIC_API_KEY": "official-key"}):
            env = claude_environment(self.profile)
        self.assertEqual("https://ollama.com", env["ANTHROPIC_BASE_URL"])
        self.assertEqual("test-ollama-credential", env["OLLAMA_API_KEY"])
        self.assertEqual(env["OLLAMA_API_KEY"], env["ANTHROPIC_AUTH_TOKEN"])
        self.assertEqual("", env["ANTHROPIC_API_KEY"])
        self.assertEqual("http://127.0.0.1:1080", env["HTTPS_PROXY"])
        self.assertEqual(env["HTTPS_PROXY"], env["HTTP_PROXY"])
        self.assertEqual("child", env["AGENT_BRIDGE_ROLE"])
        self.assertNotIn("test-ollama-credential", json.dumps(self.profile.public()))
        self.assertEqual([self.key_path], list(self.key_path.parent.iterdir()))

    def test_environment_key_takes_precedence_without_opening_file(self):
        with patch.dict(os.environ, {"OLLAMA_API_KEY": "environment-credential"}), patch("agent_bridge.runtimes.os.open") as opened:
            env = claude_environment(self.profile)
        opened.assert_not_called()
        self.assertEqual("environment-credential", env["ANTHROPIC_AUTH_TOKEN"])

    def test_wrong_endpoint_is_rejected_before_reading_any_credential(self):
        for base in ("http://ollama.com", "https://other.invalid", "https://ollama.com.evil.invalid", "https://user:pass@ollama.com", "https://ollama.com:8443"):
            with self.subTest(base=base), patch.dict(os.environ, {"ANTHROPIC_BASE_URL": base}), patch("agent_bridge.runtimes._ollama_credential") as credential:
                with self.assertRaisesRegex(BridgeError, "HTTPS ollama.com"):
                    claude_environment(replace(self.profile, endpoint_host="other.invalid"))
                credential.assert_not_called()

    def test_endpoint_host_cannot_relabel_another_destination(self):
        with patch("agent_bridge.runtimes._ollama_credential") as credential, self.assertRaises(BridgeError):
            claude_environment(replace(self.profile, endpoint_host="other.invalid"))
        credential.assert_not_called()

    def test_inherited_official_endpoint_is_not_silently_reused(self):
        with patch.dict(os.environ, {"ANTHROPIC_BASE_URL": "https://api.deepseek.com/anthropic"}), patch("agent_bridge.runtimes._ollama_credential") as credential:
            with self.assertRaises(BridgeError):
                claude_environment(self.profile)
        credential.assert_not_called()

    def test_proxy_defaults_match_wrapper_and_explicit_proxies_are_preserved(self):
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://upper:8080", "HTTP_PROXY": "http://http:8081", "https_proxy": "http://lower:9999"}):
            env = claude_environment(self.profile)
        self.assertEqual("http://upper:8080", env["HTTPS_PROXY"])
        self.assertEqual("http://http:8081", env["HTTP_PROXY"])

    def test_missing_or_symlinked_key_is_rejected_without_secret_in_error(self):
        self.key_path.unlink()
        with self.assertRaisesRegex(BridgeError, "existing private key"):
            claude_environment(self.profile)
        target = self.home / "target"
        target.write_text("secret-not-in-errors")
        target.chmod(0o600)
        self.key_path.symlink_to(target)
        with self.assertRaises(BridgeError) as error:
            claude_environment(self.profile)
        self.assertNotIn("secret-not-in-errors", str(error.exception))

    def test_non_private_or_foreign_owned_key_is_rejected(self):
        self.key_path.chmod(0o644)
        with self.assertRaisesRegex(BridgeError, "0600"):
            claude_environment(self.profile)
        self.key_path.chmod(0o600)
        with patch("agent_bridge.runtimes.os.getuid", return_value=os.getuid() + 1), self.assertRaises(BridgeError):
            claude_environment(self.profile)

    def test_nonregular_key_is_rejected(self):
        self.key_path.unlink()
        self.key_path.mkdir(mode=0o700)
        with self.assertRaisesRegex(BridgeError, "regular"):
            claude_environment(self.profile)

    def test_fifo_key_is_rejected_without_waiting_for_a_writer(self):
        self.key_path.unlink()
        os.mkfifo(self.key_path, mode=0o600)
        with self.assertRaisesRegex(BridgeError, "regular"):
            claude_environment(self.profile)

    def test_empty_multiline_oversized_and_invalid_utf8_keys_are_rejected(self):
        for data in (b"", b"key\nsecond\n", b"\n", b" key\n", b"k" * 8193, b"\xff"):
            with self.subTest(data_length=len(data)):
                self.key_path.write_bytes(data)
                with self.assertRaises(BridgeError):
                    claude_environment(self.profile)

    def test_malformed_environment_credentials_do_not_fall_back_to_the_file(self):
        for key in ("a\nb", " ", "x" * 8193, "a\x7fb"):
            with self.subTest(key_length=len(key)), patch.dict(os.environ, {"OLLAMA_API_KEY": key}), self.assertRaises(BridgeError):
                claude_environment(self.profile)

    def test_official_claude_behavior_and_credentials_are_unchanged(self):
        for provider, endpoint in (("anthropic", "https://api.anthropic.com"), ("deepseek-anthropic", "https://api.deepseek.com/anthropic")):
            with self.subTest(provider=provider), patch.dict(os.environ, {"ANTHROPIC_BASE_URL": endpoint, "ANTHROPIC_AUTH_TOKEN": "official-token"}), patch("agent_bridge.runtimes._ollama_credential") as credential:
                env = claude_environment(replace(self.profile, provider=provider))
                self.assertEqual("official-token", env["ANTHROPIC_AUTH_TOKEN"])
                self.assertEqual(endpoint, env["ANTHROPIC_BASE_URL"])
                credential.assert_not_called()


class OllamaDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def opener(self, body):
        response = Mock()
        response.read.return_value = body
        opener = Mock()
        opener.open.return_value.__enter__ = Mock(return_value=response)
        opener.open.return_value.__exit__ = Mock(return_value=False)
        return opener, response

    def test_exact_names_and_model_fallback_are_not_normalized_or_aliased(self):
        opener, response = self.opener(json.dumps({"models": [{"name": "deepseek-v4.1-flash"}, {"model": "another:cloud"}]}).encode())
        with patch("agent_bridge.discovery.build_opener", return_value=opener):
            rows = ollama_models({"OLLAMA_API_KEY": "test-credential"})
        self.assertEqual(["deepseek-v4.1-flash", "another:cloud"], [r["model"] for r in rows])
        self.assertEqual("https://ollama.com/api/tags", opener.open.call_args.args[0].full_url)
        self.assertEqual("Bearer test-credential", opener.open.call_args.args[0].get_header("Authorization"))
        self.assertEqual(10, opener.open.call_args.kwargs["timeout"])
        response.read.assert_called_once_with(2 * 1024 * 1024 + 1)
        self.assertNotIn("test-credential", json.dumps(rows))

    def test_only_ollama_credentials_are_accepted(self):
        with patch("agent_bridge.discovery.build_opener") as opened, self.assertRaisesRegex(BridgeError, "own API credential"):
            ollama_models({"ANTHROPIC_AUTH_TOKEN": "official-key"})
        opened.assert_not_called()

    def test_uppercase_proxy_values_take_precedence(self):
        opener, _ = self.opener(b'{"models": []}')
        with patch("agent_bridge.discovery.build_opener", return_value=opener), patch("agent_bridge.discovery.ProxyHandler") as proxy:
            ollama_models({"OLLAMA_API_KEY": "key", "HTTPS_PROXY": "http://upper", "https_proxy": "http://lower"})
        proxy.assert_called_once_with({"https": "http://upper"})

    def test_redirect_http_errors_never_include_credentials_or_response_text(self):
        opener, _ = self.opener(b"")
        for status in (302, 401, 500):
            opener.open.side_effect = HTTPError("https://ollama.com/api/tags", status, "secret-credential", {}, None)
            with self.subTest(status=status), patch("agent_bridge.discovery.build_opener", return_value=opener), self.assertRaisesRegex(BridgeError, f"HTTP {status}") as error:
                ollama_models({"OLLAMA_API_KEY": "secret-credential"})
            self.assertNotIn("secret-credential", str(error.exception))
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.invalid"))

    def test_invalid_and_oversized_schema_fails_instead_of_fabricating_models(self):
        for body in (b"x" * (2 * 1024 * 1024 + 1), b"not json", b"[]", b'{"data": []}', b'{"models": [null]}', b'{"models": [{}]}'):
            opener, _ = self.opener(body)
            with self.subTest(body_length=len(body)), patch("agent_bridge.discovery.build_opener", return_value=opener), self.assertRaises(BridgeError):
                ollama_models({"OLLAMA_API_KEY": "key"})

    async def test_native_discovery_uses_ollama_and_does_not_publish_credentials(self):
        profile = Profile("claude-ollama", "claude", "deepseek-v4.1-flash", provider="ollama-anthropic")
        opener, _ = self.opener(b'{"models": [{"name": "deepseek-v4.1-flash"}]}')
        with patch.dict(os.environ, {"OLLAMA_API_KEY": "private-ollama-key", "ANTHROPIC_BASE_URL": "https://ollama.com"}), patch("agent_bridge.discovery.build_opener", return_value=opener):
            rows, source = await native_models(profile, Path("/tmp"))
            result = await ModelDirectory(Path("/tmp")).get(profile, refresh=True)
        self.assertEqual("Ollama GET /api/tags", source)
        self.assertEqual(["deepseek-v4.1-flash"], [r["model"] for r in rows])
        self.assertFalse(result["models"][0]["verified"])
        self.assertNotIn("private-ollama-key", json.dumps(result))
