"""
ABOUTME: Tests for Lambda handler validation logic
ABOUTME: Tests job spec validation, URL validation, and schema versioning
"""

import unittest
import sys
import os

# Add parent directory to path to import handler
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from handler import validate_job_spec, validate_postgresql_url, CURRENT_SCHEMA_VERSION


class TestJobSpecValidation(unittest.TestCase):
    """Test job specification validation"""

    def test_valid_job_spec(self):
        """Valid job spec should pass validation"""
        spec = {
            'schema_version': '1.0',
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
            'options': {
                'drop_existing': True,
                'enable_sync': False,
                'estimated_size_bytes': 1000000
            },
            'filter': {}
        }

        is_valid, error = validate_job_spec(spec)
        self.assertTrue(is_valid, f"Expected valid spec to pass, got error: {error}")
        self.assertIsNone(error)

    def test_missing_schema_version(self):
        """Missing schema_version should fail validation"""
        spec = {
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('schema_version', error.lower())

    def test_unsupported_schema_version(self):
        """Unsupported schema_version should fail validation"""
        spec = {
            'schema_version': '99.0',
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('unsupported', error.lower())
        self.assertIn('99.0', error)

    def test_missing_required_fields(self):
        """Missing required fields should fail validation"""
        base_spec = {
            'schema_version': '1.0',
        }

        required_fields = ['command', 'source_url', 'target_url']
        for field in required_fields:
            spec = base_spec.copy()
            # Add all fields except the one we're testing
            for f in required_fields:
                if f != field:
                    spec[f] = 'test_value'

            is_valid, error = validate_job_spec(spec)
            self.assertFalse(is_valid, f"Should fail when {field} is missing")
            self.assertIn(field, error.lower())

    def test_empty_field_values(self):
        """Empty string values should fail validation"""
        spec = {
            'schema_version': '1.0',
            'command': '',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('empty', error.lower())

    def test_invalid_command(self):
        """Invalid command should fail validation"""
        spec = {
            'schema_version': '1.0',
            'command': 'delete_all_data',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('invalid command', error.lower())

    def test_spec_too_large(self):
        """Job spec exceeding size limit should fail validation"""
        # Create a spec with very large data
        huge_string = 'x' * (16 * 1024)  # 16KB string
        spec = {
            'schema_version': '1.0',
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/' + huge_string,
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('too large', error.lower())

    def test_invalid_options_type(self):
        """Invalid option types should fail validation"""
        spec = {
            'schema_version': '1.0',
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
            'options': {
                'drop_existing': 'yes'  # Should be boolean
            }
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('boolean', error.lower())

    def test_unknown_option(self):
        """Unknown option keys should fail validation"""
        spec = {
            'schema_version': '1.0',
            'command': 'init',
            'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
            'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
            'options': {
                'unknown_option': True
            }
        }

        is_valid, error = validate_job_spec(spec)
        self.assertFalse(is_valid)
        self.assertIn('unknown option', error.lower())


class TestPostgreSQLURLValidation(unittest.TestCase):
    """Test PostgreSQL URL validation"""

    def test_valid_url(self):
        """Valid PostgreSQL URL should pass validation"""
        urls = [
            'postgresql://user:pass@localhost:5432/mydb',
            'postgres://user:pass@example.com:5432/db',
            'postgresql://user@host/database',
            'postgresql://host:5432/db',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertTrue(is_valid, f"Expected {url} to be valid, got error: {error}")
            self.assertIsNone(error)

    def test_invalid_scheme(self):
        """Invalid scheme should fail validation"""
        urls = [
            'http://localhost:5432/db',
            'mysql://localhost:3306/db',
            'mongodb://localhost:27017/db',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertFalse(is_valid, f"Expected {url} to fail validation")
            self.assertIn('scheme', error.lower())

    def test_missing_hostname(self):
        """Missing hostname should fail validation"""
        url = 'postgresql:///database'

        is_valid, error = validate_postgresql_url(url)
        self.assertFalse(is_valid)
        self.assertIn('hostname', error.lower())

    def test_invalid_port(self):
        """Invalid port should fail validation"""
        urls = [
            'postgresql://localhost:0/db',
            'postgresql://localhost:99999/db',
            'postgresql://localhost:-1/db',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertFalse(is_valid, f"Expected {url} to fail validation")

    def test_command_injection_attempts(self):
        """URLs with command injection attempts should fail validation"""
        urls = [
            'postgresql://host/db; DROP TABLE users;',
            'postgresql://host/db$(malicious)',
            'postgresql://host/db`command`',
            'postgresql://host/db && echo "hacked"',
            'postgresql://host/db || echo "hacked"',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertFalse(is_valid, f"Expected {url} to fail validation")
            self.assertIn('dangerous', error.lower())

    def test_invalid_hostname_format(self):
        """Invalid hostname format should fail validation"""
        urls = [
            'postgresql://host@with@signs:5432/db',
            'postgresql://-invalid:5432/db',
            'postgresql://invalid-:5432/db',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertFalse(is_valid, f"Expected {url} to fail validation")

    def test_invalid_database_name(self):
        """Invalid database name format should fail validation"""
        urls = [
            'postgresql://host:5432/db with spaces',
            'postgresql://host:5432/db@special',
            'postgresql://host:5432/db$invalid',
        ]

        for url in urls:
            is_valid, error = validate_postgresql_url(url)
            self.assertFalse(is_valid, f"Expected {url} to fail validation")


class TestSchemaVersioning(unittest.TestCase):
    """Test schema versioning"""

    def test_current_schema_version_supported(self):
        """Current schema version should be in supported versions"""
        from handler import SUPPORTED_SCHEMA_VERSIONS
        self.assertIn(CURRENT_SCHEMA_VERSION, SUPPORTED_SCHEMA_VERSIONS)

    def test_backward_compatibility(self):
        """All supported versions should be validated"""
        from handler import SUPPORTED_SCHEMA_VERSIONS

        for version in SUPPORTED_SCHEMA_VERSIONS:
            spec = {
                'schema_version': version,
                'command': 'init',
                'source_url': 'postgresql://user:pass@localhost:5432/sourcedb',
                'target_url': 'postgresql://user:pass@localhost:5433/targetdb',
            }

            is_valid, error = validate_job_spec(spec)
            self.assertTrue(is_valid, f"Version {version} should be supported, got error: {error}")


if __name__ == '__main__':
    unittest.main()
