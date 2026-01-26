import pytest

from feast.permissions.auth_model import NoAuthConfig
from feast.permissions.client.http_auth_requests_wrapper import (
    HttpSessionManager,
    get_http_auth_requests_session,
)


class TestHttpSessionManager:
    """Test suite for HTTP session manager with connection pooling."""

    @pytest.fixture(autouse=True)
    def reset_session(self):
        """Reset the session before and after each test."""
        HttpSessionManager.close_session()
        yield
        HttpSessionManager.close_session()

    def test_session_creation(self):
        """Test that session is created with correct configuration."""
        auth_config = NoAuthConfig()
        session = get_http_auth_requests_session(auth_config)

        # Session should be created
        assert session is not None

        # Session should have HTTP and HTTPS adapters
        assert "http://" in session.adapters
        assert "https://" in session.adapters

        # Session should have keep-alive header
        assert session.headers.get("Connection") == "keep-alive"

    def test_session_reuse(self):
        """Test that session is reused across multiple calls."""
        auth_config = NoAuthConfig()

        # First call creates session
        session1 = get_http_auth_requests_session(auth_config)

        # Second call should return the same session
        session2 = get_http_auth_requests_session(auth_config)

        # Should be the exact same object
        assert session1 is session2

    def test_session_close(self):
        """Test that session can be closed and recreated."""
        auth_config = NoAuthConfig()

        # Create session
        session1 = get_http_auth_requests_session(auth_config)
        assert HttpSessionManager._session is not None

        # Close session
        HttpSessionManager.close_session()
        assert HttpSessionManager._session is None
        assert HttpSessionManager._session_auth_type is None

        # New session should be created
        session2 = get_http_auth_requests_session(auth_config)
        assert session2 is not None
        assert session2 is not session1

    def test_thread_safety(self):
        """Test that session management is thread-safe."""
        import threading

        auth_config = NoAuthConfig()
        sessions = []
        errors = []

        def get_session():
            try:
                session = get_http_auth_requests_session(auth_config)
                sessions.append(session)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=get_session) for _ in range(10)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # No errors should have occurred
        assert len(errors) == 0

        # All threads should have gotten the same session (after initial creation)
        assert len(sessions) == 10
        # All sessions should be the same object (due to caching)
        assert all(s is sessions[0] for s in sessions)

    def test_connection_pool_configuration(self):
        """Test that connection pool default configuration is correct."""
        assert HttpSessionManager.DEFAULT_POOL_CONNECTIONS == 10
        assert HttpSessionManager.DEFAULT_POOL_MAXSIZE == 50
        assert HttpSessionManager.DEFAULT_MAX_RETRIES == 3
        assert HttpSessionManager.DEFAULT_BACKOFF_FACTOR == 0.5
        assert HttpSessionManager.DEFAULT_MAX_IDLE_SECONDS == 300  # 5 minutes

    def test_custom_configuration(self):
        """Test that custom configuration is applied."""
        auth_config = NoAuthConfig()

        # Create session with custom config
        _session = get_http_auth_requests_session(  # noqa: F841
            auth_config,
            pool_maxsize=100,
            max_idle_seconds=600,
            max_retries=5,
        )

        # Verify custom config was applied
        assert HttpSessionManager._pool_maxsize == 100
        assert HttpSessionManager._max_idle_seconds == 600
        assert HttpSessionManager._max_retries == 5

    def test_disable_idle_timeout(self):
        """Test that idle timeout can be disabled by setting to 0."""
        auth_config = NoAuthConfig()

        # Create session with idle timeout disabled
        _session = get_http_auth_requests_session(  # noqa: F841
            auth_config,
            max_idle_seconds=0,
        )

        assert HttpSessionManager._max_idle_seconds == 0

    def test_idle_timeout(self):
        """Test that session is closed after idle timeout."""
        import time

        auth_config = NoAuthConfig()

        # Create session with very short timeout for testing
        session1 = get_http_auth_requests_session(
            auth_config,
            max_idle_seconds=1,  # 1 second
        )

        # Wait longer than timeout
        time.sleep(1.5)

        # Get session again - should create new one due to idle timeout
        session2 = get_http_auth_requests_session(
            auth_config,
            max_idle_seconds=1,
        )

        # Should be different sessions
        assert session1 is not session2

    def test_session_last_used_tracking(self):
        """Test that last used time is tracked correctly."""
        import time

        auth_config = NoAuthConfig()

        # Create session
        _session = get_http_auth_requests_session(auth_config)  # noqa: F841

        # Last used should be set
        assert HttpSessionManager._session_last_used is not None

        first_used = HttpSessionManager._session_last_used

        # Small delay
        time.sleep(0.1)

        # Get session again
        _session2 = get_http_auth_requests_session(auth_config)  # noqa: F841

        # Last used should be updated
        assert HttpSessionManager._session_last_used >= first_used
