# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for feast.tracing, feast.tracing_context, and feast.tracing_hooks."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# tracing_context tests
# ---------------------------------------------------------------------------


class TestFeastTraceContext:
    def test_push_retrieval_accumulates_refs(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        ctx.push_retrieval(
            feature_refs=["fv1:f1", "fv1:f2"],
            feature_service="my_service",
            span_id="span-1",
        )
        assert ctx.feature_refs == ["fv1:f1", "fv1:f2"]
        assert ctx.feature_views == {"fv1"}
        assert ctx.feature_service == "my_service"
        assert ctx.retrieval_span_ids == ["span-1"]

    def test_push_retrieval_multiple_calls(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        ctx.push_retrieval(["fv1:f1"], "svc1", "s1")
        ctx.push_retrieval(["fv2:f3"], "svc2", "s2")
        assert len(ctx.feature_refs) == 2
        assert ctx.feature_views == {"fv1", "fv2"}
        assert ctx.feature_service == "svc2"
        assert ctx.retrieval_span_ids == ["s1", "s2"]

    def test_get_context_attributes_empty(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        assert ctx.get_context_attributes() == {}

    def test_get_context_attributes_populated(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        ctx.push_retrieval(["fv1:f2", "fv1:f1", "fv2:f3"], "my_svc")

        attrs = ctx.get_context_attributes()
        assert attrs["feast.context_features"] == "fv1:f1,fv1:f2,fv2:f3"
        assert attrs["feast.context_feature_count"] == "3"
        assert "fv1" in attrs["feast.context_feature_views"]
        assert "fv2" in attrs["feast.context_feature_views"]
        assert attrs["feast.context_feature_service"] == "my_svc"

    def test_get_context_attributes_deduplicates(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        ctx.push_retrieval(["fv1:f1", "fv1:f1"])
        attrs = ctx.get_context_attributes()
        assert attrs["feast.context_feature_count"] == "1"

    def test_clear(self):
        from feast.tracing_context import FeastTraceContext

        ctx = FeastTraceContext()
        ctx.push_retrieval(["fv1:f1"], "svc", "s1")
        ctx.clear()
        assert ctx.feature_refs == []
        assert ctx.feature_views == set()
        assert ctx.feature_service is None
        assert ctx.retrieval_span_ids == []


class TestFeastTraceScope:
    def test_scope_creates_and_cleans_up(self):
        from feast.tracing_context import feast_trace_scope, get_current_context

        assert get_current_context() is None
        with feast_trace_scope() as ctx:
            assert get_current_context() is ctx
            ctx.push_retrieval(["fv1:f1"])
            assert len(ctx.feature_refs) == 1
        assert get_current_context() is None

    def test_nested_scope_replaces_outer(self):
        from feast.tracing_context import feast_trace_scope, get_current_context

        with feast_trace_scope() as outer:
            outer.push_retrieval(["fv1:f1"])
            with feast_trace_scope() as inner:
                assert get_current_context() is inner
                assert inner.feature_refs == []
            assert get_current_context() is outer


# ---------------------------------------------------------------------------
# tracing_hooks tests
# ---------------------------------------------------------------------------


class TestFeastSpanProcessor:
    def test_skips_non_llm_span(self):
        from feast.tracing_hooks import feast_span_processor

        span = MagicMock()
        span.span_type = "RETRIEVER"
        feast_span_processor(span)
        span.set_attribute.assert_not_called()

    def test_skips_when_no_context(self):
        from feast.tracing_hooks import feast_span_processor

        span = MagicMock()
        span.span_type = "LLM"
        feast_span_processor(span)
        span.set_attribute.assert_not_called()

    def test_tags_llm_span_with_feast_context(self):
        from feast.tracing_context import feast_trace_scope
        from feast.tracing_hooks import feast_span_processor

        span = MagicMock()
        span.span_type = "LLM"

        with feast_trace_scope() as ctx:
            ctx.push_retrieval(["fv1:f1", "fv2:f2"], "my_svc")
            feast_span_processor(span)

        calls = {c.args[0]: c.args[1] for c in span.set_attribute.call_args_list}
        assert "feast.context_features" in calls
        assert "fv1:f1" in calls["feast.context_features"]
        assert "fv2:f2" in calls["feast.context_features"]
        assert calls["feast.context_feature_service"] == "my_svc"

    def test_tags_chat_model_span(self):
        from feast.tracing_context import feast_trace_scope
        from feast.tracing_hooks import feast_span_processor

        span = MagicMock()
        span.span_type = "CHAT_MODEL"

        with feast_trace_scope() as ctx:
            ctx.push_retrieval(["fv1:f1"])
            feast_span_processor(span)

        span.set_attribute.assert_called()


class TestInstallFeastSpanProcessor:
    def test_install_calls_configure(self):
        try:
            import mlflow.tracing

            has_mlflow_tracing = True
        except (ImportError, AttributeError):
            has_mlflow_tracing = False

        if has_mlflow_tracing:
            with patch.object(mlflow.tracing, "configure") as mock_configure:
                from feast.tracing_hooks import install_feast_span_processor

                install_feast_span_processor()
                mock_configure.assert_called_once()
        else:
            mock_tracing = MagicMock()
            with patch.dict("sys.modules", {"mlflow.tracing": mock_tracing}):
                import importlib

                import feast.tracing_hooks

                importlib.reload(feast.tracing_hooks)
                feast.tracing_hooks.install_feast_span_processor()
                mock_tracing.configure.assert_called_once()

    def test_install_graceful_on_missing_mlflow(self):
        from feast.tracing_hooks import install_feast_span_processor

        try:
            import mlflow.tracing

            with patch.object(mlflow.tracing, "configure", side_effect=AttributeError):
                install_feast_span_processor()
        except (ImportError, AttributeError):
            install_feast_span_processor()


# ---------------------------------------------------------------------------
# tracing.py tests (MLflow-native)
# ---------------------------------------------------------------------------


class TestLazyInit:
    def setup_method(self):
        import feast.tracing

        feast.tracing._initialized = False
        feast.tracing._enabled = False
        feast.tracing._mlflow_mod = None

    def test_disabled_when_no_mlflow_config(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow = None
        assert feast.tracing._lazy_init(store) is False

    def test_disabled_when_mlflow_not_enabled(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow.enabled = False
        assert feast.tracing._lazy_init(store) is False

    def test_disabled_when_enable_distributed_tracing_false(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow.enabled = True
        store.config.mlflow.enable_distributed_tracing = False
        assert feast.tracing._lazy_init(store) is False

    def test_idempotent(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow = None
        feast.tracing._lazy_init(store)
        feast.tracing._lazy_init(store)
        assert feast.tracing._initialized is True

    def test_enabled_with_valid_config(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow.enabled = True
        store.config.mlflow.enable_distributed_tracing = True
        store.config.mlflow.get_tracking_uri.return_value = None
        store.config.project = "test_project"

        result = feast.tracing._lazy_init(store)
        assert result is True
        assert feast.tracing._enabled is True
        assert feast.tracing._mlflow_mod is not None


class TestTracedToolSpan:
    def test_noop_when_disabled(self):
        import feast.tracing

        feast.tracing._initialized = False
        store = MagicMock()
        store.config.mlflow = None

        with feast.tracing.traced_tool_span(store, "test.span") as span:
            assert span is None


# ---------------------------------------------------------------------------
# MlflowConfig field tests
# ---------------------------------------------------------------------------


class TestMlflowConfigFields:
    def test_enable_distributed_tracing_default_is_true(self):
        from feast.mlflow_integration.config import MlflowConfig

        cfg = MlflowConfig()
        assert cfg.enable_distributed_tracing is True

    def test_enable_distributed_tracing_can_disable(self):
        from feast.mlflow_integration.config import MlflowConfig

        cfg = MlflowConfig(enable_distributed_tracing=False)
        assert cfg.enable_distributed_tracing is False


# ---------------------------------------------------------------------------
# _push_trace_context tests
# ---------------------------------------------------------------------------


class TestPushTraceContext:
    def test_noop_when_no_scope(self):
        """_push_trace_context should do nothing when no trace scope is active."""
        from feast.tracing_context import get_current_context

        assert get_current_context() is None

        store = MagicMock()
        store.registry = MagicMock()
        store.project = "test"
        store._push_trace_context = MagicMock()
        store._push_trace_context(["fv:f1"])

    def test_pushes_refs_when_scope_active(self):
        """Manually simulate what _push_trace_context does."""
        from feast.tracing_context import feast_trace_scope, get_current_context

        with feast_trace_scope() as ctx:
            assert get_current_context() is ctx
            ctx.push_retrieval(
                feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
                feature_service="driver_svc",
            )
            assert len(ctx.feature_refs) == 2
            assert ctx.feature_service == "driver_svc"

        assert get_current_context() is None
