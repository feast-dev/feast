import pytest

from feast.context.errors import TokenizerNotFoundError, TokenizerUnavailableError
from feast.context.tokenizer import (
    ApproximateTokenizer,
    Cl100kBaseTokenizer,
    O200kBaseTokenizer,
    TiktokenTokenizer,
    TokenizerName,
    get_tokenizer,
)
from feast.errors import FeastInvalidBaseClass


class FakeTokenizer:
    """Named like a tokenizer, but not a Tokenizer subclass."""


class TestApproximateTokenizer:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("abcdefgh", 2),
            ("abcde", 2),  # partial tokens round up
            ("", 0),
            ("a" * 100_000, 25_000),
        ],
    )
    def test_counts_four_characters_per_token(self, text, expected):
        assert ApproximateTokenizer().count_tokens(text) == expected

    def test_chars_per_token_is_configurable(self):
        assert ApproximateTokenizer(chars_per_token=2).count_tokens("abcd") == 2

    @pytest.mark.parametrize("chars_per_token", [0, -1])
    def test_rejects_non_positive_chars_per_token(self, chars_per_token):
        with pytest.raises(ValueError):
            ApproximateTokenizer(chars_per_token=chars_per_token)


class TestTiktokenTokenizer:
    @pytest.mark.parametrize(
        "encoding", [TokenizerName.CL100K_BASE.value, TokenizerName.O200K_BASE.value]
    )
    def test_counts_tokens_for_supported_encodings(self, encoding):
        pytest.importorskip("tiktoken")
        tokenizer = TiktokenTokenizer(encoding)
        assert tokenizer.name == encoding
        assert tokenizer.count_tokens("Hello world") == 2
        assert tokenizer.count_tokens("") == 0
        # Unicode and long text: exact counts are the encoding's business, but
        # neither may crash or come back empty.
        assert tokenizer.count_tokens("北京の天気") > 0
        assert tokenizer.count_tokens("word " * 10_000) >= 10_000

    def test_special_token_text_is_counted_not_rejected(self):
        pytest.importorskip("tiktoken")
        tokenizer = Cl100kBaseTokenizer()
        assert tokenizer.count_tokens("<|endoftext|> appears in this review") > 0

    def test_unknown_encoding_raises(self):
        pytest.importorskip("tiktoken")
        with pytest.raises(TokenizerNotFoundError):
            TiktokenTokenizer("no_such_encoding")


class TestEquality:
    def test_same_name_and_settings_are_equal(self):
        assert ApproximateTokenizer() == ApproximateTokenizer()
        assert len({ApproximateTokenizer(), ApproximateTokenizer()}) == 1

    def test_different_settings_are_not_equal(self):
        assert ApproximateTokenizer() != ApproximateTokenizer(chars_per_token=3)

    def test_same_encoding_from_different_classes_is_equal(self):
        pytest.importorskip("tiktoken")
        assert Cl100kBaseTokenizer() == TiktokenTokenizer(
            TokenizerName.CL100K_BASE.value
        )
        assert Cl100kBaseTokenizer() != O200kBaseTokenizer()


class TestGetTokenizer:
    @pytest.mark.parametrize(
        "name, expected_class",
        [
            (TokenizerName.CL100K_BASE.value, Cl100kBaseTokenizer),
            (TokenizerName.O200K_BASE.value, O200kBaseTokenizer),
            (TokenizerName.APPROXIMATE.value, ApproximateTokenizer),
        ],
    )
    def test_resolves_builtin_names(self, name, expected_class):
        if expected_class is not ApproximateTokenizer:
            pytest.importorskip("tiktoken")
        assert isinstance(get_tokenizer(name), expected_class)

    def test_resolves_a_class_path(self):
        tokenizer = get_tokenizer("feast.context.tokenizer.ApproximateTokenizer")
        assert isinstance(tokenizer, ApproximateTokenizer)

    def test_rejects_a_class_path_that_is_not_a_tokenizer(self):
        with pytest.raises(FeastInvalidBaseClass):
            get_tokenizer("tests.unit.context.test_tokenizer.FakeTokenizer")

    def test_rejects_a_path_that_is_not_a_tokenizer_class_name(self):
        with pytest.raises(TokenizerNotFoundError):
            get_tokenizer("feast.repo_config.RepoConfig")

    def test_unknown_name_raises_with_builtin_names(self):
        with pytest.raises(TokenizerNotFoundError, match="cl100k_base"):
            get_tokenizer("gpt-42")

    def test_builds_once_and_caches(self):
        assert get_tokenizer("approximate") is get_tokenizer("  approximate  ")

    def test_accepts_enum_member_and_equivalent_string(self):
        assert get_tokenizer(TokenizerName.APPROXIMATE) is get_tokenizer("approximate")

    def test_tokenizer_instance_passes_through(self):
        tokenizer = ApproximateTokenizer()
        assert get_tokenizer(tokenizer) is tokenizer

    @pytest.mark.parametrize("name", ["   ", None])
    def test_rejects_invalid_names(self, name):
        with pytest.raises(TypeError):
            get_tokenizer(name)


class TestFallback:
    def test_falls_back_to_estimate_without_tiktoken(self, no_tiktoken, caplog):
        tokenizer = get_tokenizer(TokenizerName.CL100K_BASE)

        assert isinstance(tokenizer, ApproximateTokenizer)
        assert tokenizer.count_tokens("abcdefgh") == 2
        # The shared instance, so budgets that fell back stay comparable.
        assert tokenizer is get_tokenizer(TokenizerName.APPROXIMATE)
        assert any("tiktoken" in record.message for record in caplog.records)

    def test_fallback_disabled_raises(self, no_tiktoken):
        with pytest.raises(TokenizerUnavailableError, match="pip install tiktoken"):
            get_tokenizer(TokenizerName.CL100K_BASE, fallback=False)
