import pytest

from feast.config import Config
from feast.constants import ConfigOptions as opt
from feast.pyspark.launcher import _parse_additional_spark_options


class TestSparkAdditionalOpts:
    def parse(self, options_string):
        return _parse_additional_spark_options(
            Config(options={opt.SPARK_ADDITIONAL_OPTS: options_string})
        )

    def test_normal_options(self):
        options_string = "option1=aaaa;option2=bbb"
        assert self.parse(options_string) == {"option1": "aaaa", "option2": "bbb"}

    def test_value_with_delimiter(self):
        options_string = 'option1=aaaa;option2="b;b"'
        assert self.parse(options_string) == {"option1": "aaaa", "option2": "b;b"}

    def test_value_with_another_delimiter(self):
        options_string = 'option1=aaaa;option2="b=b"'
        assert self.parse(options_string) == {"option1": "aaaa", "option2": "b=b"}

    def test_error_on_wrong_format(self):
        options_string = "option1=aaaa;option2"
        with pytest.raises(ValueError):
            self.parse(options_string)
