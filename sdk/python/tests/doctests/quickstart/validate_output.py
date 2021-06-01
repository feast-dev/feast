import os
import re
import sys
import textwrap

output_logs_folder = sys.argv[1]


def test_init():
    with open(os.path.join(output_logs_folder, "2")) as f:
        actual_output = f.read()
    output_regex_text = r"""
    Creating a new Feast repository in .+?feature_repo

    """
    output_regex_text = textwrap.dedent(output_regex_text).strip()
    print_info(actual_output, output_regex_text)
    assert re.search(output_regex_text, actual_output)


def test_apply():
    with open(os.path.join(output_logs_folder, "3")) as f:
        actual_output = f.read()
    output_regex_text = r"""
    Registered entity driver_id

    """
    output_regex_text = textwrap.dedent(output_regex_text).strip()
    print_info(actual_output, output_regex_text)
    assert re.search(output_regex_text, actual_output)


def test_training():
    with open(os.path.join(output_logs_folder, "4")) as f:
        actual_output = f.read()
    output_regex_text = r"""
    event_timestamp.+?driver_id.+?driver_hourly_stats__conv_rate.+?driver_hourly_stats__acc_rate.+?driver_hourly_stats__avg_daily_trips

    """
    output_regex_text = textwrap.dedent(output_regex_text).strip()
    print_info(actual_output, output_regex_text)
    assert re.search(output_regex_text, actual_output)


def test_predict():
    with open(os.path.join(output_logs_folder, "6")) as f:
        actual_output = f.read()
    output_regex_text = r"""
    \'driver_hourly_stats__conv_rate\': \[[0-9]+?\.[0-9].+?\]

    """
    output_regex_text = textwrap.dedent(output_regex_text).strip()
    print_info(actual_output, output_regex_text)
    assert re.search(output_regex_text, actual_output)


def print_info(actual_output, output_regex_text):
    print("Searching for regex match:")
    print("----")
    print(output_regex_text)
    print("----")
    print("Within:")
    print("----")
    print(actual_output)
    print("----")


def run_tests():

    test_init()

    test_apply()

    test_training()

    test_predict()

    return


if __name__ == "__main__":
    run_tests()
