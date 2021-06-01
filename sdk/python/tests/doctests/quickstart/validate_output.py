import os
import re
import sys
import textwrap

output_logs_folder = sys.argv[1]


def test_init():
    with open(os.path.join(output_logs_folder, "2")) as f:
        actual_output = f.read()
    output_regex_text = """
    Creating a new Feast repository in .+?feature_repo

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

    return


if __name__ == "__main__":
    run_tests()
