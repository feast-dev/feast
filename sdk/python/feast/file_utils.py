#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


def replace_str_in_file(file_path, match_str, sub_str):
    """
    Replace a string, in-place, in a text file, throughout.
    Does not return anything, side-effect only.
    Inputs are:
        file_path, a string with the path to the ascii file to edit
        match_str, the substring to be replaced (as many times as it's found)
        sub_str,   the string to insert in place of match_str
    NOTE: not suitable for very large files (it does all in-memory).
    """
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


def remove_lines_from_file(file_path, match_str, partial=True):
    """
    Edit an ascii file (in-place) by removing all lines that
    match a given string (partially or totally).
    Does not return anything, side-effect only.
    Inputs are:
        file_path, a string with the path to the ascii file to edit
        match_str, the string to look for in the file lines
        partial, a boolean: if True, any line with match_str as substring
            will be removed; if False, only lines matching it entirely.
    NOTE: not suitable for very large files (it does all in-memory).
    """

    def _line_matcher(line, _m=match_str, _p=partial):
        if _p:
            return _m in line
        else:
            return _m == line

    with open(file_path, "r") as f:
        file_lines = list(f.readlines())

    new_file_lines = [line for line in file_lines if not _line_matcher(line)]

    with open(file_path, "wt") as f:
        f.write("".join(new_file_lines))


def write_setting_or_remove(
    file_path, setting_value, setting_name, setting_placeholder_value
):
    """
    Utility to adapt a settings-file template to some provided values.
    Assumes the file has lines such as
        "    username: c_username"
    (quotes excluded) where the placeholder might be replaced with actual value
    or the line might not be needed altogether.
    Then, calling
        write_settings_or_remove(file_path, new_username, 'username', 'c_username')
    the file is edited in-place in one of two ways:
        1. if new_username is None, the line disappears completely
        2. if e.g.  new_username == 'jenny', the line becomes
            "    username: jenny"
    This utility is called repeatedly (a bit inefficiently, admittedly)
    to refine the template feature-store yaml config to suit the parameters
    supplied during a "feast init" feature store setup.
    """
    if setting_value is not None:
        replace_str_in_file(file_path, setting_placeholder_value, str(setting_value))
    else:
        remove_lines_from_file(file_path, setting_name)
