import os

import pkg_resources
from click.testing import CliRunner
import pytest
import requests
import traceback

from feast.cli import config, member, project

@pytest.fixture(scope="module")
def runner():
    return CliRunner()

@pytest.mark.integration
def test_config_list(runner):
  result = runner.invoke(
      config, [
        'list'
      ])
  print(result.output)

@pytest.mark.integration
def test_config_set(runner):
  result = runner.invoke(
      config, [
        'set',
        'test_prop',
        'test_value'
      ]),

@pytest.mark.integration
def test_project_create(runner):
  result = runner.invoke(
      project, [
        'create',
        'project1'
      ]
  )
  print(result.output)

@pytest.mark.integration
def test_project_archive(runner):
  result = runner.invoke(
      project, [
        'archive',
        'project1'
      ]
  )
  print(result.output)

@pytest.mark.integration
def test_project_list(runner):
  result = runner.invoke(
      project, [
        'list'
      ]
  )
  # print(result.output)


@pytest.mark.integration
def test_member_list(runner):
  result = runner.invoke(
      member, [
        'list',
        'customer_satisfaction'
      ]
  )
  print(result.output)

@pytest.mark.integration
def test_member_add(runner):
  result = runner.invoke(
      member, [
        'add',
        'member1',
        'customer_satisfaction'
      ]
  )
  print(result.output)

@pytest.mark.integration
def test_member_remove(runner):
  result = runner.invoke(
      member, [
        'remove',
        'member1',
        'customer_satisfaction'
      ]
  )
  print(result.output)
