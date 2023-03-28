from src.load import read_data
from moto import mock_s3
# from unittest.mock import pandas as pd
from unittest.mock import patch
from unittest.mock import Mock
import pytest
import boto3
import botocore
from pathlib import Path
import os

# @pytest.fixture(scope='function')
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#     os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@mock_s3
# Tests read_data func
def test_reads_empty_bucket():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test_bucket_28")
    actual = read_data('test_bucket_28')
    assert actual == {}
    
# def test_reads_contenet_of_bucket():
#     with mock_s3:
#         conn = boto3.resource("s3", region_name="us-east-1")
#         conn.create_bucket(Bucket="test_bucket_28")
#         insert_mock_data = conn.s3.put_object(Bucket="test_bucket_28", Key="test_key", Body={'test_body'}.encode("utf-8"))
#         insert_mock_data.save()
#         actual = read_data('test_bucket_28')
#         print (actual)
#         assert actual == {}

def test_corruption_checker_outputs_success_message_if_okay():
    pass


def test_corruption_checker_outputs_helpful_message_on_failure():
    pass


def test_corruption_checker_outputs_helpful_message_on_different_failure():
    pass

# Tests for make_connection subfunc


def test_make_connection_func():
    pass

# Integration tests for load_to_warehouse
# Make mock_db again, push to it with func, then check


def test_load_to_warehouse_can_push_to_warehouse():
    pass


def test_load_to_warehouse_throws_helpful_error_on_incorrect_format():
    pass


def test_another_fail_case():
    pass

# Tests for AWS Cloudwatch (possible?)
