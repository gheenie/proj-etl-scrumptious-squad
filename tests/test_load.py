from src.load import read_data
from moto import mock_s3
# from unittest.mock import pandas as pd
from unittest.mock import patch
from unittest.mock import Mock
import pytest
import boto3
import botocore
from pathlib import Path
from mymodule import MyModel



@mock_s3
# Tests read_data func
def test_reads_empty_bucket():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test_bucket_28")
    actual = read_data('test_bucket_28')
    assert actual == {}
    
@mock_s3
def test_reads_contenet_of_bucket():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test_bucket_28")
    my_data = Data()



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
