#
# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import tempfile

import boto3
import grpc
import pandas as pd
import pandavro
import pytest
from moto import mock_s3
from pandas.testing import assert_frame_equal
from pytest import fixture, raises

from feast.job import JobProto, RetrievalJob
from feast.serving import ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import DataFormat, GetJobResponse
from feast.serving.ServingService_pb2 import Job as BatchRetrievalJob
from feast.serving.ServingService_pb2 import JobStatus, JobType

BUCKET = "test_bucket"

TEST_DATA_FRAME = pd.DataFrame(
    {
        "driver": [1001, 1002, 1003],
        "transaction": [1001, 1002, 1003],
        "driver_id": [1001, 1002, 1003],
    }
)


class TestRetrievalJob:
    @fixture
    def retrieve_job(self):

        serving_service_stub = Serving.ServingServiceStub(grpc.insecure_channel(""))
        job_proto = JobProto(
            id="123",
            type=JobType.JOB_TYPE_DOWNLOAD,
            status=JobStatus.JOB_STATUS_RUNNING,
        )
        return RetrievalJob(job_proto, serving_service_stub)

    @fixture
    def avro_data_path(self):
        final_results = tempfile.mktemp()
        pandavro.to_avro(file_path_or_buffer=final_results, df=TEST_DATA_FRAME)
        return final_results

    def test_to_dataframe_local_file_staging_should_pass(
        self, retrieve_job, avro_data_path, mocker
    ):
        mocker.patch.object(
            retrieve_job.serving_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"file://{avro_data_path}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )
        retrived_df = retrieve_job.to_dataframe()
        assert_frame_equal(TEST_DATA_FRAME, retrived_df, check_like=True)

    @mock_s3
    def test_to_dataframe_s3_file_staging_should_pass(
        self, retrieve_job, avro_data_path, mocker
    ):
        s3_client = boto3.client("s3")
        target = "test_proj/test_features.avro"
        s3_client.create_bucket(Bucket=BUCKET)
        with open(avro_data_path, "rb") as data:
            s3_client.upload_fileobj(data, BUCKET, target)

        mocker.patch.object(
            retrieve_job.serving_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"s3://{BUCKET}/{target}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )
        retrived_df = retrieve_job.to_dataframe()
        assert_frame_equal(TEST_DATA_FRAME, retrived_df, check_like=True)

    @pytest.mark.parametrize(
        "job_proto,exception",
        [
            (
                GetJobResponse(
                    job=BatchRetrievalJob(
                        id="123",
                        type=JobType.JOB_TYPE_DOWNLOAD,
                        status=JobStatus.JOB_STATUS_DONE,
                        data_format=DataFormat.DATA_FORMAT_AVRO,
                        error="Testing job failure",
                    )
                ),
                Exception,
            ),
            (
                GetJobResponse(
                    job=BatchRetrievalJob(
                        id="123",
                        type=JobType.JOB_TYPE_DOWNLOAD,
                        status=JobStatus.JOB_STATUS_DONE,
                        data_format=DataFormat.DATA_FORMAT_INVALID,
                    )
                ),
                Exception,
            ),
        ],
        ids=["when_retrieve_job_fails", "when_data_format_is_not_avro"],
    )
    def test_to_dataframe_s3_file_staging_should_raise(
        self, retrieve_job, mocker, job_proto, exception
    ):
        mocker.patch.object(
            retrieve_job.serving_stub, "GetJob", return_value=job_proto,
        )
        with raises(exception):
            retrieve_job.to_dataframe()
