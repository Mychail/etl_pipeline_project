from utils.aws import aws_session
from utils.fakers_operations import profile_data_generator

import awswrangler as wr


def data_extract_to_s3():
    wr.s3.to_parquet(
        df=profile_data_generator(200),
        path="s3://faker-data-test-bucket/user_profile/",
        boto3_session=aws_session(),
        mode="append",
        dataset=True
    )
    return "Data successfully written to s3"
