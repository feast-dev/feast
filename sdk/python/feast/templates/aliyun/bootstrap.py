# Copy from aws template
import click

import odps 
from feast.infra.utils import aliyun_utils
import pandas as pd
import time

def replace_str_in_file(file_path, match_str, sub_str):
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


def bootstrap():
    # Bootstrap() will automatically be called from the init_repo() during `feast init`

    import pathlib
    from datetime import datetime, timedelta

    from feast.driver_test_data import create_driver_hourly_stats_df

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)

    driver_entities = [1001, 1002, 1003, 1004, 1005]
    driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

    ak = click.prompt("Aliyun access key")
    sk = click.prompt("Aliyun sercet key")
    p = click.prompt("Maxcompute project")
    region = click.prompt("Maxcompute region")
    endpoint = click.prompt("Maxcompute endpoint")
    
    holo_host = click.prompt("Aliyun Hologres host")
    holo_port = click.prompt("Aliyun Hologres port")
    holo_db = click.prompt("Aliyun Hologres db")

    client = client = aliyun_utils.get_maxcompute_client(
            ak=ak,
            sk=sk,
            project=p,
            region=region,
            endpoint=endpoint,
        )

    if click.confirm(
        "Should I upload example data to Maxcompute (overwriting 'feast_driver_hourly_stats' table)?",
        default=True,
    ):
        
        upload_df = odps.df.DataFrame(driver_df)
        upload_df.persist("feast_driver_hourly_stats", odps=client, lifecycle=1)
        print("upload `feast_driver_hourly_stats` succ!")
    
    if click.confirm(
        "Should I upload entity data to Maxcompute (overwriting 'feast_driver_entity_table' table)?",
        default=True,
    ):
        entity_df = pd.DataFrame(
		{
		    "event_timestamp": [
			pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
			for dt in pd.date_range(
			    start=datetime.now() - timedelta(days=2),
			    end=datetime.now(),
			    periods=2,
			)
		    ],
		    "driver_id": [1004, 1005],
		}
	    )

       

        upload_df = odps.df.DataFrame(entity_df)
        upload_df.persist("feast_driver_entity_table", odps=client, lifecycle=1)
        print("upload `feast_driver_entity_table` succ!")

    repo_path = pathlib.Path(__file__).parent.absolute()
    config_file = repo_path / "feature_store.yaml"

    replace_str_in_file(config_file, "%ALIYUN_REGION%", region)
    replace_str_in_file(config_file, "%ALIYUN_ACCESS_KEY%", ak)
    replace_str_in_file(config_file, "%ALIYUN_SECRET_KEY%", sk)
    replace_str_in_file(config_file, "%MAXCOMPUTE_PROJECT%", p)
    replace_str_in_file(config_file, "%MAXCOMPUTE_ENDPOINT%", endpoint) 
    replace_str_in_file(config_file, "%HOLOGRES_HOST%", holo_host)
    replace_str_in_file(config_file, "%HOLOGRES_PORT%", holo_port)
    replace_str_in_file(config_file, "%HOLOGRES_DB%", holo_db)



if __name__ == "__main__":
    bootstrap()
