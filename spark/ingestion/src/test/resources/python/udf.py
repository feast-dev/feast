import sys

from pyspark import cloudpickle
from pyspark.sql.types import BooleanType

import pandas as pd
import numpy as np

from great_expectations.dataset import PandasDataset


def create_suite():
    df = pd.DataFrame()
    df['num'] = np.random.randint(0, 10, 100)
    df['num2'] = np.random.randint(0, 20, 100)
    ds = PandasDataset.from_dataset(df)

    ds.expect_column_values_to_be_between('num', 0, 10)
    ds.expect_column_values_to_be_between('num2', 0, 20)

    return ds.get_expectation_suite()


def create_validator(suite):
    def validate(df) -> pd.DataFrame:
        ds = PandasDataset.from_dataset(df)
        # print(ds, ds.shape)
        result = ds.validate(suite, result_format='COMPLETE')
        valid_rows = pd.Series([True] * ds.shape[0])
        # print(result)
        for check in result.results:
            if check.success:
                continue

            valid_rows.iloc[check.result['unexpected_index_list']] = False
        return valid_rows

    return validate


def main(dest_path):
    with open(dest_path, 'wb') as f:
        fun = create_validator(create_suite())
        command = (fun, BooleanType())
        cloudpickle.dump(command, f)


if __name__ == '__main__':
    main(sys.argv[1])
