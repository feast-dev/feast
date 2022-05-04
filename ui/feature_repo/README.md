# Feast repo

## Overview

This pulls from the dataset used in https://github.com/feast-dev/feast-aws-credit-scoring-tutorial but adds metadata for a full set of Feast FCOs.

This also adds an on demand feature view + feature services + a saved dataset.

## Setting up Feast

Install a dev build Feast using pip

Clone a feast repo: 
```bash
git clone https://github.com/feast-dev/feast.git
```

Install a dev build of feast
```bash
cd feast
pip install -e ".[dev]"
```

Then for this demo, you'll actually need to fix a bug by adding this to `type_map.py#L144`:
```python
if isinstance(value, np.bool_):
    return ValueType.BOOL
```

## Test features
We have already set up a feature repository here using test data from [data](data)). Features have already been pre-materialized to a local sqlite online store. The results of `feast registry-dump` have been thrown into [registry.json](../public/registry.json)

To query against this registry, you can use run the `test_get_features.py`
```bash
python test_get_features.py
```

Output:
```
--- Historical features (from saved dataset) ---
   mortgage_due  credit_card_due  missed_payments_1y  total_wages        dob_ssn           event_timestamp state  tax_returns_filed location_type  population        city  zipcode
0        741165             2944                   3     71067272  19781116_7723 2021-04-12 08:12:10+00:00    MI               2424       PRIMARY        4421     WEIDMAN    48893
1         91803             8419                   0    534687864  19530219_5179 2021-04-12 10:59:42+00:00    GA              19583       PRIMARY       38542      DALTON    30721
2       1553523             5936                   0    226748453  19500806_6783 2021-04-12 15:01:12+00:00    TX               6827       PRIMARY       12902    CLEBURNE    76031
3        976522              833                   0     34796963  19931128_5771 2021-04-12 16:40:26+00:00    VA               1287       PRIMARY        2342  GLADE HILL    24092

--- Online features ---
city  :  ['DALTON']
credit_card_due  :  [8419]
dob_ssn  :  ['19530219_5179']
location_type  :  ['PRIMARY']
missed_payments_1y  :  [0]
mortgage_due  :  [91803]
population  :  [38542]
state  :  ['GA']
tax_returns_filed  :  [19583]
total_wages  :  [534687864]
zipcode  :  [30721]
city  :  ['DALTON']
credit_card_due  :  [8419]
dob_ssn  :  ['19530219_5179']
location_type  :  ['PRIMARY']
missed_payments_1y  :  [0]
mortgage_due  :  [91803]
population  :  [38542]
state  :  ['GA']
tax_returns_filed  :  [19583]
total_wages  :  [534687864]
transaction_gt_last_credit_card_due  :  [False]
zipcode  :  [30721]

```
