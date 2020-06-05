import datetime 
import numpy as np
import pandas as pd
from feast import Feature, FeatureSet, Entity, ValueType
from pytz import utc

"""
Examples of anticipated daily feature ingestion load:

Product Computer Vision features (20K/day - generated once a day), see create_product_image_features_df() below:
- CV1: 4 x 64 float
- CV2: 2 x 64 float
- CV3: 256 x float
- CV4: 8192 x float

Product Text Attributes (20K/day - generated once a day), see create_product_text_attributes_df() below: 
- TX1-n: string or list of string

Fraud features: customer counts for different windows of time (15M throughout day):
- FR1-7: int
"""

product = Entity('product_id', ValueType.INT64)


PRODUCT_IMAGE_FEATURE_SET = FeatureSet(
    'product_image_features',
    entities=[Entity('product_id', ValueType.INT64)],
    features=[
        Feature('cv1', ValueType.DOUBLE_LIST),
        Feature('cv2', ValueType.DOUBLE_LIST),
        Feature('cv3', ValueType.DOUBLE_LIST),
        Feature('cv4', ValueType.DOUBLE_LIST),
    ]
)


PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET = FeatureSet(
    'product_text_attributes',
    entities=[Entity('product_id', ValueType.INT64)],
    features=[
        Feature('brand', ValueType.STRING),
        Feature('brand-range', ValueType.STRING),
        Feature('colours', ValueType.STRING_LIST),
        Feature('footware', ValueType.STRING),
        Feature('heel-height', ValueType.STRING),
        Feature('heel-type', ValueType.STRING),
        Feature('materials', ValueType.STRING_LIST),
        Feature('sole-height', ValueType.STRING),
    ]
)


FRAUD_COUNTS_FEATURE_SET = FeatureSet(
    'fraud_count_features',
    entities=[Entity('customer_id', ValueType.INT64)],
    features=[
        Feature('window_count1', ValueType.INT64),
        Feature('window_count2', ValueType.INT64),
        Feature('window_count3', ValueType.INT64),
        Feature('window_count4', ValueType.INT64),
        Feature('window_count5', ValueType.INT64),
        Feature('window_count6', ValueType.INT64),
        Feature('window_count7', ValueType.INT64),
    ]
)


PRODUCT_ATTRIBUTES = {
    'brand': [
        'Ash',
        'PRADA',
        'Birkenstock',
        'Valentino Garavani',
        'LEMAIRE',
        'R . M . Williams',
        'Eres',
        'Moschino',
        'GIUSEPPE JUNIOR',
        'Montelpare Tradition',
        'Moa Master Of Arts',
        "L ' Autre Chose",
        'See by Chloé',
        'Y - 3',
        'Nike',
    ],
    'brand-range': [
        'Gizeh',
        'Gain',
        'Stingray',
        'Majolica',
        'Glory',
        'Revekka',
        'Diana Strass',
        'Blackout',
        'Air Jordan 1 High',
        'Authentic',
        'Galore',
        'Montecarlo Mondial',
    ],
    'colours': [
        ['black', 'white'],
        ['silver'],
        ['red'],
        ['light green'],
        ['bright orange'],
        ['tan brown'],
        ['yellow', 'green'],
        ['bright blue'],
        ['orange'],
        ['cinnamon brown'],
        ['hot - pink'],
        ['silver grey'],
        ['navy'],
    ],
    'footware': [    	
        'mules',
        'sandals',
        'flip - flops',
        'sliders',
        'ballerina shoes',
        'mule',
        'slingbacks',
        'derby shoes',
        'school shoes',
        'wedge shoes',
        'runner',
    ],
    'heel-height': ['{}mm'.format(x) for x in range(0, 101, 5)],
    'heel-type': [
        'sculpted',
        'chunky',
        'screw',
        'stacked',
        'discrete',
        'slender',
        'collapsible',
        'platform',
    ],
    'materials': [
        ['raffia'],
        ['rubber', 'polyester'],
        ['nylon'],
        ['silk'],
        ['patent sheepskin'],
        ['mesh'],
    ],
    'sole-height': ['{}mm'.format(x) for x in range(0, 101, 5)],
}


def create_cv1():
    value = np.random.random((4, 64))
    value[:, 25:] = 0
    np.random.shuffle(value.T)
    return value


def create_cv2():
    value = np.random.random((2, 64))
    value[:, 15:] = 0
    np.random.shuffle(value.T)
    return value


def create_cv3():
    value = np.random.randn(256).astype(np.float32)
    return value / np.linalg.norm(value)


def create_cv4():
    value = np.random.randn(8192)
    return value / np.linalg.norm(value)


def create_product_image_features_df(initial_product_id=1, n=20000):
    dt = datetime.datetime.now(datetime.timezone.utc)
    return pd.DataFrame(
    {
        'datetime': dt,
        'product_id': list(range(initial_product_id, initial_product_id + n)),
        'cv1': [create_cv1().flatten() for _ in range(n)],
        'cv2': [create_cv2().flatten() for _ in range(n)],
        'cv3': [create_cv3() for _ in range(n)],
        'cv4': [create_cv4() for _ in range(n)],
    })


def create_product_text_attributes_df(initial_product_id=1, n=20000):
    dt = datetime.datetime.now(datetime.timezone.utc)
    return pd.DataFrame(
    {
        'datetime': dt,
        'product_id': list(range(initial_product_id, initial_product_id + n)),
        'brand': [np.random.choice(PRODUCT_ATTRIBUTES['brand']) for _ in range(n)],
        'brand-range': [np.random.choice(PRODUCT_ATTRIBUTES['brand-range']) for _ in range(n)],
        'colours': [np.random.choice(PRODUCT_ATTRIBUTES['colours']) for _ in range(n)],
        'footware': [np.random.choice(PRODUCT_ATTRIBUTES['footware']) for _ in range(n)],
        'heel-height': [np.random.choice(PRODUCT_ATTRIBUTES['heel-height']) for _ in range(n)],
        'heel-type': [np.random.choice(PRODUCT_ATTRIBUTES['heel-type']) for _ in range(n)],
        'materials': [np.random.choice(PRODUCT_ATTRIBUTES['materials']) for _ in range(n)],
        'sole-height': [np.random.choice(PRODUCT_ATTRIBUTES['sole-height']) for _ in range(n)],
    })

