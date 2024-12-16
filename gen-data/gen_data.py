import pandas as pd
import numpy as np
import random
from faker import Faker
import datetime
from unidecode import unidecode as ud
from rstr import xeger
import yaml

fake = Faker('vi_VN')
models = {}

def random_date(start_year=1970, end_year=2024, str_format=None, **kwargs):
    # Generate a random date between start_year and end_year
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    a = start_date + datetime.timedelta(days=random_days)
    if str_format:
        a = a.strftime(str_format)
    else:
        a = a.strftime("%Y-%m-%d")
    return a

def random_int(min_value=0, max_value=1_000_000, num_records=1, unique=False, **kwargs):
    if num_records == 1:
        return random.randint(min_value, max_value)
    else:
        return np.random.choice(np.arange(min_value, max_value+1, 1), size=num_records, replace=not unique)

def random_float(min_value=0, max_value=1_000_000, digits=None, **kwargs):
    if digits is None:
        return random.uniform(min_value, max_value)
    return round(random.uniform(min_value, max_value), digits)

def random_string(regex_pattern, unidecode=False, upper=False, **kwargs):
    a = xeger(regex_pattern)
    return postprocess_str(a, unidecode, upper, **kwargs)

def random_name(unidecode=False, upper=False, **kwargs):
    a = fake.name()
    return postprocess_str(a, unidecode, upper, **kwargs)

def random_address(unidecode=False, upper=False, **kwargs):
    a = fake.address()
    return postprocess_str(a, unidecode, upper, **kwargs)

def random_street_address(unidecode=False, upper=False, **kwargs):
    a = fake.street_address()
    return postprocess_str(a, unidecode, upper, **kwargs)

def random_email(unidecode=False, upper=False, **kwargs):
    a = fake.email()
    return postprocess_str(a, unidecode, upper, **kwargs)

def postprocess_str(a, unidecode=False, upper=False, **kwargs):
    if unidecode:
        a = ud(a)
    if upper:
        a = a.upper()
    return a

def random_category(elements, unidecode=False, upper=False, **kwargs):
    a = fake.random_element(elements=elements)
    if type(a) is str:
        return postprocess_str(a, unidecode, upper, **kwargs)
    else:
        return a

def maybe_null(value, prob=0.3):
    return value if random.random() > prob else None

def all_null(num_records):
    return [None for _ in range(num_records)]

def random_foreign_key(to, field, size, unique=False):
    return np.random.choice(models[to][field].values, size=size, replace=not unique)

def generate_data(config, num_records=10):
    data = {}
    for column in config:
        if 'relationships' in column:
            kwargs = {k: v for k,v in column['relationships'].items()}
            data[column['name']] = random_foreign_key(**kwargs, size=num_records)
            if 'null_rate' in column:
                data[column['name']] = [maybe_null(x, column['null_rate']) for x in data[column['name']]]

        elif column.get('null_rate', 0) == 1:
            data[column['name']] = all_null(num_records)

        else:
            gen_func = globals()['random_'+column['type']]
            kwargs = {k: v for k,v in column.items() if k not in ['name','type','null_rate']}

            if 'unique' in column:
                data[column['name']] = gen_func(num_records=num_records, **kwargs)
            else:
                data[column['name']] = [gen_func(**kwargs) for _ in range(num_records)]

            if 'null_rate' in column:
                data[column['name']] = [maybe_null(x, column['null_rate']) for x in data[column['name']]]

    return pd.DataFrame.from_dict(data).astype(str).replace('None', '')

models['corebank_branch'] = pd.read_csv("/home/dev/duy/stb_samples/models/corebank_branch.csv")

# with open('/home/dev/duy/stb_samples/models/corebank_customer.yml', 'r') as f:
#     t24_config = yaml.safe_load(f)
#     models['corebank_customer'] = generate_data(t24_config, 100_000)
# # models['corebank_customer'] = pd.read_csv('/home/dev/duy/stb_samples/target/corebank_customer.csv')

# with open('/home/dev/duy/stb_samples/models/corecard_customer.yml', 'r') as f:
#     card_config = yaml.safe_load(f)
#     models['corecard_customer'] = generate_data(card_config, 100_000)
# # models['corecard_customer'] = pd.read_csv('/home/dev/duy/stb_samples/target/corecard_customer.csv')

# with open('/home/dev/duy/stb_samples/models/crm_customer.yml', 'r') as f:
#     card_config = yaml.safe_load(f)
#     models['crm_customer'] = generate_data(card_config, 100_000)
# # models['crm_customer'] = pd.read_csv('/home/dev/duy/stb_samples/target/crm_customer.csv')


with open('/home/dev/duy/stb_samples/models/corebank_corp.yml', 'r') as f:
    t24_config = yaml.safe_load(f)
    models['corebank_corp'] = generate_data(t24_config, 100_000)
# models['corebank_corp'] = pd.read_csv('/home/dev/duy/stb_samples/target/corebank_corp.csv')

with open('/home/dev/duy/stb_samples/models/corecard_corp.yml', 'r') as f:
    card_config = yaml.safe_load(f)
    models['corecard_corp'] = generate_data(card_config, 100_000)
# models['corecard_corp'] = pd.read_csv('/home/dev/duy/stb_samples/target/corecard_corp.csv')


for model in models:
    # if model in ['crm_customer']:
    models[model].to_csv(f'/home/dev/duy/stb_samples/target/{model}.csv', index=False, doublequote=False, escapechar='\\')
