import os

import faker
import pandas as pd
from rich.progress import track


def generate_fake_data(n=100):
    f = f'data/fake_data_people_{n}.parquet'
    if f in os.listdir('data'):
        return pd.read_parquet(f)

    fake = faker.Faker()
    data = []
    for _ in track(range(n), description="Processing..."):
        data.append({
            'name': fake.name(),
            'email': fake.email(),
            'address': fake.address()
        })
    df = pd.DataFrame(data)
    df.to_parquet(f, index=False)
    return df
