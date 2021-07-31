import pandas as pd
import os
from pathlib import Path

tsv_path = "../index/index.csv"
df = pd.read_csv(tsv_path, ",")

if __name__ == "__main__":
  for index, row in df.iterrows():
    path = "../static/{}".format(row["nanoid"])

    os.mkdir(path)
    Path(os.path.join(path, ".gitkeep")).touch()
    os.mkdir(os.path.join(path, "resource"))
    Path(os.path.join(path, "resource/.gitkeep")).touch()
