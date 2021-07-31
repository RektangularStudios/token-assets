import os
import pandas as pd
from shutil import copyfile
import json
import yaml

script_dir = os.path.dirname(os.path.realpath(__file__))
static_dir = os.path.join(script_dir, "../static")

index_dir = os.path.join(script_dir, "../index")
static_csv = os.path.join(index_dir, "index.csv")

metadata_dir = os.path.join(script_dir, "../metadata")

config_dir = os.path.join(script_dir, "../config")
cfg_yaml = os.path.join(config_dir, "config.yaml")

def main():
  with open(cfg_yaml, 'r') as cfg_file:
    cfg = yaml.full_load(cfg_file)

  # read static serving index
  print("reading static serving index from {}".format(static_csv))
  static_df = pd.read_csv(static_csv)
  print(static_df.head())

  for _, row in static_df.iterrows():
    print("Checking {}".format(row["name"]))

    # load "onchain metadata"
    try:
      with open(os.path.join(static_dir, row["nanoid"], "onchain.json"), 'r') as f:
        onchain = json.load(f)
    except:
      print("Missing onchain.json, skipping")
      continue

    asset_id = list(onchain["721"][cfg["policy_id"]])[0]

    in_path = os.path.join(static_dir, row["nanoid"], "onchain.json")
    out_path = os.path.join(metadata_dir, "{}.json".format(asset_id))
    copyfile(in_path, out_path)

if __name__ == "__main__":
  main()
