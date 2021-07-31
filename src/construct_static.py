import os
import re
import pandas as pd
import json
import urllib.request
import subprocess
import yaml

script_dir = os.path.dirname(os.path.realpath(__file__))
static_dir = os.path.join(script_dir, "../static")

ipfs_static_dir = os.path.join(script_dir, "..", "constructed_ipfs_static")
sia_static_dir = os.path.join(script_dir, "..", "constructed_sia_static")
cdn_static_dir = os.path.join(script_dir, "..", "constructed_cdn_static")

index_dir = os.path.join(script_dir, "../index")
static_csv = os.path.join(index_dir, "index.csv")

config_dir = os.path.join(script_dir, "../config")
cfg_yaml = os.path.join(config_dir, "config.yaml")

def translateDecentralizedUrl(url):
    siaPrefix = "sia://"
    siaPortal = "https://siasky.net/"

    ipfsPrefix = "ipfs://"
    ipfsGateway = "https://api.rektangularstudios.com/ipfs/"

    httpPattern = "https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)"

    if url.startswith(siaPrefix):
      return [url.replace(siaPrefix, siaPortal), "sia"]
    if url.startswith(ipfsPrefix):
      return [url.replace(ipfsPrefix, ipfsGateway), "ipfs"]
    if re.match(httpPattern, url):
      return [url, "cdn"]
    return ["https://" + url, "cdn"]

def get_ipfs_multihash(file_path):
  # requires that script caller has IPFS installed
  # ipfs add file_path --only-hash -q
  result = subprocess.run(['ipfs', 'add', file_path, '--only-hash', '-q'], stdout=subprocess.PIPE)
  return result.stdout.decode('utf-8').strip()

def get_filename_from_resource(r):
  names = {
    "Artwork": ["artwork_low.jpg", "artwork.png"],
    "Video": ["video.mp4"],
    "Card": ["card_low.jpg", "card.png"],
    "OccultaNovelliaCharacter": ["character.json"],
  }
  return names[r["resource_id"]][r["priority"]]

def main():
  with open(cfg_yaml, 'r') as cfg_file:
    cfg = yaml.full_load(cfg_file)

  # read static serving index
  print("reading static serving index from {}".format(static_csv))
  static_df = pd.read_csv(static_csv)
  print(static_df.head())

  dirs = {
    "cdn": cdn_static_dir,
    "ipfs": ipfs_static_dir,
    "sia": sia_static_dir,
  }

  # make root directories
  for k in dirs:
    if not os.path.isdir(dirs[k]):
      os.mkdir(dirs[k])

  for _, row in static_df.iterrows():
    print("Checking {}".format(row["name"]))

    # make character subdirectory
    for k in dirs:
      p = os.path.join(dirs[k], row["nanoid"])
      if not os.path.isdir(p):
        os.mkdir(p)

    # load "onchain metadata"
    try:
      with open(os.path.join(static_dir, row["nanoid"], "onchain.json"), 'r') as f:
        onchain = json.load(f)
    except:
      print("Missing onchain.json, skipping")
      continue

    # get URL to novellia resource
    asset_id = list(onchain["721"][cfg["policy_id"]])[0]
    onchain_resource = onchain["721"][cfg["policy_id"]][asset_id]["resource"]
    if onchain_resource[0]["resource_id"] != "Novellia":
      raise ValueError("Didn't get Novellia resource_id")
    nvla_resource = onchain_resource[0]

    # download and verify novellia resource
    for u in nvla_resource["url"]:
      translate = translateDecentralizedUrl(u)
      url = translate[0]
      t = translate[1]
      file_path = os.path.join(dirs[t], row["nanoid"], "nvla.json")
      if not os.path.exists(file_path):
        urllib.request.urlretrieve(url, file_path)
      m = get_ipfs_multihash(file_path)
      if m != nvla_resource["multihash"]:
        raise ValueError("({}) Bad nvla multihash, {} != {}".format(url, nvla_resource["multihash"], m))

    # download thumbnail for visual sanity check
    for k in dirs:
      translate = translateDecentralizedUrl(onchain["721"][cfg["policy_id"]][asset_id]["image"])
      url = translate[0]
      t = translate[1]
      file_path = os.path.join(dirs[k], row["nanoid"], "thumbnail.jpg")
      if not os.path.exists(file_path):
        urllib.request.urlretrieve(url, file_path)

    # make resource subdirectory
    for k in dirs:
      p = os.path.join(dirs[k], row["nanoid"], "resource")
      if not os.path.isdir(p):
        os.mkdir(p)

    # load novellia resource
    with open(os.path.join(static_dir, row["nanoid"], "nvla.json"), 'r') as f:
      nvla = json.load(f)

    # download and verify each character resource
    for r in nvla["details"]["resource"]:
      for u in r["url"]:
        translate = translateDecentralizedUrl(u)
        url = translate[0]
        t = translate[1]
        file_name = get_filename_from_resource(r)
        file_path = os.path.join(dirs[t], row["nanoid"], "resource", file_name)
        if not os.path.exists(file_path):
          urllib.request.urlretrieve(url, file_path)
        m = get_ipfs_multihash(file_path)
        if m != r["multihash"]:
          raise ValueError("({}) Bad resource multihash, {} != {}".format(url, r["multihash"], m))

if __name__ == "__main__":
  main()
