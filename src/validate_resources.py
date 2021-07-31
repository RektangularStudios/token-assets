import requests
import json
import urllib.request
import pandas as pd
import os
from pathlib import Path
from PIL import Image

script_dir = os.path.dirname(os.path.realpath(__file__))
index_dir = os.path.join(script_dir, "../index")

static_og_dir = os.path.join(script_dir, "../static")
static_cdn_dir = os.path.join(script_dir, "../constructed_cdn_static")
static_ipfs_dir = os.path.join(script_dir, "../constructed_ipfs_static")
static_sia_dir = os.path.join(script_dir, "../constructed_sia_static")

static_csv = os.path.join(index_dir, "index.csv")
df = pd.read_csv(static_csv, ",")

mb_to_bytes = 1e6

image_resource_specifications = {
  'card_low.jpg': {
    'width': '1200',
    'height': '1550',
    'max_size': 0.4 * mb_to_bytes,
    'min_size': 0
  },
  'card.png': {
    'width': '2400',
    'height': '3100',
    'max_size': 15 * mb_to_bytes,
    'min_size': 2 * mb_to_bytes
  },
  'artwork_low.jpg': {
    'width': '1920',
    'height': '1080',
    'max_size': 0.4 * mb_to_bytes,
    'min_size': 0
  },
  'artwork.png': {
    'width': '3840',
    'height': '2160',
    'max_size': 15 * mb_to_bytes,
    'min_size': 2 * mb_to_bytes
  },
}

def validate_file_size(filename, r_path):
  if not filename in image_resource_specifications:
    return

  size = os.path.getsize(r_path)
  spec_max_size = image_resource_specifications[filename]["max_size"]
  spec_min_size = image_resource_specifications[filename]["min_size"]
  if size > spec_max_size:
    print("Size too big for {}: actual ({}) > spec ({})".format(filename, size, spec_max_size))
  if size < spec_min_size:
    print("Size too small for {}: actual ({}) > spec ({})".format(filename, size, spec_max_size))

def validate_image_dimensions(filename, r_path):
  if not filename in image_resource_specifications:
    return

  im = Image.open(r_path)
  width, height = im.size
  spec_width = int(image_resource_specifications[filename]["width"])
  spec_height = int(image_resource_specifications[filename]["height"])
  if width != spec_width or height != spec_height:
    print("Dimensions for {} do not match spec: actual ({},{}) != spec ({},{})".format(filename, width, height, spec_width, spec_height))

def main():
  for static_dir in [static_og_dir, static_cdn_dir, static_ipfs_dir, static_sia_dir]:
    print("\nTesting {}\n".format(static_dir))
    for _, row in df.iterrows():
      resource_dir = os.path.join(static_dir, row["nanoid"], "resource")
      print("\n\nValidating {}".format(row["name"]))
      for r in ['card_low.jpg', 'card.png', 'artwork_low.jpg', 'artwork.png', 'video.mp4', 'character.json']:
        r_path = Path(os.path.join(resource_dir, r))
        
        if not r_path.is_file():
          print("Warning: {} does not exist".format(r))
          continue
        
        size = os.path.getsize(r_path)
        if size < 10:
          print("Found empty file: {}".format(r_path))

        validate_file_size(r, r_path)
        validate_image_dimensions(r, r_path)

        if r == 'character.json':
          with open(r_path, 'r') as f:
            j = json.load(f)
            if j["name"] != row["name"]:
              raise ValueError("Mismatched character JSON: {}".format(r_path))

if __name__ == '__main__':
  main()
