import pandas as pd
import os
from pathlib import Path
import yaml
import subprocess
import numpy as np
import json

script_dir = os.path.dirname(os.path.realpath(__file__))

out_dir = os.path.join(script_dir, "../out")
data_dir = os.path.join(script_dir, "../data")
index_dir = os.path.join(script_dir, "../index")
static_dir = os.path.join(script_dir, "../static")

org_csv = os.path.join(data_dir, "organization.csv")
mkt_csv = os.path.join(data_dir, "market.csv")
chr_csv = os.path.join(data_dir, "characters_master.csv")
bundles_csv = os.path.join(data_dir, "bundles.csv")

static_csv = os.path.join(index_dir, "index.csv")
config_dir = os.path.join(script_dir, "../config")
cfg_yaml = os.path.join(config_dir, "config.yaml")

def mime_type_from_file_path(file_path):
  _, file_extension = os.path.splitext(file_path)
  extensionToMimeType = {
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".mp4": "video/mp4",
    ".json": "application/json",
  }
  try:
    return extensionToMimeType[file_extension]
  except Exception as e:
    raise ValueError("Unknown MimeType for file {}, detected extension {}".format(file_path, file_extension))

def list_to_postgres_text(l):
  if np.nan in l or not l or len(l) == 0:
    return ''
  text = '{'
  for i in range(len(l)):
    text += str(l[i])
    if i < len(l) - 1:
      text += ','
  text += '}'
  return text

def get_ipfs_multihash(file_path):
  # requires that script caller has IPFS installed
  result = subprocess.run(['ipfs', 'add', file_path, '--only-hash', '-q'], stdout=subprocess.PIPE)
  return result.stdout.decode('utf-8').strip()

if __name__ == "__main__":
  # read export configuration
  print("reading export config from {}".format(cfg_yaml))
  with open(cfg_yaml, 'r') as cfg_file:
    cfg = yaml.full_load(cfg_file)
  print(cfg)

  # read static serving index
  print("reading static serving index from {}".format(static_csv))
  static_df = pd.read_csv(static_csv)
  print(static_df.head())

  # read organizations
  print("reading organizations from {}".format(org_csv))
  org_df = pd.read_csv(org_csv)
  print(org_df.head())
  if cfg['organization_id'] not in org_df['organization_id'].tolist():
    raise ValueError("organization_id not registered")

  # read markets
  print("reading markets from {}".format(mkt_csv))
  mkt_df = pd.read_csv(mkt_csv)
  print(mkt_df.head())
  if cfg['market_id'] not in mkt_df['market_id'].tolist():
    raise ValueError("market_id not registered")

  # read master character list
  print("reading master character list from {}".format(chr_csv))
  chr_df = pd.read_csv(chr_csv)
  chr_df = chr_df.replace(np.nan, '', regex=True)
  print(chr_df.head())

  # read bundles list
  print("reading bundles list from {}".format(bundles_csv))
  bundles_df = pd.read_csv(bundles_csv)
  bundles_df = bundles_df.replace(np.nan, '', regex=True)
  print(bundles_df.head())

  # create dataframes according to schema
  native_token_df = pd.DataFrame(columns=[
    'native_token_id',
    'policy_id',
    'asset_id'
  ])
  product_df = pd.DataFrame(columns=[
    'product_id',
    'product_name',
    'organization_id',
    'market_id',
    'price_currency_id',
    'price_unit_amount',
    'max_order_size',
    'date_listed',
    'date_available',
    'native_token_id',
  ])
  commission_df = pd.DataFrame(columns=[
    'product_id',
    'recipient_name',
    'recipient_address',
    'commission_percent',
  ])
  remote_resource_df = pd.DataFrame(columns=[
    'product_id',
    'resource_id',
    'resource_description',
    'priority',
    'multihash',
    'hash_source_type',
    'resource_urls',
    'content_type',
  ])
  product_detail_df = pd.DataFrame(columns=[
    'product_id',
    'copyright',
    'publisher',
    'product_version',
    'id',
    'tags',
    'description_short',
    'description_long',
    'stock_available',
    'total_supply',
  ])
  product_attribution_df = pd.DataFrame(columns=[
    'product_id',
    'author_name',
    'author_urls',
    'work_attributed',
  ])

  # generate data to insert to DB
  for index, row in chr_df.iterrows():
    if index + 1 not in cfg['include_cards']:
      continue
    # insert native token
    # generate asset_id
    # - replace dashes with spaces
    # - capitalize first letter of each word
    # - strip whitespace
    asset_id = row['product_name']
    asset_id = asset_id.replace('-', ' ').title().replace(' ', '')
    native_token = {
      'native_token_id': "{}.{}".format(cfg['policy_id'], asset_id),
      'policy_id': cfg['policy_id'],
      'asset_id': asset_id,
    }
    native_token_df = native_token_df.append(native_token, ignore_index=True)

    # determine price from rarity
    if row['card_rarity'] == 'Rare':
      price = cfg['price_rare']
    elif row['card_rarity'] == 'Kinda Rare':
      price = cfg['price_kinda_rare']
    elif row['card_rarity'] == 'Not That Rare':
      price = cfg['price_not_that_rare']

    # insert product
    product = {
      'product_id': row['product_id'],
      'product_name': row['product_name'],
      'organization_id': cfg['organization_id'],
      'market_id': cfg['market_id'],
      'price_currency_id': cfg['price_currency'],
      'price_unit_amount': price,
      'max_order_size': cfg['max_order_size'],
      'date_listed': row['date_listed'],
      'date_available': row['date_available'],
      'native_token_id': native_token['native_token_id'],
    }
    product_df = product_df.append(product, ignore_index=True)

    # insert commission
    commission = {
      'product_id': row['product_id'],
      'recipient_name': cfg['commission_name'],
      'recipient_address': cfg['commission_address'],
      'commission_percent': cfg['commission_percent'],
    }
    commission_df = commission_df.append(commission, ignore_index=True)

    # insert remote resources
    # TODO: learn how to use Pandas because this query is embarrasing
    nanoid = static_df[static_df['product_id'] == row['product_id']]['nanoid'].tolist()[0]
    static_base_url = "{}/{}".format(cfg['static_host'], nanoid)
    static_resource_base_url = "{}/{}".format(static_base_url, 'resource')
    static_base_path = os.path.join(static_dir, nanoid)
    static_resource_base_path = os.path.join(static_base_path, 'resource')

    remote_resource_list = []
    card_low_multihash = ""
    for r in ['card_low.jpg', 'card.png', 'artwork_low.jpg', 'artwork.png', 'video.mp4', 'character.json']:
      r_path = Path(os.path.join(static_resource_base_path, r))
      if not r_path.is_file():
        continue

      ipfs_multihash = get_ipfs_multihash(r_path)
      if r == 'card_low.jpg':
        card_low_multihash = ipfs_multihash

      resource_urls = []
      # Static Hosting
      resource_urls.append("{}/{}".format(static_resource_base_url, r))
      # IPFS
      resource_urls.append("ipfs://{}".format(ipfs_multihash))
      # Sia / Skynet
      try:
        sia_url_mapping = {
          'card_low.jpg': row['card_low_sia'],
          'card.png': row['card_high_sia'],
          'artwork_low.jpg': row['artwork_low_sia'],
          'artwork.png': row['artwork_high_sia'],
          'video.mp4': row['video_sia'],
          'character.json': row['character_sia'],
        }
        sia_url = sia_url_mapping[r]
        if len(sia_url):
          resource_urls.append(sia_url)
      except:
        pass

      resource_mapping = {
        'card_low.jpg': ['Card', 'Low Resolution Card', 0],
        'card.png': ['Card', 'High Resolution Card', 1],
        'artwork_low.jpg': ['Artwork', 'Low Resolution Artwork', 0],
        'artwork.png': ['Artwork', 'High Resolution Artwork', 1],
        'video.mp4': ['Video', 'Video of artwork creation process', 0],
        'character.json': ['OccultaNovelliaCharacter', 'Occulta Novellia collectible character information such as stats and moves', 0],
      }
      r_details = resource_mapping[r]

      remote_resource = {
        'product_id': row['product_id'],
        'resource_id': r_details[0],
        'resource_description': r_details[1],
        'priority': r_details[2],
        'multihash': ipfs_multihash,
        # we are using a multihash generated from IPFS (which uses unixfs)
        'hash_source_type': "ipfs",
        'resource_urls': list_to_postgres_text(resource_urls),
        'content_type': mime_type_from_file_path(r_path),
      }
      remote_resource_df = remote_resource_df.append(remote_resource, ignore_index=True)
      remote_resource_list.append(remote_resource)
      remote_resource_list[-1]['resource_urls'] = resource_urls

    # insert product detail
    description_with_lore = "# Product\n\n{}\n\n# Character Lore\n\n{}".format(cfg['description_long'], row['lore'])
    product_detail = {
      'product_id': row['product_id'],
      'copyright': cfg['copyright'],
      'publisher': list_to_postgres_text(cfg['publisher']),
      'product_version': cfg['product_version'],
      'id': row['card_number'],
      'tags': list_to_postgres_text(cfg['tags']),
      'description_short': cfg['description_short'],
      'description_long': description_with_lore,
      #'stock_available': ,
      #'total_supply': ,
    }
    product_detail_df = product_detail_df.append(product_detail, ignore_index=True)

    # insert product attribution
    if row['author_name']:
      product_attribution = {
        'product_id': row['product_id'],
        'author_name': row['author_name'],
        'author_urls': list_to_postgres_text([row['author_url']]),
        'work_attributed': "{} Illustration".format(row['product_name']),
      }
      product_attribution_df = product_attribution_df.append(product_attribution, ignore_index=True)

    # write novellia resource
    nvla_resource_json = {
      "novellia_version": cfg["novellia_version"],
      "metadata": {
        "copyright": cfg['copyright'],
        "publisher": cfg["publisher"],
        "version": cfg["product_version"],
        "extension": cfg["extension"],
      },
      "token": {
        "policy_id": cfg["policy_id"],
        "asset_id": asset_id,
      },
      "details": {
        "name": row['product_name'],
        "description": {
          "short": cfg['description_short'],
          "long": cfg['description_long']
        },
        "tags": cfg["tags"],
        "commission": {
          'name': cfg['commission_name'],
          'address': cfg['commission_address'],
          'percent': cfg['commission_percent'],
        },
        "resource": [],
      }
    }
    for r in remote_resource_list:
      nvla_resource_json["details"]["resource"].append({
        "resource_id": r["resource_id"],
        "description": r["resource_description"],
        "priority": r["priority"],
        "multihash": r["multihash"],
        "hash_source_type": r["hash_source_type"],
        "url": r["resource_urls"],
        "content_type": r["content_type"],
      })
    nvla_resource_path = os.path.join(static_base_path,  "nvla.json")
    with open(nvla_resource_path, 'w') as nvla_file:
      json.dump(nvla_resource_json, nvla_file, indent=2)

    # write on-chain metadata
    nvla_resource_multihash = get_ipfs_multihash(nvla_resource_path)
    nvla_resource = {
      "resource_id": "Novellia",
      "description": "Off-chain Novellia extended metadata",
      "priority": 0,
      "multihash": nvla_resource_multihash,
      "hash_source_type": "ipfs",
      "url": [
        "{}/{}".format(static_base_url, "nvla.json"),
        "ipfs://{}".format(nvla_resource_multihash),
        row['novellia_sia'],
      ],
      "content_type": "application/json",
    }
    os.path.join(static_base_path,  "onchain.json")
    onchain_resource_path = os.path.join(static_base_path,  "onchain.json")
    if card_low_multihash == "":
      raise ValueError("card_low_multihash is empty for {}".format(row["product_name"]))
    onchain_resource = {
      "721": {
        "copyright": cfg['copyright'],
        "publisher": cfg["publisher"],
        "version": cfg["product_version"],
        "extension": cfg["extension"],
        cfg["policy_id"]: {
          asset_id: {
            "id": row['card_number'],
            "name": row['product_name'],
            "description": {
              "short": cfg['description_short'],
              "long": cfg['description_long_metadata']
            },
            "tags": cfg["tags"],
            "image": "ipfs://{}".format(card_low_multihash),
            "resource": [nvla_resource],

          }
        }
      },
    }
    with open(onchain_resource_path, 'w') as onchain_file:
      json.dump(onchain_resource, onchain_file, indent=2)

  # add bundles
  for index, row in bundles_df.iterrows():
    # insert product
    product = {
      'product_id': row['product_id'],
      'product_name': row['product_name'],
      'organization_id': cfg['organization_id'],
      'market_id': cfg['market_id'],
      'price_currency_id': cfg['price_currency'],
      'price_unit_amount': row['price'],
      'max_order_size': cfg['max_order_size'],
      'date_listed': row['date_listed'],
      'date_available': row['date_available'],
    }
    product_df = product_df.append(product, ignore_index=True)

    # insert remote resources
    # TODO: learn how to use Pandas because this query is embarrasing
    nanoid = static_df[static_df['product_id'] == row['product_id']]['nanoid'].tolist()[0]
    static_base_url = "{}/{}".format(cfg['static_host'], nanoid)
    static_resource_base_url = "{}/{}".format(static_base_url, 'resource')
    static_base_path = os.path.join(static_dir, nanoid)
    static_resource_base_path = os.path.join(static_base_path, 'resource')

    for r in ['card_low.jpg', 'card.png']:
      r_path = Path(os.path.join(static_resource_base_path, r))
      if not r_path.is_file():
        continue

      ipfs_multihash = get_ipfs_multihash(r_path)

      resource_urls = []
      # Static Hosting
      resource_urls.append("{}/{}".format(static_resource_base_url, r))
      # IPFS
      resource_urls.append("ipfs://{}".format(ipfs_multihash))
      resource_mapping = {
        'card_low.jpg': ['Card', 'Low Resolution Card', 0],
        'card.png': ['Card', 'High Resolution Card', 1],
      }
      r_details = resource_mapping[r]

      remote_resource = {
        'product_id': row['product_id'],
        'resource_id': r_details[0],
        'resource_description': r_details[1],
        'priority': r_details[2],
        'multihash': ipfs_multihash,
        # we are using a multihash generated from IPFS (which uses unixfs)
        'hash_source_type': "ipfs",
        'resource_urls': list_to_postgres_text(resource_urls),
        'content_type': mime_type_from_file_path(r_path),
      }
      remote_resource_df = remote_resource_df.append(remote_resource, ignore_index=True)

    product_detail = {
      'product_id': row['product_id'],
      'copyright': cfg['copyright'],
      'publisher': list_to_postgres_text(cfg['publisher']),
      'product_version': cfg['product_version'],
      'tags': list_to_postgres_text(cfg['tags_bundle']),
      'description_short': cfg['description_short_bundle'],
      'description_long': row['description'],
    }
    product_detail_df = product_detail_df.append(product_detail, ignore_index=True)

  # write CSVs
  native_token_df.to_csv(os.path.join(out_dir, "native_token.csv"), index=False)
  product_df.to_csv(os.path.join(out_dir, "product.csv"), index=False)
  commission_df.to_csv(os.path.join(out_dir, "commission.csv"), index=False)
  remote_resource_df.to_csv(os.path.join(out_dir, "remote_resource.csv"), index=False)
  product_detail_df.to_csv(os.path.join(out_dir, "product_detail.csv"), index=False)
  product_attribution_df.to_csv(os.path.join(out_dir, "product_attribution.csv"), index=False)
