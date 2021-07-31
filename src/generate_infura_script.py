import os
import subprocess

script_dir = os.path.dirname(os.path.realpath(__file__))
static_dir = os.path.join(script_dir, "../static")

def get_ipfs_multihash(file_path):
  # requires that script caller has IPFS installed
  result = subprocess.run(['ipfs', 'add', file_path, '--only-hash', '-q'], stdout=subprocess.PIPE)
  return result.stdout.decode('utf-8').strip()


with open(os.path.join(script_dir, "infura_deploy_pins.sh"), 'w') as f:
  f.write("#!/usr/bin/env bash\n\n")
  for path, subdirs, files in os.walk(static_dir):
    for name in files:
      if name == "card_low.jpg":
        abs_path = os.path.join(path, name)
        
        multihash = get_ipfs_multihash(abs_path)

        c = "# {}\n".format(abs_path)
        a = "curl -X POST -F \"file=@{}\" \"https://ipfs.infura.io:5001/api/v0/add?progress=true\"\n".format(abs_path)
        l = "curl -X POST \"https://ipfs.infura.io:5001/api/v0/pin/add?arg={}&progress=true\"\n\n".format(multihash)

        f.write(c)
        f.write(a)
        f.write(l)
