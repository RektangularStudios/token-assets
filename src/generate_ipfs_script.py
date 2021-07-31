import os

script_dir = os.path.dirname(os.path.realpath(__file__))
static_dir = os.path.join(script_dir, "../static")

with open(os.path.join(script_dir, "ipfs_deploy_pins.sh"), 'w') as f:
  f.write("#!/usr/bin/env bash\n\n")
  for path, subdirs, files in os.walk(static_dir):
    for name in files:
      if name != ".gitkeep":
        abs_path = os.path.join(path, name)
        rel_path = os.path.relpath(abs_path, static_dir)
        
        line = "ipfs add {} --pin=true\n".format(rel_path)
        f.write(line)
