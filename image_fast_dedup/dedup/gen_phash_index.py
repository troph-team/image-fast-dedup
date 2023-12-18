from itertools import combinations
import argparse
import os

class Arguments:
    input: str
    # n_thread: int
    dest_path: str
    # hash_func: str

args = Arguments()

parser = argparse.ArgumentParser(description='These is a duplicated images detection program. You need to provide pHashs of all images.')

parser.add_argument('--input', required=True, help='The file path of pHashs for all images, where the first line is "pHash imageName"')

parser.add_argument('--dest-path', default='', help='The output path stores the images phashs index base, which is a json file. It is usually used to be queried if an image is duplicated in the image base')

parser.parse_args(namespace=args)

if args.dest_path == '':
    args.dest_path = os.path.join(os.path.split(args.input)[0], 'hash.json')

with open(args.input, 'r') as f:
    s = []
    f.readline()
    for x in f:
        s.append(x.strip().split(' '))

n_split = 4
#Error
#dicts = [{}] * n_split
dicts = [{} for i in range(n_split)]

for i in range(n_split):
    for x in s:
        c_part = x[0][i*int(16/n_split):(i+1)*int(16/n_split)]
        if c_part not in dicts[i]:
            dicts[i][c_part] = []
        assert x[1] not in dicts[i][c_part], x[1]
        dicts[i][c_part].append(x[1])

for i in range(n_split):
    for img_list in dicts[i].values():
        print(i)
        print(img_list)
import json
json.dump(dicts, open(args.dest_path, 'w'))