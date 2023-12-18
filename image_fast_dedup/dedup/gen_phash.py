import cv2
import os
import imagehash
from PIL import Image
import argparse
import sys
import numpy as np
import multiprocessing as mp
import pandas as pd

counter = mp.Value('i', 0)

def map(samples):
    a = []
    global counter
    for sample in samples:
        c_hash = imagehash.phash(Image.open(sample))
        # c_hash = imagehash.phash(Image.open(sample)).convert('RGB')
        a.append([c_hash, sample])
        with counter.get_lock():
            counter.value += 1
            if counter.value % 10 == 0:
                print(counter.value)
    return a
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='This script generates phash of all the images.')

  parser.add_argument("--input", type=str, help="Input parquet file path")

  parser.add_argument('--n_thread', default=1, help='The number of threads')

  parser.add_argument(
      "--dest-dir",
      type=str,
      required=True,
      help="destination directory, for example /opt/ml/input/data/airflow/resized/xxxx/",
  )

  parser.parse_args(namespace=args)
  assert args.input.endswith(".parquet"), "Input file must be a parquet file."
  
  data = pd.read_parquet(args.input)
  image_paths = data['file-path']
  
  args.n_thread = 1 if args.n_thread < 1 else args.n_thread
  
  if FLAGS.n_thread == 1:
    split_names = [image_paths]
    # p／rint("split_names".split_names)
  else:
      n = len(image_paths)
      jiange = int(n / args.n_thread)
      split_point = []
      for i in range(1, args.n_thread):
          split_point.append(i*jiange)
      split_names = np.split(image_paths, split_point)
  
  #动态生成多个进程
  processS = mp.Pool(processes=len(split_names))

  c_results = [[] for i in range(len(split_names))]

  queue = mp.Manager().Queue()
  #枚举函数；i表示循环下标；block表示从迭代器获取到的元素
  for i, block in enumerate(split_names):
      c_results[i] = processS.apply_async(func=map, args=(block, ))
  processS.close()
  processS.join()

  s = ['pHash imageName']

  for i, values in enumerate(c_results):
      values = values.get()
      for value in values:
          s.append('\n{} {}'.format(value[0], value[1]))

  dest_path = os.path.join(
        args.dest_dir,
        f"phash_{model}.txt",
  )
  f = open(args.dest_path, 'w')
  f.writelines(s)
  f.close()