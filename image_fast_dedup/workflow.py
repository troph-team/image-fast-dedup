import argparse
import os
import queue as Queue
import threading
from concurrent import futures
from concurrent.futures import as_completed
import time

import pandas as pd
import pyarrow.parquet as pq
from PIL import Image
from tqdm.auto import tqdm

# from upscale.scunet_model import UpscalerScuNET
from dedup.imagedup import *
from dedup.fastdup import *

lock = threading.Lock()


class Arguments:
    input: str
    hash_file: str
    column: str
    dest_dir: str
    model: str
    # max_pixels: float
    # min_pixels: float


args = Arguments()


class ThreadPoolExecutorWithQueueSizeLimit(futures.ThreadPoolExecutor):
    def __init__(self, maxsize=50, *args, **kwargs):
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = Queue.Queue(maxsize=maxsize)

def dedup_images(df: pd.DataFrame, model: str):
  os.makedirs(args.dest_dir, exist_ok=True)
  start_time = time.perf_counter()
  
  dest_file = os.path.join(
        args.dest_dir,
        f"dedup_{model}.csv",
  )
  
  if model == "imagedup":
    # duplicate_list = dedup_using_imagedup(df)
    # write_result(df['file_path'], duplicate_list, dest_file)
    print(" ------- start imagedup -------")
    image_paths, duplicate_list = dedup_with_hashfile_using_imagedup(args.hash_file)
    write_result(image_paths, duplicate_list, dest_file)
  else :
    table = pq.read_table(args.input)
    df = table.to_pandas()
    df.to_csv("./tmp.csv", header=None, index=None)
    res_df = dedup_using_fastdup("./tmp.csv", args.dest_dir)
    res_df.to_csv(dest_file, index=False,sep=',', header=False)

  file_path = "./model_output.txt"  # 替换为你想要保存的文件路径

  with open(file_path, "a") as file:
    file.write(f"model: {model} -finished. consumed time: {time.perf_counter() - start_time}\n")
  print('------- Dedup finished. ---------')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dedup images.")
    parser.add_argument("--input", type=str, help="Input parquet file path")
    parser.add_argument(
        "--hash-file",
        type=str,
        help="generated hash file, for example /opt/ml/input/data/airflow/resized/xxxx/",
    )
    parser.add_argument(
        "--column", type=str, default="image", help="Column name of the image path"
    )
    parser.add_argument(
        "--dest-dir",
        type=str,
        required=True,
        help="destination directory, for example /opt/ml/input/data/airflow/resized/xxxx/",
    )
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="dedup model",
    )
    parser.parse_args(namespace=args)

    assert args.input.endswith(".parquet"), "Input file must be a parquet file."
    data = pd.read_parquet(args.input)
    dedup_images(
      data,
      args.model
    )