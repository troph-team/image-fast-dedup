import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import os

def getFilePathArr(num):
  prefix = "/home/ubuntu/dedup/train_images/image_"
  paths = []
  for i in range(1, num + 1):
    formatted_num = '{:05d}'.format(i)
    file_path = prefix + formatted_num + '.jpg'
    paths.append(file_path)
  return paths

def get_input_file(folder_path):
  paths = []
  for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    paths.append(file_path)
  return paths

def write_input_file():
  # fileArr = getFilePathArr(1000)
  fileArr = get_input_file("/home/ubuntu/image-fast-dedup/train_images")
  print(len(fileArr))
  data = {
    'file_path': fileArr
  }
  df = pd.DataFrame(data)

  table = pa.Table.from_pandas(df)

  file_path = '/home/ubuntu/image-fast-dedup/parquet_files/test_input.parquet'  # 替换为你想要保存的 Parquet 文件路径

  pq.write_table(table, file_path)
  print('finished')
  
def read_parquet_file(input_file):
  table = pq.read_table(input_file)
  df = table.to_pandas()
  
if __name__ == "__main__":
  write_input_file()
  # 30k - 198s