from kakaimagededup.methods import PHash
import pandas as pd

def dedup_using_imagedup(df: pd.DataFrame):
  phasher = PHash()
  encodings = {}
  for path in df['file_path'].tolist():
    encoding = phasher.encode_image(path)
    encodings[path] = encoding
  duplicates = phasher.find_duplicates_to_remove(encoding_map=encodings)
  return duplicates

def dedup_with_hashfile_using_imagedup(hash_file: str, search_method='hashblock'):
  phasher = PHash()
  encodings = {}
  df = pd.read_csv(hash_file, header=None, names=['file_path', 'hash_value'])
  for index, path in enumerate(df['file_path'].tolist()):
    encodings[path] = df['hash_value'][index]
  duplicates = phasher.find_duplicates_to_remove(encoding_map=encodings, search_method=search_method, no_cython=True)
  return df['file_path'], duplicates

def write_result(image_paths, duplicate_list, dest_file):
    is_dups = []
    for path in image_paths:
      if path in duplicate_list:
        is_dups.append(True)
      else :
        is_dups.append(False)
    data_list = {
      'file_path': image_paths,
      'is_dup': is_dups
    }
    new_df = pd.DataFrame(data_list)
    new_df.to_csv(dest_file, index=False,sep=',', header=False)
    