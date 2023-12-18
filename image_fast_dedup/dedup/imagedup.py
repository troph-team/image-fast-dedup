from imagededup.methods import PHash
import pandas as pd

def dedup_using_imagedup(df: pd.DataFrame):
  phasher = PHash()
  encodings = {}
  for path in df['file_path'].tolist():
    encoding = phasher.encode_image(path)
    encodings[path] = encoding
  duplicates = phasher.find_duplicates_to_remove(encoding_map=encodings)
  return duplicates

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
    