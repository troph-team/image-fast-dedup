import fastdup
fastdup.__version__

import os
import pandas as pd
from tqdm.auto import tqdm

THRESHOLD = 0.95

# Modified component function
def get_component_ids_with_high_mean_distance(df, threshold):
    component_ids = df.loc[df["mean_distance"] > threshold, "component_id"].tolist()
    component_ids = list(set(component_ids))
    return component_ids

# Modified file size function
def add_file_size_to_df(df):
    tqdm.pandas(desc="Calculating file sizes")
    df["size_filtering"] = df["filename"].progress_apply(
        lambda x: os.path.getsize(x) if os.path.exists(x) else 0
    )
    return df

# Modified move files function
def mark_files_as_duplicates(
    component_ids: list[int], df: pd.DataFrame, keep: bool = True
) -> pd.DataFrame:
    # Filter dataframe based on the list of component_ids
    filtered_df = df[df["component_id"].isin(component_ids)]

    # Group by 'component_id' and get the row with the maximum 'size_filtering' for each group
    largest_files_df = filtered_df.groupby("component_id").apply(
        lambda x: x.loc[x["size_filtering"].idxmax()]
    )

    # Initialize new columns
    df['duplicate_group'] = None
    df['is_dup'] = False

    for _, row in tqdm(filtered_df.iterrows(), desc="Marking files"):
        component_id = row["component_id"]
        filename = row["filename"]

        # Mark the duplicate_group
        df.loc[df['filename'] == filename, 'duplicate_group'] = component_id

        # If keep is True and the file is the largest in its group, don't mark it as a duplicate
        if keep and filename == largest_files_df.loc[component_id, "filename"]:
            continue

        # Otherwise, mark it as a duplicate
        df.loc[df['filename'] == filename, 'is_dup'] = True

    return df
  
def dedup_using_fastdup(input: str, dest_dir):
  fd = fastdup.create(input_dir=input, work_dir=dest_dir)
  fd.run(overwrite=True, ccthreshold=0.9, threshold=0.8)
  # Component
  connected_components_df, _ = fd.connected_components()
  component_ids = get_component_ids_with_high_mean_distance(
      connected_components_df, threshold=THRESHOLD
  )

  # Add file size to DataFrame
  connected_components_df = add_file_size_to_df(connected_components_df)

  # Mark files as duplicates
  marked_df = mark_files_as_duplicates(component_ids, connected_components_df, keep=True)
  new_df = marked_df[['filename', 'is_dup']]
  print(marked_df)
  print(new_df)
  return new_df