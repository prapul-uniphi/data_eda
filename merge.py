import pandas as pd

# Read the two CSV files
file1_path = "chargebee_SHQ.csv"
file2_path = "new_df.csv"

df1 = pd.read_csv(file1_path)
df2 = pd.read_csv(file2_path)

# Merge the two DataFrames based on the different column names "query_id" and "queryId"
merged_df = pd.merge(df1, df2, left_on="query_id", right_on="queryId")

# Drop the duplicate column "queryId" after merging, if needed
merged_df.drop(columns=["queryId"], inplace=True)

# Save the merged DataFrame to a new CSV file
merged_file_path = "complete merged_csv_file.csv"
merged_df.to_csv(merged_file_path, index=False)

