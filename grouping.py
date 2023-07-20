import pandas as pd
import ast

# Function to convert values to bytes
def convert_to_bytes(value):
    if pd.notnull(value):
        if 'KB' in value:
            return int(float(value.replace('KB', '').strip()) * 1024)
        elif 'MB' in value:
            return int(float(value.replace('MB', '').strip()) * 1024 * 1024)
        elif 'bytes' in value:
            return int(value.replace('bytes', '').strip())
    return None

# Read the CSV file
filepath = "/home/muttineni/PycharmProjects/json tabulation/csv files/chargebee.csv"
df = pd.read_csv(filepath)

# Convert the "childOperators" column to Python objects and extract the first child
df["childOperators"] = df["childOperators"].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else None)
df["firstChild"] = df["childOperators"].apply(lambda x: x[0]["operator"] if isinstance(x, list) and len(x) > 0 else None)

# Create a dictionary of DataFrames for each unique operator
df1_dict = {}
for op in df["operator"].unique():
    df1_dict[op] = df[df["operator"] == op].copy()

# Process each DataFrame separately
for op, dataframe in df1_dict.items():
    print(f"DataFrame for {op}:")
    print(dataframe)
    print()
    # Combine DataFrames into a single DataFrame
    combined_df = pd.concat(df1_dict.values())

# Drop the "childOperators" column
if "childOperators" in combined_df.columns:
    combined_df = combined_df.drop("childOperators", axis=1)

# Convert "read_io_bytes" to bytes
combined_df["read_io_bytes"] = combined_df["read_io_bytes"].apply(convert_to_bytes)
combined_df["memory_consumption"] = combined_df["memory_consumption"].apply(convert_to_bytes)


# Filter the DataFrame and save to a new CSV file
newdf = combined_df[(combined_df['queryId'].notnull()) & (combined_df['operator'] != "SinkOperator")]
querylist = newdf['queryId'].unique()
filtered_df2 = combined_df[combined_df["queryId"].isin(querylist)]
# filtered_df2.to_csv("combined_filtered_data.csv", index=False)

# Extract the first child without suffixes
new_df = pd.DataFrame()
for column in filtered_df2.columns:
    if filtered_df2[column].astype(str).str.contains(r'\d+\.\d+% \(\d+\.\d+ ms\)').any():
        split_data = filtered_df2[column].str.extract(r'(\d+\.\d+)% \((.*?) ms\)')
        split_data.columns = [f'{column}_Percentage', f'{column}_Values']
        new_df = pd.concat([new_df, split_data], axis=1)
    else:
        new_df[column] = filtered_df2[column]

# Store the "firstChild" without suffixes
if "firstChild_Percentage" in new_df.columns and "firstChild_Values" in new_df.columns:
    new_df["firstChild"] = new_df["firstChild_Values"]
    new_df = new_df.drop(["firstChild_Percentage", "firstChild_Values"], axis=1)
    # Delete columns with all NaN values
deleted_columns = filtered_df2.columns[filtered_df2.isnull().all()].tolist()
filtered_df2 = filtered_df2.dropna(axis=1, how='all')

# Print the names of the deleted columns
print("Deleted columns:")
print(deleted_columns)


new_df.to_csv("new_df.csv", index=False)
