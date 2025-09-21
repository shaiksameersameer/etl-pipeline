import pandas as pd
import boto3

# Step 1: Clean Data
def clean_data(df):
    df = df.dropna()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip().str.lower()
    return df

# Step 2: Transform Data
def transform_data(df):
    grouped = df.groupby("Branch", as_index=False)["Marks"].mean()
    grouped.rename(columns={"Marks": "AvgMarks"}, inplace=True)
    return grouped

# Step 3: Save CSV
def save_data(df, filename):
    df.to_csv(filename, index=False)
    print(f"✅ Saved {filename}")

# Step 4: Upload to S3
def upload_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client("s3")
    if object_name is None:
        object_name = file_name
    s3.upload_file(file_name, bucket, object_name)
    print(f"☁️ Uploaded {file_name} to s3://{bucket}/{object_name}")

if __name__ == "__main__":
    # Input CSV
    df = pd.read_csv("students.csv")
    
    # Clean
    df_clean = clean_data(df)

    # Transform
    df_transformed = transform_data(df_clean)

    # Save
    save_data(df_transformed, "transformed.csv")

    # Upload
     upload_to_s3("transformed.csv", "sameer-data-2025")
