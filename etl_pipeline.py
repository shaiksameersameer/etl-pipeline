import pandas as pd
import boto3

def clean_data(df):
    # Drop nulls
    df = df.dropna()
    # Lowercase + strip spaces
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.lower().str.strip()
    return df

def transform_data(df):
    # Group by branch and calculate average marks
    return df.groupby("Branch")["Marks"].mean().reset_index()

def save_data(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"✅ Saved cleaned & transformed CSV to {output_file}")

def upload_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client("s3")
    if object_name is None:
        object_name = file_name
    s3.upload_file(file_name, bucket, object_name)
    print(f"☁️ Uploaded {file_name} to s3://{bucket}/{object_name}")

if __name__ == "__main__":
    # Step 1: Read input CSV
    df = pd.read_csv("students.csv")

    # Step 2: Clean
    df_clean = clean_data(df)

    # Step 3: Transform
    df_transformed = transform_data(df_clean)

    # Step 4: Save
    save_data(df_transformed, "transformed.csv")

    # Step 5: Upload to S3
    upload_to_s3("transformed.csv", "sameer-data-2025")
