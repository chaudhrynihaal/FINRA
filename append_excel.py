import pandas as pd
import sys

def append_excel_files(file1_path, file2_path, output_path):
    """
    Append the contents of the second Excel file to the first Excel file.

    Args:
        file1_path (str): Path to the first Excel file.
        file2_path (str): Path to the second Excel file.
        output_path (str): Path to save the combined Excel file.
    """
    try:
        # Read the Excel files
        df1 = pd.read_excel(file1_path)
        df2 = pd.read_excel(file2_path)

        # Append df2 to df1
        combined_df = pd.concat([df1, df2], ignore_index=True)

        # Save to output file
        combined_df.to_excel(output_path, index=False)

        print(f"Successfully appended {file2_path} to {file1_path} and saved to {output_path}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python append_excel.py <first_file.xlsx> <second_file.xlsx> <output_file.xlsx>")
        print("Example: python append_excel.py data1.xlsx data2.xlsx combined.xlsx")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]
    output = sys.argv[3]

    append_excel_files(file1, file2, output)