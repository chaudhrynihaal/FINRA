import pandas as pd
import csv
import sys
import os

def clean_and_align_csv(destination_path):
    """
    Clean corrupted columns and properly align CSV
    """
    print("=" * 70)
    print("CLEANING AND ALIGNING CSV FILE")
    print("=" * 70)
    
    # Create backup
    backup_path = destination_path.replace('.csv', '_backup.csv')
    print(f"\n[1] Creating backup...")
    import shutil
    shutil.copy2(destination_path, backup_path)
    print(f"    ✓ Backup created")
    
    # Read the file line by line to fix structure
    print(f"\n[2] Reading and fixing file structure...")
    
    corrected_rows = []
    with open(destination_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # Find the correct headers (remove corrupted ones at the end)
        correct_headers = []
        for h in headers:
            # Stop when we hit corrupted data that looks like JSON or dict
            if h.startswith('[{') or h.startswith('{') or h == 'SMRI' or 'SMRI' in str(h):
                break
            correct_headers.append(h)
        
        print(f"    ✓ Original columns: {len(headers)}")
        print(f"    ✓ Clean columns: {len(correct_headers)}")
        print(f"    ✓ First 5 clean headers: {correct_headers[:5]}")
        print(f"    ✓ Last 5 clean headers: {correct_headers[-5:]}")
        
        # Process each row
        for row_num, row in enumerate(reader):
            # Take only the clean columns
            clean_row = row[:len(correct_headers)]
            corrected_rows.append(clean_row)
            
            if (row_num + 1) % 100000 == 0:
                print(f"    ... Processed {row_num + 1} rows")
    
    print(f"    ✓ Total rows processed: {len(corrected_rows)}")
    
    # Create DataFrame with clean headers
    print(f"\n[3] Creating cleaned DataFrame...")
    df = pd.DataFrame(corrected_rows, columns=correct_headers)
    
    # Convert data types (optional - keeps everything as string to avoid warnings)
    print(f"    ✓ DataFrame shape: {df.shape}")
    
    # Save to temporary file
    temp_path = destination_path.replace('.csv', '_temp.csv')
    print(f"\n[4] Saving cleaned file...")
    df.to_csv(temp_path, index=False)
    print(f"    ✓ Saved to temporary file")
    
    # Replace original
    print(f"\n[5] Replacing original file...")
    if os.path.exists(destination_path):
        os.remove(destination_path)
    os.rename(temp_path, destination_path)
    print(f"    ✓ Original file replaced")
    
    # Verification
    print(f"\n[6] VERIFICATION - Reading first 5 rows:")
    df_check = pd.read_csv(destination_path, nrows=5)
    print(df_check.head())
    
    print(f"\n[7] Column information:")
    print(f"    ✓ Total columns: {len(df_check.columns)}")
    print(f"    ✓ First 5 columns: {list(df_check.columns[:5])}")
    print(f"    ✓ Last 5 columns: {list(df_check.columns[-5:])}")
    
    print("\n" + "=" * 70)
    print("✓ CSV FILE HAS BEEN CLEANED AND ALIGNED")
    print("=" * 70)
    print(f"\n📊 FINAL SUMMARY:")
    print(f"   • Original had {len(headers)} columns (with {len(headers)-len(correct_headers)} corrupted)")
    print(f"   • Cleaned to {len(correct_headers)} columns")
    print(f"   • Total rows: {len(corrected_rows)}")
    print(f"   • Backup saved as: {backup_path}")
    print(f"\n✓ File is now properly aligned from Column A")
    print(f"✓ All corrupted JSON/dict columns have been removed")

if __name__ == "__main__":
    destination = r"C:\Users\Kavtech\Desktop\FINRAC\Copy of N8N - Project All Records - BrokerCheck - Individual.csv"
    
    print("\n⚠ IMPORTANT: Please close the CSV file if open in any program\n")
    input("Press Enter to continue...")
    
    clean_and_align_csv(destination)