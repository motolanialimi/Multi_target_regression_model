def load_data(file_path):
    """
    Loads data from a CSV file and saves it as a pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.
    """
    try:
        data = pd.read_csv(file_path)
        print("Data loaded successfully.")
        return data
    except FileNotFoundError:
        print("File not found. Please check the file path.")
    except pd.errors.EmptyDataError:
        print("No data found in the file.")
    except pd.errors.ParserError:
        print("Error parsing the file.")
    except Exception as e:
        print(f"An error occurred: {e}")

def optimize_memory(df):
    """
    Optimize memory usage of a DataFrame by downcasting numeric types.
    """
    for col in df.select_dtypes(include=['int', 'float']).columns:
        df[col] = pd.to_numeric(df[col], downcast='unsigned')
    return df

def combined_join(datasets, join_key, join_type='inner'):
    """
    Combines multiple datasets using a specified join type on a single join key.

    Args:
        datasets (list of pd.DataFrame): List of DataFrames to join.
        join_key (str): The key to join on for each DataFrame.
        join_type (str): Type of join to perform. Options are 'inner', 'outer', 'left', 'right'.
                         Default is 'inner'.

    Returns:
        pd.DataFrame: The combined DataFrame after joining.
    """
    if len(datasets) < 2:
        raise ValueError("At least two datasets are required.")
    
    # Optimize memory usage for each DataFrame
    optimized_datasets = [optimize_memory(df) for df in datasets]

    combined_data = optimized_datasets[0]
    for i in range(1, len(optimized_datasets)):
        suffixes = (f'_left_{i}', f'_right_{i}')
        try:
            combined_data = combined_data.merge(optimized_datasets[i], on=join_key, how=join_type, suffixes=suffixes)
        except pd.errors.MergeError as e:
            print(f"MergeError: {e}")
            break
        except MemoryError:
            print("MemoryError: Unable to allocate memory. Try reducing the chunk size.")
            break

    return combined_data
        
def clean_data(df, drop_columns=None, drop_rows_threshold=0.5):
    """
    Enhanced data cleaning function for a DataFrame, focusing on removing duplicates,
    trimming whitespace, dropping specified columns, and dropping rows based on a threshold
    of missing values.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be cleaned.
    - drop_columns (list of str): List of column names to drop.
    - drop_rows_threshold (float): Threshold of missing values percentage to drop a row.

    Returns:
    - pd.DataFrame: The cleaned DataFrame
    """
    # Remove duplicates
    #df = df.drop_duplicates()
    #print("Duplicates removed.")

    # Trim whitespace from string columns
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    print("Whitespace trimmed from string columns.")

    # Drop specified columns
    if drop_columns:
        df = df.drop(columns=drop_columns, errors='ignore')  # Using 'errors=ignore' to avoid errors if columns don't exist
        print(f"Columns dropped: {drop_columns}")
    
    # Drop rows based on threshold of missing values
    df = df.dropna(thresh=int((1 - drop_rows_threshold) * df.shape[1]))
    print(f"Rows dropped based on missing values threshold.")
        
    return df
        
def write_excel_and_create_folder(data, folder_name, file_name):
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        
    # Define the full path for the Excel file
    file_path = os.path.join(folder_name, file_name)   
    
    # Create a dataframe from the data provided
    df = pd.DataFrame(data) 
    
    # Write the Dataframe to an Excel file 
    df.to_excel(file_path, index= False)
    
    # Notify the user
    print(f'Excel file saved at {folder_name}')
    
    
def drop_duplicate_columns(df):
    duplicates= set()
    # Compare each column with every other column
    for i in range(df.shape[1]):
        for j in range(i+1, df.shape[1]):
            if df.iloc[:, i].equals(df.iloc[:,j]):
                duplicates.add(df.columns[j])
    df= df.drop(columns=duplicates)
    return df    
