import pandas as pd
import re
import os

def parse_message_logs(log_data: str) -> pd.DataFrame:
    """
    Parses the structured message log data into a horizontal DataFrame format.

    Args:
        log_data: A string containing the entire combined message log.

    Returns:
        A pandas DataFrame with the extracted data in the requested horizontal format.
    """
    # Define the regular expression pattern to capture a single message block.
    # re.VERBOSE allows for comments and multiline patterns.
    # re.DOTALL allows the '.' to match newlines, crucial for MESSAGE CONTENT.
    pattern = re.compile(r"""
        MESSAGE\sRECEIVED:\s(?P<ReceivedTime>.*)\n
        ={70,}\n
        Channel\sID:\s(?P<ChannelID>.*)\n
        Channel\sName:\s(?P<ChannelName>.*)\n
        Message\sID:\s(?P<MessageID>.*)\n
        Timestamp:\s(?P<Timestamp>.*)\n
        \n
        ---\sMESSAGE\sCONTENT\s---\s*
        (?P<MessageContent>.*?)\n
        ={70,}\n
    """, re.VERBOSE | re.DOTALL)

    parsed_records = []

    # Iterate over all matches found in the log data
    for match in pattern.finditer(log_data):
        record = match.groupdict()

        # Clean up the Message Content: remove surrounding whitespace
        # and replace internal newlines/excess space with a single space for horizontal display.
        message_content = record['MessageContent'].strip()
        message_content = re.sub(r'\s*\n\s*', ' ', message_content)

        # Create the final, cleaned record dictionary
        cleaned_record = {
            'MESSAGE RECEIVED': record['ReceivedTime'].strip(),
            'Channel ID': record['ChannelID'].strip(),
            'Channel Name': record['ChannelName'].strip(),
            'Message ID': record['MessageID'].strip(),
            'Timestamp': record['Timestamp'].strip(),
            'MESSAGE CONTENT': message_content,
        }
        parsed_records.append(cleaned_record)

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(parsed_records)

    # Ensure the columns are in the requested order
    column_order = [
        'MESSAGE RECEIVED', 'Channel ID', 'Channel Name',
        'Message ID', 'Timestamp', 'MESSAGE CONTENT'
    ]
    if not df.empty:
        return df[column_order]
    else:
        return df

# --- Main Execution ---

file_names = ['messages_2025-12-12.txt', 'messages_2025-12-10.txt']
combined_log_content = ""

# 1. Read and combine all file contents
for file_name in file_names:
    try:
        # Use 'r' for read mode and 'utf-8' for broad character support (emojis, etc.)
        with open(file_name, 'r', encoding='utf-8') as f:
            combined_log_content += f.read() + "\n\n"
        print(f"Successfully read: {file_name}")
    except FileNotFoundError:
        print(f"Error: File not found: {file_name}. Skipping.")
    except Exception as e:
        print(f"An error occurred while reading {file_name}: {e}")

# 2. Parse the combined data
if combined_log_content.strip():
    final_df = parse_message_logs(combined_log_content)

    print(f"\nSuccessfully parsed {len(final_df)} total messages.")

    # 3. Display the results
    print("\n--- Combined Log Data (First 10 Records) ---")
    # Display without the pandas index for a clean record format
    print(final_df.head(10).to_string(index=False))

    # Optional: Save to a CSV file
    # final_df.to_csv('parsed_messages.csv', index=False, encoding='utf-8')
    # print("\nData saved to 'parsed_messages.csv'")

else:
    print("\nNo log content was read. Please ensure files are in the same directory.")