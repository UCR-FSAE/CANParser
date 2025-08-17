import cantools
import pandas as pd
import csv
from datetime import datetime

class CANParser:
    def __init__(self, dbc_file_path):
        self.db = cantools.database.load_file(dbc_file_path)
        
    def parse_csv(self, input_csv_path, output_csv_path):        
        # Read  CSV
        csv_in = pd.read_csv(input_csv_path)
        
        # Initialize list to store decoded messages
        decoded_messages = []
        
        for index, row in csv_in.iterrows():
            try:
                # Extract CAN frame data
                timestamp = row['timestamp']  
                can_id = int(row['can_id'], 16) if isinstance(row['can_id'], str) else row['can_id']
                data = bytes.fromhex(row['data'])  # Convert hex string to bytes
                
                #decode the message using DBC
                try:
                    message = self.db.get_message_by_frame_id(can_id)
                    decoded_data = message.decode(data)
                    
                    # Create row with timestamp, message name, and all signals
                    decoded_row = {
                        'timestamp': timestamp,
                        'message_name': message.name,
                        'can_id': hex(can_id)
                    }
                    ``
                    # Add all decoded signals to the row
                    decoded_row.update(decoded_data)
                    decoded_messages.append(decoded_row)
                    
                except KeyError:
                    print(f"Unknown message ID: {hex(can_id)}")
                    continue
                    
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        # Convert to DataFrame and save
        if decoded_messages:
            output_df = pd.DataFrame(decoded_messages)
            output_df.to_csv(output_csv_path, index=False)
            print(f"Saved {len(decoded_messages)} decoded messages to {output_csv_path}")
        else:
            print("No messages were successfully decoded")

def main():
    parser = CANParser('test.dbc') 
    parser.parse_csv('test.csv', 'decoded_can_data.csv')

if __name__ == "__main__":
    main()
