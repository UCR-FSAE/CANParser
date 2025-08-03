import cantools
import csv
import pandas as pd

def decode_can(csv_path, dbc_path, output_path):
    # load dbc
    db = cantools.database.load_file(dbc_path)

    # load output_csv
    with open(output_path, 'w') as outfile:
        fieldnames = ['timestamp', 'can_id', 'name', 'raw_value', 'value', 'unit']
        writer = None

        # decode all frames
        with open(csv_path, 'r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                try:
                    # line converts CAN ID form CSV to int
                    can_id = int(row["ID"], 16) if "0x" in row["ID"] else int(row["ID"])
                    # converts hex string to bytes
                    data = bytes.fromhex(row["Data"].replace(" ", ""))
                    # converts microseconds to seconds
                    timestamp = int(row["Time(us)"]) / 1_000_000  # convert us → s

                    # decodes data using dbc
                    msg = db.get_message_by_frame_id(can_id)
                    decoded = msg.decode(data)

                    # initializes the CSV writer on the first message
                    if writer is None:
                        all_signals = list(decoded.keys())
                        fieldnames.extend(all_signals)
                        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                        writer.writeheader()

                    # creates output row, writes to CSV
                    output_row = {'Timestamp (s)': timestamp, 'CAN ID': hex(can_id)}
                    output_row.update(decoded)
                    writer.writerow(output_row)


                except Exception as e:
                    # update later with error handling
                    continue
                
    return output_path