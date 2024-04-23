import json
from tqdm import tqdm
import re

def preprocess(record):
    keys_to_keep = ['title', 'also_buy' ,'price' , 'brand' ]
    record = {key: record[key] for key in keys_to_keep if key in record}
    return record

def sample_and_limit(input_file, target_size_gb, limit, output_file):
    target_size_bytes = target_size_gb * 1024**3
    current_size_bytes = 0
    lines_written = 0
    sampled_data = []
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line in tqdm(infile, desc="Processing", unit=" lines"):
            if lines_written >= limit:
                break
            
            record = json.loads(line)
            record = preprocess(record)
            
            if 'also_buy' in record and record['also_buy']:
                sampled_data.append(record)
                current_size_bytes += len(json.dumps(record).encode('utf-8'))
                lines_written += 1
                
                if current_size_bytes >= target_size_bytes:
                    break
    
    print(f"Finished sampling. Output size: {current_size_bytes / 1024**3:.2f} GB")
    

    with open(output_file, 'w', encoding='utf-8') as outfile:
        for record in sampled_data:
            outfile.write(json.dumps(record) + '\n')

    print(f"Sampled data saved to {output_file}")


sample_and_limit("e:\SEMESTER 4\Big Data\A4\data.json", target_size_gb=1, limit=100000, output_file="outputt.json")
