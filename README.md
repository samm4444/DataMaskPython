# IrisDataMasker

Mask specific fields from a mySQL database table and export to a separate database/table.

## Installation

1. Download
2. Run
   ```
   pip install -r requirements.txt
   ```
3. Start masking!

## Usage

__Create a config JSON to specify which fields to mask and how:__
```bash
    IrisDataMasker.py setup [filename]
```

__Mask a table using the config file:__

*Input and output databases must be in the folowing format:*
```
    [hostname]:[database]:[table]
```

Run the command:
```bash
    IrisDataMasker.py mask [inputDB] [outputDB] [configfile] 
```
