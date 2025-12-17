# script to a) extract world bank indicator data b) convert into data packages
# built for python3

import urllib.request
import urllib.parse
import json
import csv
import codecs
import os
import zipfile
import io

class Processor(object):

    def __init__(self, indicator):
        self.indicator = indicator
        if 'http' in self.indicator:
            # https://data.worldbank.org/indicator/SL.GDP.PCAP.EM.KD?locations=BR&view=chart
            path = urllib.parse.urlparse(self.indicator)[2]
            self.indicator = path.split('/')[-1]

        self.meta_url = 'https://api.worldbank.org/v2/indicator/%s?format=json' % self.indicator
        self.data_url = 'https://api.worldbank.org/v2/en/indicator/%s?downloadformat=csv' % self.indicator
        self.meta_dest = os.path.join('cache', '%s.meta.json' % self.indicator)
        self.data_dest = os.path.join('cache', '%s.csv' % self.indicator)
    
    def execute(self, cache=True):
        '''Retrieve a world bank indicator and convert to a data package.

        Data Package is stored at ./indicators/{indicator-name}
        '''
        try:
            if cache:
                self.retrieve()

                if not os.path.exists(self.meta_dest):
                    raise Exception(f"Metadata file not found: {self.meta_dest}")
                if not os.path.exists(self.data_dest):
                    raise Exception(f"Data file not found: {self.data_dest}")
                
                (meta, data) = self.extract(open(self.meta_dest), open(self.data_dest))
            else:
                meta_response = urllib.request.urlopen(self.meta_url)
                data_response = urllib.request.urlopen(self.data_url)
                zip_data = io.BytesIO(data_response.read())
                with zipfile.ZipFile(zip_data) as zf:
                    # Find the main data file (starts with "API_")
                    csv_files = [f for f in zf.namelist() if f.startswith('API_') and f.endswith('.csv')]
                    if not csv_files:
                        raise Exception(f"No API CSV file found in ZIP for {self.indicator}")
                    
                    with zf.open(csv_files[0]) as csv_file:
                        csv_text = io.TextIOWrapper(csv_file, encoding='utf-8')
                        (meta, data) = self.extract(meta_response, csv_text)

            basepath = os.path.join('indicators', meta['name'])
            os.makedirs(basepath, exist_ok=True)
            self.datapackage(meta, data, basepath)
            return basepath
        except Exception as e:
            print(f"Skipping indicator {self.indicator}: {e}")
            return None

    def retrieve(self):
        os.makedirs('cache', exist_ok=True)
        
        # Check if cache files already exist and are valid
        cache_exists = os.path.exists(self.meta_dest) and os.path.exists(self.data_dest)
        if cache_exists:
            print(f"Using cached files for {self.indicator}")
            return
        
        try:
            # Download metadata with timeout
            req = urllib.request.Request(self.meta_url)
            with urllib.request.urlopen(req, timeout=30) as response:
                with open(self.meta_dest, 'wb') as f:
                    f.write(response.read())
        except Exception as e:
            print(f"Warning: Failed to download metadata for {self.indicator}: {e}")
            if not os.path.exists(self.meta_dest):
                raise
        
        # Download ZIP and extract the main CSV
        zip_dest = self.data_dest + '.zip'
        try:
            req = urllib.request.Request(self.data_url)
            with urllib.request.urlopen(req, timeout=60) as response:
                if response.status != 200:
                    print(f"Warning: HTTP {response.status} for {self.indicator}")
                    return
                
                with open(zip_dest, 'wb') as f:
                    f.write(response.read())
            
            with zipfile.ZipFile(zip_dest) as zf:
                # Find and extract the main data file (starts with "API_")
                csv_files = [f for f in zf.namelist() if f.startswith('API_') and f.endswith('.csv')]
                if not csv_files:
                    print(f"Warning: No API CSV file found in ZIP for indicator {self.indicator}")
                    return
                
                with zf.open(csv_files[0]) as csv_file:
                    with open(self.data_dest, 'wb') as f:
                        f.write(csv_file.read())
            
            if os.path.exists(zip_dest):
                os.remove(zip_dest)
        except Exception as e:
            print(f"Warning: Failed to download or extract CSV for indicator {self.indicator}: {e}")
            if os.path.exists(zip_dest):
                os.remove(zip_dest)

    @classmethod
    def extract(self, metafo, datafo):
        '''Extract raw metadata and data into nicely structured form.

        @metafo: world bank json metadata file object
        @datafo: world bank CSV data file object
        @return: (metadata, data) where metadata is Data Package JSON and data is normalized CSV.
        '''
        metadata = {}
        data = []
        tmpmeta = json.load(metafo)[1][0]
        # raw metadata looks like
        # [{"page":1,"pages":1,"per_page":"50","total":1},[{"id":"GC.DOD.TOTL.GD.ZS","name":"Central government debt, total (% of GDP)","source":{"id":"2","value":"World Development Indicators"},"sourceNote":"Debt is the entire stock of direct government fixed-term contractual obligations to others outstanding on a particular date. It includes domestic and foreign liabilities such as currency and money deposits, securities other than shares, and loans. It is the gross amount of government liabilities reduced by the amount of equity and financial derivatives held by the government. Because debt is a stock rather than a flow, it is measured as of a given date, usually the last day of the fiscal year.","sourceOrganization":"International Monetary Fund, Government Finance Statistics Yearbook and data files, and World Bank and OECD GDP estimates.","topics":[{"id":"3","value":"Economy & Growth"},{"id":"13","value":"Public Sector "}]}]]
        metadata = {
            'title': tmpmeta['name'],
            'name': tmpmeta['id'].lower(),
            'worldbank': {
                'indicator': tmpmeta['id'].lower()
            },
            'readme': tmpmeta['sourceNote'],
            'licenses': [{
                'name': 'CC-BY-4.0'
                }],
            'keywords': [ x['value'] for x in tmpmeta['topics'] ]
        }
        
        tmpdata = csv.reader(datafo)
        
        # New World Bank CSV format (as of 2025):
        # Row 1: "Data Source","World Development Indicators",
        # Row 2: (empty)
        # Row 3: "Last Updated Date","2025-12-15",
        # Row 4: (empty)
        # Row 5: "Country Name","Country Code","Indicator Name","Indicator Code","1960","1961",...
        # Row 6+: Data rows with years as columns
        
        # Skip the first 4 rows (metadata rows)
        for _ in range(4):
            try:
                tmpdata.__next__()
            except StopIteration:
                # If we can't skip 4 rows, the file might be in old format
                # Reset and try old format parsing
                datafo.seek(0)
                tmpdata = csv.reader(datafo)
                break
        
        fields = tmpdata.__next__()
        
        # Remove BOM if present at start of file
        if fields[0].startswith('\ufeff'):
            fields[0] = fields[0].replace('\ufeff', '').strip('"')
        
        # Check format: new format has years starting at index 4
        # Old format: Country Name, Country Code, Year, Value (no year columns)
        # New format: Country Name, Country Code, Indicator Name, Indicator Code, 1960, 1961, ...
        
        if len(fields) > 4 and fields[4].isdigit():
            year_columns = fields[4:]
            outdata = [['Country Name', 'Country Code', 'Year', 'Value']]
            
            for row in tmpdata:
                if len(row) < 4:
                    continue
                    
                country_name = row[0]
                country_code = row[1]
                
                # Unpivot: convert year columns to rows
                for year, value in zip(year_columns, row[4:]):
                    if value and value.strip():
                        outdata.append([country_name, country_code, year, value])
        else:
            # fallback to old format
            outdata = [fields[:2] + ['Year', 'Value']]
            for row in tmpdata:
                for year, col in zip(fields[2:], row[2:]):
                    if col.strip():
                        outdata.append(row[0:2] + [year, col])

        return (metadata, outdata)

    @classmethod
    def datapackage(self, metadata, data, basepath):
        dpjson = os.path.join(basepath, 'datapackage.json')
        readme = os.path.join(basepath, 'README.md')
        datafp = os.path.join(basepath, 'data.csv')

        metadata['resources'] = [{
            'name': 'data',
            'title': 'Indicator data',
            'path': 'data.csv',
            'format': 'csv',
            'mediatype': 'text/csv',
            'encoding': 'utf-8',
            'schema': {
                'fields': [
                    {
                        'name': 'Country Name',
                        'type': 'string',
                        'description': 'Country or Region name'
                    },
                    {
                        'name': 'Country Code',
                        'type': 'string',
                        'description': 'ISO 3-digit ISO code extended to include regional codes e.g. EUR, ARB etc'
                    },
                    {
                        'name': 'Year',
                        'type': 'year',
                        'description': 'Year'
                    },
                    {
                        'name': 'Value',
                        'type': 'number', # TODO check it is always numeric ...!
                        'description': metadata['readme']
                    }
                ]
            }
        }]

        with open(dpjson, 'w') as fo:
            json.dump(metadata, fo, indent=2)
        with open(readme, 'w') as fo:
            fo.write(metadata['readme'])
        with open(datafp, 'w') as fo:
            writer = csv.writer(fo)
            writer.writerows(data)


import tempfile
import os
def test_it():
    url = 'https://data.worldbank.org/indicator/GC.DOD.TOTL.GD.ZS'
    indicator = 'GC.DOD.TOTL.GD.ZS'
    processor = Processor(url)

    assert processor.indicator == indicator

    (meta, data) = processor.extract(
            urllib.request.urlopen(processor.meta_url),
            codecs.iterdecode(urllib.request.urlopen(processor.data_url), 'utf-8')
            )
    assert meta['title'] == 'Central government debt, total (% of GDP)'
    print(data[0:2])
    assert data[0] == ['Country Name', 'Country Code', 'Year', 'Value']
    assert data[1] == ['Caribbean small states', 'CSS', '2006', '53.4626403130906']

    with tempfile.TemporaryDirectory() as basepath:
        processor.datapackage(meta, data, basepath)
        readme = open(os.path.join(basepath, 'README.md')) 
        dp = json.load(open(os.path.join(basepath, 'datapackage.json')))
        data = open(os.path.join(basepath, 'data.csv'))

        assert dp['resources'][0]['schema']['fields'][3]['name'] == 'Value'


import sys
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('''
Usage: python scripts/get.py {indicator or indicator url}

Example:

    python scripts/get.py https://data.worldbank.org/indicator/GC.DOD.TOTL.GD.ZS
''')
        sys.exit(1)

    indicator = sys.argv[1] 
    processor = Processor(indicator)
    out = processor.execute()
    print('Indicator data package written to: %s' % out)

