import os
import requests
import datetime
from datetime import timedelta
import luigi
from luigi import LocalTarget


class GHArchiveRetrieveData(luigi.Task):
    """
        Initialize the GHArchiveRetrieveData class.
        
        :param base_path: Base path where the data will be stored.
        """
    
    def __init__(self, *args, **kwargs):
        super(GHArchiveRetrieveData, self).__init__(*args, **kwargs)
        self.output_dir_input_data = self.output_dir + "/input_data"
        
    output_dir = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.datetime.now().date() - timedelta(days=1))
    
    
    def output(self):
        """
        Check if the GH Archive data for the day has already been downloaded.
        """
        return LocalTarget(self.output_dir_input_data)

    def run(self):
        """
        Download the GH Archive data for the specified date.
        """
        if not os.path.exists(self.output_dir_input_data):
            os.makedirs(self.output_dir_input_data)

        for hour in range(24):
            file_name = f"gharchive-{self.date}-{hour:02d}.json.gz"
            output_file = os.path.join(self.output_dir_input_data, file_name)
            
            if not os.path.exists(output_file):
                url = f"http://data.gharchive.org/{self.date}-{hour:01d}.json.gz"
                print(f"Downloading data from {url}...")
                response = requests.get(url, stream=True)

                if response.status_code == 200:
                    # Save the data to a file
                    with open(output_file, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Data saved to {output_file}.")
                else:
                    print(f"Failed to download data. Status code: {response.status_code}")

if __name__ == "__main__":
    luigi.run()
    
