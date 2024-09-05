import luigi
from luigi.contrib.external_program import ExternalProgramTask
import datetime
import utils.datetime_utils as datetime_utils
import config
from download_gharchive import GHArchiveRetrieveData
from luigi import LocalTarget


class GenerateJob(ExternalProgramTask):
    # Parameters
    date = luigi.DateParameter(default=datetime.datetime.now().date())
    
    @property
    def output_dir(self):
        # Calculate the path for the previous day's date
        previous_day = self.date - datetime.timedelta(days=1)
        return datetime_utils.build_data_storage_path(base_path=config.DATA_DIR, date=previous_day)


    def requires(self):
        # Calculate the previous day and build the output directory path
        return GHArchiveRetrieveData(output_dir=self.output_dir)


    def program_args(self):
        # Construct the spark-submit command
        return [
            'spark-submit',
            '--master', config.SPARK_MASTER,
            '--deploy-mode', 'client',
            '--packages', 'io.delta:delta-spark_2.12:3.0.0',  # Include Delta Lake dependency
            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'generate_tables_pyspark.py',
            self.output_dir
        ]
    def output(self):
        aggregation_types = ['user', 'repository']
        formats = ['parquet', 'csv']
        
        targets = []
        
        for agg in aggregation_types:
            for fmt in formats:
                folder_path = self.output_dir + "/" + agg + "_aggregates." + fmt
                folder_target = LocalTarget(folder_path)
                # Check if the folder exists and add to the list of targets
                if folder_target.exists():
                    print(f"Folder exists: {folder_path}")
                else:
                    print(f"Folder does NOT exist: {folder_path}")
                
                targets.append(folder_target)

        # Return a list of LocalTarget objects
        return targets
        
    def run(self):
        # Run the Spark job using the provided program_args
        super(GenerateJob, self).run()

        # Mark the task as complete by creating the output file
        print("Spark job completed successfully.")

if __name__ == "__main__":
    luigi.run()
