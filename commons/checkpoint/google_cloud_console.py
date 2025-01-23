from google.cloud import storage
from typing import List
from pytz import timezone
from datetime import datetime
import os

class GoogleCloudStorage:
    def __init__(self, bucket_name: str, env: str, on_server: bool):
        """
        Parameters
        ----------
            bucket_name: str
                specified bucket name, which contained in `env`
            
            env: str
                working environment options, "dev" (development) or "prod" (production)
            
            on_server: bool
                whether the access is in local or AWS server
        """
        self.bucket_name = bucket_name
        self.env = env
        self.on_server = on_server

        self.cred_path = f'./credentials/{env}/{env}.json' if not on_server else '../../keys/sa-gcp-agriaku.json'
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.cred_path
        
        # Set bucket of Google Cloud Storage
        self.create_bucket(bucket_name=bucket_name)

        # Define bucket
        self.storage_client = storage.Client.from_service_account_json(json_credentials_path=self.cred_path)
        self.bucket = self.storage_client.bucket(bucket_name)


    def create_bucket(self, bucket_name:str, storage_class:str ='STANDARD', location:str ='asia-southeast2'):
        storage_client = storage.Client.from_service_account_json(json_credentials_path=self.cred_path)
        bucket = storage_client.bucket(bucket_name)
        
        if not bucket.exists():
            bucket.storage_class = storage_class
            bucket = storage_client.create_bucket(bucket, location=location) 


    def upload_cs_file(self, source_file_name:str, destination_file_name:str): 
        """
        Upload a file from the bucket

        Parameters
        ----------
            source_file_name: str
                source file path

            destination_file_name: str
                destinated file path
        """
        blob = self.bucket.blob(destination_file_name)
        blob.upload_from_filename(source_file_name)

    def download_cs_file(self, file_name:str, destination_file_name:str): 
        """
        Download a file from the bucket

        Parameters
        ----------
            file_name: str
                source file path

            destination_file_name: str
                destinated file path
        """
        blob = self.bucket.blob(file_name)
        blob.download_to_filename(destination_file_name)

        return True

    def list_cs_files(self): 
        """
        List files in the bucket - have the same purpose as os.listdir, but in bucket.

        Returns
        ----------
            file_list: List[str]
                list of files contained in bucket
        """
        file_list = self.storage_client.list_blobs(self.bucket_name)
        file_list = [file.name for file in file_list]

        return file_list

    def get_blob(self, file_name:str):
        """
        Instantiate blob object from specific file

        Parameters
        ----------
            file_name: str
                specified file name

        Returns
        ----------
            blob: blob object of file
        """
        blob = self.bucket.blob(file_name)
        return blob
    
    def get_file_created_date(self, file_name:str, timezone_loc:str='Asia/Jakarta'):
        """
        Obtain date created of file

        Parameters
        ----------
            file_name: str
                specified file name

            timezone_loc: str
                specified timezone location (ex.: 'Asia/Jakarta')

        Returns
        ----------
            date_created: datetime.date
                date created of specified file
        """
        for o in self.bucket.list_blobs(prefix=file_name):
            time_created = o.time_created.astimezone(timezone(timezone_loc))
            date_created = time_created.date()
            return date_created

    def delete_files(self, files_to_delete: List[str]):
        """
        Removing multiple files on Google Cloud Storage

        Parameters
        ----------
            files_to_delete: List[str]
                list of files to delete
        """        
        for file in files_to_delete:
            blob = self.bucket.blob(file)
            blob.delete()