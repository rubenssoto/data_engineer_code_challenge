import shutil
import boto3
import os
import logging
from urllib.parse import urlparse

logger = logging.getLogger()

class Checkpoint:
    """
    Represents a checkpoint for moving files from a source S3 path to a destination S3 path.

    The `Checkpoint` class provides a way to move all files from a source S3 path to a
    destination S3 path. It iterates through all files in the source path and moves each
    file to the corresponding location in the destination path.

    Args:
        source_path (str): The S3 path (s3a://bucket_name/path) containing the files to be moved.
        destination_path (str): The S3 path (s3a://bucket_name/path) where the files will be moved.

    Methods:
        move_files(): Moves all files from the source S3 path to the destination S3 path.
    """

    def __init__(self, source_path: str, destination_path: str):
        self.s3 = boto3.client('s3')
        self.source_bucket, self.source_prefix = self._parse_s3_path(source_path)
        self.destination_bucket, self.destination_prefix = self._parse_s3_path(destination_path)

    def _parse_s3_path(self, s3_path: str):
        """
        Parses an S3 path in the format s3a://bucket_name/path and returns the bucket name and key.
        
        Args:
            s3_path (str): The S3 path to parse.
            
        Returns:
            tuple: A tuple containing the bucket name and key (path).
        """
        parsed_url = urlparse(s3_path)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        return bucket_name, key

    def move_files(self):
        logger.info(f"Moving files from {self.source_bucket}/{self.source_prefix} to {self.destination_bucket}/{self.destination_prefix}")

        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.source_bucket, Prefix=self.source_prefix)

        for page in pages:
            for obj in page.get('Contents', []):
                source_key = obj['Key']
                destination_key = source_key.replace(self.source_prefix, self.destination_prefix, 1)

                # Copy the object to the new location
                copy_source = {'Bucket': self.source_bucket, 'Key': source_key}
                self.s3.copy_object(CopySource=copy_source, Bucket=self.destination_bucket, Key=destination_key)

                # Delete the original object
                self.s3.delete_object(Bucket=self.source_bucket, Key=source_key)

                logger.info(f"Moved {source_key} to {destination_key}")

        logger.info(f"Moved files from {self.source_bucket}/{self.source_prefix} to {self.destination_bucket}/{self.destination_prefix}")
