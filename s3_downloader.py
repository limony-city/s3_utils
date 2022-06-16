import json
import os
from typing import List

import boto3

import typer

cli = typer.Typer()


class Color:
    END = '\x1b[0m'
    TITLE = '\x1b[6;30;43m'
    FILE = '\x1b[0;33;40m'


@cli.command()
def ls(bucket: str, prefix: str = '', profile: str = 'production'):
    """
    List all the queried aws objects.

    Args:
        bucket: Bucket name.
        prefix: Path to object. Contains dirs and file prefix of a specific object. For example: 'logs/2022-01-01'
        (all file in dir `logs` that starts with `2022-01-01`)
        profile: AWS_PROFILE to connect to.
    """
    set_profile(profile=profile)
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    print(Color.TITLE + f'Listing bucket `{s3_bucket.name}`' + Color.END)
    for s3_object in s3_bucket.objects.filter(Prefix=prefix):
        print(Color.FILE + f'{s3_object.key}' + Color.END)


@cli.command()
def dump(
        bucket: str, prefix: str = '', profile: str = 'production',
        keys: List[str] = None, text: bool = True,
):
    """
    Prints the content of json files.

    Args:
        bucket: Bucket name.
        prefix: Path to object. Contains dirs and file prefix of a specific object. For example: 'logs/2022-01-01'
        (all file in dir `logs` that starts with `2022-01-01`)
        profile: AWS_PROFILE to connect to.
        keys: List of json keys to present.
        text: Prints all payloads as is.
         If not, will treat the files content as json and will have the ability to use `keys`
    """
    set_profile(profile=profile)
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)

    print(Color.TITLE + f'Showing content in bucket `{s3_bucket.name}` with prefix `{prefix}`' + Color.END)
    for s3_object in s3_bucket.objects.filter(Prefix=prefix):

        # Loads each file to dict and filter the requested keys.
        print(Color.FILE + f'{s3_object.key}' + Color.END)
        content = s3_object.get().get('Body').read().decode('utf-8')
        if not text:
            json_content = json.loads(content)
            for row in json_content:
                display_dict = {key: row.get(key) for key in keys} if keys else row
                print(display_dict)
        else:
            print(content)


@cli.command()
def download(
        bucket: str, prefix: str = '', profile: str = 'production',
        target: str = '.', new_dir: bool = False, raw: bool = False, merge: bool = False
):
    """
    Download s3 files.

    Args:
        bucket: Bucket name.
        prefix: Path to object. Contains dirs and file prefix of a specific object. For example: 'logs/2022-01-01'
        (all file in dir `logs` that starts with `2022-01-01`)
        profile: AWS_PROFILE to connect to.
        target: Local directory path to download files to.
        new_dir: Whether to create new dirs if not exists.
        raw: If true, downloads the raw bytes and inserts them to new files. If false, just download the files as is.
        merge: If true, will merge all files payloads into one huge file.
    """
    merged_bytes = b''
    set_profile(profile=profile)
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)

    print(Color.TITLE + f'Downloading content in bucket `{s3_bucket.name}` with prefix `{prefix}`' + Color.END)

    # Create directories
    if new_dir:
        os.makedirs(name=target, exist_ok=True)

    for s3_object in s3_bucket.objects.filter(Prefix=prefix):
        file_name = os.path.basename(s3_object.key)
        path = os.path.join(target, file_name)
        if not raw:
            # Downloading complete files
            with open(path, 'wb') as file:
                # file.write(bytes_content)
                s3_bucket.download_fileobj(s3_object.key, file)
            print(Color.FILE + f'Downloaded `{s3_object.key}` to `{path}`' + Color.END)
        else:
            bytes_content = s3_object.get().get('Body').read()

            if merge:
                # Store all files content
                merged_bytes += bytes_content + b'\n'
            else:
                # Create files
                with open(path, 'wb') as file:
                    file.write(bytes_content)
                print(Color.FILE + f'Downloaded `{s3_object.key}` to `{path}`' + Color.END)

    # Merge all downloaded files into one file.
    if merge:
        file_name = (s3_bucket.name + '__' + prefix + '.json').replace('/', '__')
        path = os.path.join(target, file_name)
        with open(path, 'wb') as file:
            file.write(merged_bytes)
        print(Color.FILE + f'Downloaded `{s3_bucket.name}/{prefix}` to `{path}`' + Color.END)


def set_profile(profile: str):
    os.environ['AWS_PROFILE'] = profile


if __name__ == '__main__':
    cli()
