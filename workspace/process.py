import boto3
import os
import json
import subprocess

import rasterio

import os
from dotenv import load_dotenv
load_dotenv()

ACCESS_ID = os.environ('YOUR_ACCESS_KEY_ID')  # replace with your AWS access key ID
SECRET_ID = os.environ('SECRET_ID')
REGION_CODE = 'ap-southeast-1'
s3_client = boto3.client('s3', aws_access_key_id=ACCESS_ID, aws_secret_access_key=SECRET_ID, region_name=REGION_CODE)

def download_s1_bucket_to_local(bucket: str, key: str, path: str):
    return s3_client.download_file(bucket, key, path)

def get_s1_from_bucket(bucket: str, key: str, path: str):
    return s3_client.list_objects_v2(Bucket=bucket, Prefix=key, )
    
def list_s1(bucket: str, key: str, path: str):
    object = get_s1_from_bucket(bucket=bucket, key=key, path=path)
    print(object)
    keys = object['Contents']
    name_s1 = list()

    for key_object in keys:
       if len(key_object['Key']) > 100:
            name_s1.append(key_object['Key'])
    
    return name_s1

def download_s1_to_local(bucket: str, key: str, path: str):
    
    path = f'{path}/rawdata/sentinel1'
    
    product = list_s1(bucket=bucket, key=key, path=path)
    # print(product)
    for key in product:
        directory = '/'.join(key.split('/')[10:-1])
        filename = key.split('/')[-1]

        directory = f"{path}/{directory}"

        path_local = f"{directory}/{filename}"

        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory ->>> {directory} created")

        download_s1_bucket_to_local(bucket=bucket, key=key, path=path_local)
        print(f"key -->>>> {key}")
        print(f"path ->>>> {path_local}")

    return
        

def process(path: str):
    # path = '/Users/mapedia/Documents/project/sentinel1_snap/workspace/rawdata'
    path = f'{path}/rawdata'
    for filename in os.listdir(path):

        # Replace with the path to your JSON file
        file_path = f"{path}/{filename}/productInfo.json"
        with open(file_path, "r") as json_file:
            data = json.load(json_file)

        print(data['id'])
        path_data = f'{path}/{filename}/manifest.safe'
        filename = data['id']
        # output = f'root/sentinel_process/workspace/result/{filename}.tif'
        output = f'workspace/result/{filename}.tif'
        print('input', path_data)
        print('output', output)

        if os.path.exists(output):
            print(f"File exists stop create {output}.")
        else:
            print('start generate sentinel1 tif')

            cmd = f'/usr/local/snap/bin/gpt /root/sentinel_process/workspace/xml/S1_GRD_preprocessingV4.xml  -Pin_put={path_data} -Pout_tif={output}'
            print(cmd)

            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError:
                print("Error: Failed convert tif .")

    return

def delete_file_on_local(bucket: str, key: str, path: str):
    product = list_s1(bucket=bucket, key=key, path=path)
    path_raw = f'{path}/rawdata/sentinel1'
    
    # remove rawdata when uoloded
    for key in product:
        directory = '/'.join(key.split('/')[10:-1])
        filename = key.split('/')[-1]

        directory = f"{path_raw}/{directory}"

        path_local = f"{directory}/{filename}"

        # print(f"key -->>>> {key}")
        if os.path.exists(path_local):
            os.remove(path_local)
            print(f"path delete --> {path_local}")

    # remove result when uploded
    
    # Docker
    result_path = f'{path}/result'
    
    # local
    # result_path = 'D:/mitrphol_practice_vscode/mitrphol-snap-main/root/sentinel_process/workspace/result'
    
    
    for f in os.listdir(result_path):
        f_path = f'{result_path}/{f}'
        print(f_path)
        if os.path.exists(f_path):
            os.remove(f_path)
            print(f"path delete --> {f_path}")
        
    return
        
    


def upload_s1_to_bucket(bucket: str, agrs: dict, path: str):
    # Docker
    path = f'{path}/result'

    # my local
    # path = "D:/mitrphol_practice_vscode/mitrphol-snap-main/root/sentinel_process/workspace/result"
    
    for product in os.listdir(path):

        s1_path = os.path.join(path, product)
        print(s1_path)
        
        split_band = product.split(".")[0]
        if split_band in ["band1", "band2"]:

            # ASCENDING T1
            try:
                T = agrs["PROD"].split('T')[1][0]

                if T == '1':
                    key = 'satellite-sentinel1-raw2VH_VV/S1_ASC'
                    
                    # Given that VV = band1, VH = band2
                    if split_band == 'band1':
                        key_path = f'{key}/{agrs["FAC"]}/{agrs["YEAR"]}/{agrs["MONTH"]}/{agrs["DAY"]}/IW/DV/{agrs["PROD"]}/VV_{agrs["PROD"]}.tif'
                    else:
                        key_path = f'{key}/{agrs["FAC"]}/{agrs["YEAR"]}/{agrs["MONTH"]}/{agrs["DAY"]}/IW/DV/{agrs["PROD"]}/VH_{agrs["PROD"]}.tif'
                        
                    print(f"UPLOADING ASCENDING KEY -> {key_path}")
                    s3_client.upload_file(s1_path, bucket, key_path)
                
                # DESCENDING T2
                if T == '2':
                    key = 'satellite-sentinel1-raw2VH_VV/S1_DES'

                    # Given that VV = band1, VH = band2
                    if split_band == 'band1':
                        key_path = f'{key}/{agrs["FAC"]}/{agrs["YEAR"]}/{agrs["MONTH"]}/{agrs["DAY"]}/IW/DV/{agrs["PROD"]}/VV_{agrs["PROD"]}'
                    else:
                        key_path = f'{key}/{agrs["FAC"]}/{agrs["YEAR"]}/{agrs["MONTH"]}/{agrs["DAY"]}/IW/DV/{agrs["PROD"]}/VH_{agrs["PROD"]}'
                        
                    print(f"UPLOADING DESCENDING KEY -> {key_path}")
                    s3_client.upload_file(s1_path, bucket, key_path)
            except Exception as e:
                print(e)
    return

def set_up_key():
    import argparse

    parse = argparse.ArgumentParser()

    parse.add_argument("-d","--DAY", help="DAY", required=True)
    parse.add_argument("-m","--MONTH", help="MONTH", required=True)
    parse.add_argument("-y", "--YEAR", help="YEAR", required=True)
    parse.add_argument("-p", "--PROD", help="PRODUCT", required=True)
    parse.add_argument("-f", "--FAC", help="FACTORY", required=True)
    parse.add_argument("-or", "--ORBIT", help="ASCENDING OR DESCENDING", required=True)


    args = vars(parse.parse_args())

    # key = 'satellite-sentinel1-raw/S1_ASC/MPK/GRD/2024/2/10/IW/DV/S1A_IW_GRDH_1SDV_20240210T112151_20240210T112216_052496_065965_D1FB/')
    key = f'satellite-sentinel1-raw/{args["ORBIT"]}/{args["FAC"]}/GRD/{args["YEAR"]}/{args["MONTH"]}/{args["DAY"]}/IW/DV/{args["PROD"]}/'
    print(f"{args} --> (keys) {key}")

    return key, args
    


def split_product(args: dict, path: str):
    productID = args['PROD']
    #Docker
    path =  f"{path}/result"
    
    #Local
    # path = "D:/mitrphol_practice_vscode/mitrphol-snap-main/root/sentinel_process/workspace/result"

    # 
    with rasterio.open(f"{path}/{productID}.tif") as src:
        band1 = src.read(1)
        band2 = src.read(2)
        
        metadata = src.meta.copy()
        print(metadata)
        
        #  band1 == VV
        with rasterio.open(f"{path}/band1.tif", "w", **metadata) as dst1:
            dst1.write(band1, 1)
            print("success band1 -> VV")
        # band2 == VH
        with rasterio.open(f"{path}/band2.tif", "w", **metadata) as dst2:
            dst2.write(band2, 2)
            print("success band2 - > VH")

    return
    

if __name__ == '__main__':
    bucket = 'mitrphol-satellite-207185859506'
    '''key have to indentify 
    1. factory
    2. year month day
    3. product ID
    4. asc or des
    '''
    # key = 'satellite-sentinel1-raw/S1_ASC/MPK/GRD/2024/2/10/IW/DV/S1A_IW_GRDH_1SDV_20240210T112151_20240210T112216_052496_065965_D1FB/'

    # Docker
    path = "/root/sentinel_process/workspace"
    
    # local
    # path = "D:/mitrphol_practice_vscode/mitrphol-snap-main/workspace"
    
    key, args = set_up_key()
    print(key, args)
    
    download_s1_to_local(bucket=bucket, key=key, path=path)
    
    process(path=path)
    split_product(path=path, args=args)
    
    upload_s1_to_bucket(bucket=bucket, agrs=args, path=path)
    delete_file_on_local(path=path, bucket=bucket, key=key)

# s3://mitrphol-satellite-207185859506/satellite-sentinel1-raw/S1_ASC/MPK/GRD/2024/2/10/IW/DV/S1A_IW_GRDH_1SDV_20240210T112151_20240210T112216_052496_065965_D1FB/


