import json
import random
from datetime import date, datetime, timedelta
from sagemaker import get_execution_role
import os

import sagemaker
from sagemaker.processing import Processor, ScriptProcessor, ProcessingInput, ProcessingOutput

import boto3

from dotenv import load_dotenv
load_dotenv()


today = datetime.now()
# today_bkk = today + timedelta(hours=7)

s3_client = boto3.client("s3")
FACTORY = ["MPK", "MPL", "MPV", "MDC", "MAC", "MSB", "MKS"]

# FACTORY = ["MKS"]
BUCKET = os.environ('YOUR_BUCKET')  # replace with your bucket name
YOUR_ROLE_ACCESS = os.environ('YOUR_ROLE_ACCESS')  # replace with your SageMaker execution role
YOUR_DOCKER_IMAGE_FROM_ECR = os.environ('YOUR_DOCKER_IMAGE_FROM_ECR')  # replace with your ECR image URI

# # #########################################################

def lambda_handler(event, context):
    # TODO implement
    print('NOOOOOOO')
    get_args_of_dict()
    
    # test
    # book_args = {'DAY': '24',
    #     'MONTH': '3',
    #     'YEAR': '2024',
    #     'FAC': 'MAC',
    #     'ORBIT': 'S1_ASC',
    #     'PROD': 'S1A_IW_GRDH_1SDV_20240324T111330_20240324T111355_053123_066F5F_7278'
    # }
    # print(book_args[DAY])
    # docker_process(book_args=book_args)

    return {
        'statusCode': 200,
        'body': json.dumps(f"{today}")
    }

def docker_process(book_args: dict):
    try:
        # TODO implement
        today = date.today()
        year = str(today.year)
        month = str(today.month)
        # print(book_args)
        # ml.g5.2xlarge	-> 7 hours
        COMPUTE_TYPE = 'ml.c5.2xlarge'
        
        ''' Initial SageMaker ProcessingJob which using entrypoint way meaning is main code is in docker image. '''
        Processor_Job1 = Processor(
            entrypoint = ['python3', '/root/sentinel_process/workspace/process.py', '-d', book_args['DAY'], '-m', book_args['MONTH'], '-y', book_args['YEAR'], '-f', book_args['FAC'], '-or', book_args['ORBIT'], '-p', book_args['PROD']],
            # entrypoint=['cd', '/root/sentinel_process/root/sentinel_process/workspace/result/'],
            role=YOUR_ROLE_ACCESS,
            image_uri=YOUR_DOCKER_IMAGE_FROM_ECR,
            instance_count=1,
            instance_type=COMPUTE_TYPE, 
            volume_size_in_gb=30,
            max_runtime_in_seconds=172800,
            tags=[
                {"Key":"ou",       "Value":"digit-a"},
                {"Key":"Project",  "Value":"farmfocus"},
                {"Key":"createdBy","Value":"thanutj"},
                {"Key":"productOwner",   "Value":"athikank"},
                {"Key":"env",       "Value":"test"},
                ]
        )
        
        rand_hash = str("%08x" % random.getrandbits(32))
        rand_hash = rand_hash.replace(" ","")
        cname = COMPUTE_TYPE.replace(".","")
        job_name = f"mitrphol-farmfocus-harvest-{book_args['FAC']}-{book_args['DAY']}-{book_args['MONTH']}-{rand_hash}"
        print(job_name)
        
        Processor_Job1.run(
            # arguments=[
            #     "-year", year,
            #     "-month", f'{month}'
            #     ],
            job_name=job_name, 
            wait=False,
            logs=False
        )
        
        print(COMPUTE_TYPE)
        print("calling docker is finished")
    except Exception as e:
        print(e)



def arg_dict_of_bucket_raw(book_args: dict):
    
    path_raw = f'satellite-sentinel1-raw/{book_args["ORBIT"]}/{book_args["FAC"]}/GRD/{book_args["YEAR"]}/{book_args["MONTH"]}/{book_args["DAY"]}/IW/DV/'

    list_object = s3_client.list_objects(Bucket=BUCKET, Prefix=path_raw, Delimiter='/')

    # assume bucket raw have more 2 productID 
    for product_raw in list_object['CommonPrefixes']:
        
        try:
            product_raw = product_raw['Prefix'].split('/')[9]
            # should add compare between bucket_raw and bucket_rawVV_VH that it have product yet?
            book_args.update(PROD = product_raw)
            print(book_args)
            # it have to error if it want to go docker process
            compare_bucket_rawdata_and_processdata(book_args=book_args, product_raw=product_raw)
        
        except Exception as e:
            print("processing docker")
            docker_process(book_args=book_args)
            print("-"*50)

    return book_args
        

def compare_bucket_rawdata_and_processdata(book_args: dict, product_raw: str):
    
    # dest_bucket
    bucket_dest = f'satellite-sentinel1-raw2VH_VV/{book_args["ORBIT"]}/{book_args["FAC"]}/{book_args["YEAR"]}/{book_args["MONTH"]}/{book_args["DAY"]}/IW/DV/'
    # print(bucket_dest)
    list_object_of_bucket_raw2VH_VV = s3_client.list_objects(Bucket=BUCKET, Prefix=bucket_dest, Delimiter='/')

    for product_raw2VV_VH in list_object_of_bucket_raw2VH_VV['CommonPrefixes']:
        product_raw2VV_VH = product_raw2VV_VH['Prefix'].split('/')[8]
        if product_raw == product_raw2VV_VH:
            print(f"file product s1 has exits -> {product_raw2VV_VH}")
    
    return
    

def get_args_of_dict():
    
    lst_book = []
    date = datetime(2024,3, 24)
    now = datetime(2024, 3, 24)
    # yesterday
    FACTORY = ["MPK", "MPL", "MPV", "MDC", "MAC", "MSB", "MKS"]
    # FACTORY = ['MAC']
    
    while date <= now:
        for FACT in FACTORY:
        
            book_args = {"DAY": f'{date.day}',
                "MONTH": f'{date.month}',
                "YEAR": f'{date.year}',
                "FAC" : f"{FACT}",
                }
            try:
                
                book_args.update(ORBIT="S1_ASC")
                arg_dict_of_bucket_raw(book_args=book_args)
                # print("asc", dict_of_arg)
                # lst_book.append(book_args)
                
            except:
                try:
                    book_args.update(ORBIT="S1_DES")
                    arg_dict_of_bucket_raw(book_args=book_args)
                    # print("des", dict_of_arg)
                    # lst_book.append(book_args)
                except Exception as e:
                    pass
                
        date += timedelta(days=1)

    return 
