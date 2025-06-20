import json
import random
from datetime import datetime
import os

import sagemaker
from sagemaker.processing import Processor, ScriptProcessor, ProcessingInput, ProcessingOutput

from dotenv import load_dotenv
load_dotenv()

YOUR_ROLE_ACCESS = os.environ('YOUR_ROLE_ACCESS')  # replace with your SageMaker execution role
YOUR_DOCKER_IMAGE_FROM_ECR = os.environ('YOUR_DOCKER_IMAGE_FROM_ECR')  # replace with your ECR image URI

def processing(mod: str, f: str, d: str, m: str, y: str, de=0, me=0, ye=0):
    # TODO implement
    
    # ml.g5.2xlarge	-> 7 hours
    COMPUTE_TYPE = 'ml.m5.2xlarge'
    
    ''' Initial SageMaker ProcessingJob which using entrypoint way meaning is main code is in docker image. '''
    Processor_Job1 = Processor(
        
        # select date
        # entrypoint=['python3', '/root/sentinel_process/workspace/coper2S1.py', '-m', f'{mod}', '-f', f'{f}', '-ds', f'{d}', '-ms', f'{m}', '-ys', f'{y}', '-de', f'{de}', '-me', f'{me}', '-ye', f'{ye}'],
        
        #everyday
        entrypoint=['python3', '/root/sentinel_process/workspace/coper2S1.py', '-m', f'{mod}', '-f', f'{f}', '-ds', f'{d}', '-ms', f'{m}', '-ys', f'{y}'],
        
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
    job_name = f"mitrphol-farmfocus-co2S3-{f}-{y}-{m}-{cname}-{rand_hash}"
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

def lambda_handler(event, context):
    today = datetime.today()
    
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    
    # select date
    # day = 1
    # month = 3
    # year = 2024
    # day_end = 9
    # month_end = 4
    # year_end = 2024
    
    # mode 1 is select date and mode 0 is everday. you can see more mode in the code 
    mode = 0
    
    factory = ["MPK", "MPL", "MPV", "MDC", "MAC", "MSB", "MKS"]
    # factory = ['MPK']
    
    for fac in factory:
        
        # select date
        # processing(mod=mode, f=fac, d=day, m=month, y=year, de=day_end, me=month_end, ye=year_end)
        
        # everyday
        processing(mod=mode, f=fac, d=day, m=month, y=year)
        
        print(f"processing see you in sagemaker processing job {fac}")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'{year} {month} {day}')
    }

    


    
