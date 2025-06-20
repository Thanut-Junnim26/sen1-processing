import requests
from datetime import datetime, timedelta
import json
import os

import boto3

from dotenv import load_dotenv
load_dotenv()

ACCESS_ID = os.getenv('ACCESS_ID')
SECRET_ID = os.getenv('SECRET_ID')
YOUR_BUCKET = os.getenv('YOUR_BUCKET')  # replace with your bucket name

def main(event: dict, factory: str):

    product_list = list_product(event=event, factory=factory)
    print(factory)

    for product_name in product_list:

        # if ascend or descend
        # Case Ascending
        if product_name.split('_')[4].split('T')[1][0] == '1':

            list_of_all_file_of_product = list_all_files_in_product(
                event=event, file_name=product_name)

            for file_name in list_of_all_file_of_product:

                print(file_name)
                # Check if vh or vv
                # v_type = file_name.split('.')[0][-2:]

                # if v_type == 'vv' or v_type == 'vh':
                print(file_name)
                file_data = get_all_files_in_product_from_s3(key=file_name)
                dest_path = "satellite-sentinel1-raw/S1_ASC/{}/{}".format(
                    factory,file_name)
                put_file_to_dest_s3(data=file_data, key=dest_path)

        # Case Descending
        if product_name.split('_')[4].split('T')[1][0] == '2':

            list_of_all_file_of_product = list_all_files_in_product(
                event=event, file_name=product_name)

            for file_name in list_of_all_file_of_product:

                # # Check if vh or vv
                # v_type = file_name.split('.')[0][-2:]

                # if v_type == 'vv' or v_type == 'vh':

                file_data = get_all_files_in_product_from_s3(key=file_name)
                dest_path = "satellite-sentinel1-raw/S1_DES/{}/{}".format(
                    factory,file_name)
                put_file_to_dest_s3(data=file_data, key=dest_path)


def list_product(event: dict, factory: str):

    query_date = event['query-date']
    query_date = f'{query_date:%Y-%m-%d}'
    if factory == 'MPK':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((102.09%2016.46,%20102.14%2016.46,%20102.14%2016.5,%20102.09%2016.5,%20102.09%2016.46))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MPL':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((101.81%2017.19,%20101.84%2017.19,%20101.84%2017.21,%20101.81%2017.21,%20101.81%2017.19))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MPV':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((102.4%2016.45,%20102.52%2016.45,%20102.52%2016.54,%20102.4%2016.54,%20102.4%2016.45))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MDC':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((99.7%2014.82,%2099.8%2014.82,%2099.8%2014.89,%2099.7%2014.89,%2099.7%2014.82))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MAC':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((104.46%2015.77,%20104.65%2015.77,%20104.65%2015.91,%20104.46%2015.91,%20104.46%2015.77))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MSB':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((100.23%2014.75,%20100.46%2014.75,%20100.46%2014.94,%20100.23%2014.94,%20100.23%2014.75))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    if factory == 'MKS':
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?&$filter=((Collection/Name%20eq%20%27SENTINEL-1%27%20and%20(Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27instrumentShortName%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27SAR%27)%20and%20(contains(Name,%27GRD%27)%20and%20not%20contains(Name,%27_COG%27)%20and%20OData.CSC.Intersects(area=geography%27SRID=4326;POLYGON%20((103.98%2016.44,%20104.06%2016.44,%20104.06%2016.49,%20103.98%2016.49,%20103.98%2016.44))%27)))%20and%20(Online%20eq%20true%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27platformSerialIdentifier%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27A%27)%20and%20Attributes/OData.CSC.StringAttribute/any(att:att/Name%20eq%20%27operationalMode%27%20and%20att/OData.CSC.StringAttribute/Value%20eq%20%27IW%27)))%20and%20ContentDate/Start%20ge%20{query_date}T00:00:00.000Z%20and%20ContentDate/Start%20lt%20{query_date}T23:59:59.999Z)&$orderby=ContentDate/Start%20desc&$expand=Attributes&$count=True&$top=50&$expand=Assets&$skip=0"
    
    response = requests.request("GET", url,)

    data_dict = json.loads(response.text)

    num_of_data = data_dict['@odata.count']
    print("Number of data :", num_of_data)

    name_list = list()
    for value in data_dict['value']:
        name_list.append(value['Name'].split('.')[0])

    return name_list


def list_all_files_in_product(event: dict, file_name: str):

    # s3_sentinel_1_region = 'eu-central-1'
    s3_sentinel_1_bucket_name = 'sentinel-s1-l1c'

    query_date = event['query-date']

    s3_sentinel_1 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=SECRET_ID
        # region_name=s3_sentinel_1_region
    )
    response = s3_sentinel_1.list_objects_v2(
        Bucket=s3_sentinel_1_bucket_name, Prefix=f"GRD/{query_date.year}/{query_date.month}/{query_date.day}/IW/DV/{file_name}/", RequestPayer='requester')
    # print(response)
    list_of_file = list()
    for value in response['Contents']:
        list_of_file.append(value['Key'])
    return list_of_file


def get_all_files_in_product_from_s3(key: str):
    s3_sentinel_1_region = 'eu-central-1'
    s3_sentinel_1_bucket_name = 'sentinel-s1-l1c'

    s3_sentinel_1 = boto3.client(
        's3',
        region_name=s3_sentinel_1_region,
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=SECRET_ID
    )

    response = s3_sentinel_1.get_object(
        Bucket=s3_sentinel_1_bucket_name, Key=key, RequestPayer='requester')

    return response['Body'].read()


def put_file_to_dest_s3(data: str, key: str):
    dest_s3_bucket_name = YOUR_BUCKET

    dest_s3 = boto3.client('s3', aws_access_key_id=ACCESS_ID,
                           aws_secret_access_key=SECRET_ID)

    dest_s3.put_object(Body=data, Bucket=dest_s3_bucket_name, Key=key)
    print(f"uploaded to '{dest_s3_bucket_name}/{key}'.")

def get_arg():
    
    import argparse

    parse = argparse.ArgumentParser()

    # MODE = YOU CAN SELECT ACTION BETWEEN SELECT DATE OR EVERYDAY
    parse.add_argument("-m","--MODE", help="MODE", required=True, type=int)
    parse.add_argument("-f","--FAC", help="FACTORY", required=True)
    # DATE START REQUIRED TRUE  
    parse.add_argument("-ds","--DAY", help="DAY START", required=True, type=int)
    parse.add_argument("-ms","--MONTH", help="MONTH START", required=True, type=int)
    parse.add_argument("-ys","--YEAR", help="YEAR START", required=True, type=int)
    # DATE END REQUIRED FALSE
    parse.add_argument("-de","--DAYEND", help="DAY END", required=False, type=int)
    parse.add_argument("-me","--MONTHEND", help="MONTH END", required=False, type=int)
    parse.add_argument("-ye","--YEAREND", help="YEAR END", required=False, type=int)
    
    args = vars(parse.parse_args())
    
    # print(args)


    return args


if __name__ == '__main__':


    # parser argument
    args  = get_arg()

    

    # Normal
    if args["MODE"] == 0 and args["FAC"].isupper():
            # utc_now = datetime.utcnow()
            start_date = datetime(args["YEAR"], args["MONTH"], args["DAY"])
            print(f"This mode 0 just have {start_date}")
            event = dict()
            event['query-date'] = start_date
            print(args)
            main(event=event, factory=args["FAC"])

    # Select Date
    if args["MODE"] == 1 and (args["YEAREND"] != None and args["MONTHEND"] != None and args["DAYEND"] != None) and args["FAC"].isupper():
        
        start_date = datetime(args["YEAR"], args["MONTH"], args["DAY"])
        end_date = datetime(args["YEAREND"], args["MONTHEND"], args["DAYEND"])
        query_date = start_date
        print(f"This mode 1 == {start_date} and {end_date}")
        while query_date <= end_date:
            # for fac in factory:
            # print(query_date)
            event = dict()
            event['query-date'] = query_date
            print(args)
            print(query_date)
            main(event=event, factory=args["FAC"])
            query_date = query_date + timedelta(days=1)
