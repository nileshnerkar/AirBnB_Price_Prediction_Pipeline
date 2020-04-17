import requests, patoolib
import urllib
import re, glob, os
import pandas as pd, boto3

from datetime import datetime
from bs4 import BeautifulSoup

url = 'http://insideairbnb.com/get-the-data.html'
input_bucket = 'airbnb-final-project'
s3_folder_name = 'Staging'
s3_archive_name = 'Archive'
downloaded_directory = '/tmp/Downloads/'

def lambda_handler(event, context):
    
    #1. Get the most recent download link for New York City
    response = requests.get(url=url)
    soup = BeautifulSoup(response.text, 'html.parser')
    list_of_links = soup.find_all("table", {'class':'new-york-city'})[0].find_all("a", href=re.compile("listings.csv.gz"))

    download_list = []
    for link in list_of_links:
        download_list.append(link.attrs['href'])

    recent_download_link = sorted(download_list, key= lambda x: datetime.strptime(x.split('/')[6],"%Y-%m-%d"), reverse=True)[0]
    print(recent_download_link)
    
    #2. Download most recent file and extract it
    # wget.download(url=recent_download_link, out='/tmp/')
    urllib.request.urlretrieve(recent_download_link, '/tmp/listings.csv.gz')
    patoolib.extract_archive('/tmp/listings.csv.gz',outdir='/tmp/')
    #downloaded_files = glob.glob(os.getcwd()+'\\*.csv.gz')
    
    #3. Pre-Processing and Copying to Staging Directory and Remove original File
    df = pd.read_csv('/tmp/listings.csv', low_memory=False)

    cols_to_drop = ['summary','name','space','description',
                    'neighborhood_overview','notes','transit','access',
                    'interaction','house_rules','host_about','host_location',
                    'host_verifications','street','smart_location','amenities']

    df = df.drop(labels=cols_to_drop, axis=1)


    if not os.path.exists(downloaded_directory):
        os.mkdir(downloaded_directory)

    new_file_name = recent_download_link.split('/')[6] + '.csv'
    df.to_csv(downloaded_directory + new_file_name, sep="|", index=False)

    if os.path.exists('/tmp/listings.csv.gz'):
        os.remove('/tmp/listings.csv.gz')
        os.remove('/tmp/listings.csv')

    s3_client = boto3.client('s3')
    
    #5. Check if bucket exits, if not then create it
    bucket_exists = False

    for bucket in s3_client.list_buckets()['Buckets']:
        if bucket['Name'] == input_bucket:
            bucket_exists = True

    if not bucket_exists:
        s3_client.create_bucket(Bucket=input_bucket, ACL='public-read')
        print('Bucket {} created'.format(input_bucket))
       
    
    old_file_name = ''
    try:
        #Get old file name
        for obj in s3_client.list_objects(Bucket=input_bucket)['Contents']:
            if s3_folder_name + '/' in obj['Key']:
                old_file_name = obj['Key'].split('/')[1]

        if old_file_name != new_file_name:
            #Copy old file to Archive
            copy_source = {'Bucket': input_bucket, 'Key': s3_folder_name+"/"+old_file_name}
            s3_client.copy_object(Bucket=input_bucket, 
                                  CopySource=copy_source,
                                  Key=s3_archive_name+"/"+old_file_name)

            #Delete old file from Staging
            s3_client.delete_object(Bucket=input_bucket, 
                                    Key=s3_folder_name+"/"+old_file_name)

            #Uploading new file to Staging
            s3_client.upload_file(Bucket=input_bucket, 
                                  Filename=downloaded_directory + new_file_name, 
                                  Key=s3_folder_name+'/'+new_file_name, ExtraArgs={'ACL': 'public-read'})
        else:
            print('No new files present to upload')

    except Exception:
        print("Initial File Uploaded --> {}".format(new_file_name))
        s3_client.upload_file(Bucket=input_bucket, 
                              Filename=downloaded_directory + new_file_name, 
                              Key=s3_folder_name+'/'+new_file_name, ExtraArgs={'ACL': 'public-read'})