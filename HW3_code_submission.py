#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3 as boto


# In[5]:


s3 = boto.resource('s3', aws_access_key_id='security_redaction', aws_secret_access_key='security_redaction')


# In[6]:


try: 
    s3.create_bucket(Bucket='hw3-nosql', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}) 
except Exception as e: 
    print (e)


# In[7]:


bucket = s3.Bucket('hw3-nosql') 


# In[8]:


bucket.Acl().put(ACL='public-read') 


# In[9]:


body = open('.\exp1.csv', 'rb') 


# In[10]:


o = s3.Object('hw3-nosql', 'test').put(Body=body)


# In[11]:


s3.Object('hw3-nosql', 'test').Acl().put(ACL='public-read')


# In[14]:


dyndb = boto.resource('dynamodb', region_name='us-west-2', aws_access_key_id='security_redaction', aws_secret_access_key='security_redaction' )


# In[15]:


try: 
    table = dyndb.create_table( 
        TableName='DataTable', 
        KeySchema=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'KeyType': 'HASH' 
            }, 
            { 
                'AttributeName': 'RowKey', 
                'KeyType': 'RANGE' 
            } 
        ], 
        AttributeDefinitions=[ 
            { 
                'AttributeName': 'PartitionKey', 
                'AttributeType': 'S' 
            }, 
            { 
                'AttributeName': 'RowKey', 
                'AttributeType': 'S' 
            }, 
 
        ], 
        ProvisionedThroughput={ 
            'ReadCapacityUnits': 5, 
            'WriteCapacityUnits': 5 
        } 
    ) 
except Exception as e: 
    print (e) 
    #if there is an exception, the table may already exist.   if so... 
    table = dyndb.Table("DataTable")


# In[16]:


table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')


# In[17]:


print(table.item_count)


# In[18]:


import csv 


# In[19]:


with open('.\experiments.csv', 'r') as csvfile: 
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf: 
        print(item)
        body = open('.\\'+item[3] + '.csv', 'rb') 
        s3.Object('hw3-nosql', item[3]).put(Body=body ) 
        md = s3.Object('hw3-nosql', item[3]).Acl().put(ACL='public-read') 
         
        url = " https://s3-us-west-2.amazonaws.com/hw3-nosql/"+item[3] 
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],  
                 'description' : item[4], 'date' : item[2], 'url':url}  
        try: 
            table.put_item(Item=metadata_item) 
        except: 
            print("item may already be there or another failure")


# In[20]:


response = table.get_item( 
    Key={ 
        'PartitionKey': 'experiment3', 
        'RowKey': '4' 
    } 
) 
item = response['Item'] 
print(item)


# In[21]:


response


# In[ ]:




