import datetime

DATE_PIPELINE = datetime.datetime.now().date().strftime("%Y_%m_%d")

ACCESS_TOKEN = 'DefaultEndpointsProtocol=https;AccountName=brewery;AccountKey=pKvUu+yrAIgIAukVXWjsIuBMzkNj9bk/EJCu6LJCVCdPN9WBj3KFZklQiArIDSqxKDiNO76wdxbe+ASt3zMrhg==;EndpointSuffix=core.windows.net'

ACCESS_KEY = 'pKvUu+yrAIgIAukVXWjsIuBMzkNj9bk/EJCu6LJCVCdPN9WBj3KFZklQiArIDSqxKDiNO76wdxbe+ASt3zMrhg=='

ACCOUNT_NAME = 'brewery'

URL_API = 'https://api.openbrewerydb.org/v1/breweries'

BRONZE_CONTAINER = 'raw'

SILVER_CONTAINER = 'transformed'

GOLD_CONTAINER = 'grouped'