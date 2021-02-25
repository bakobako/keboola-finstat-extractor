# Finstat Extractor for Keboola

This extractor enables you to extract data from the Finstat website (finstat.sk) to get data on companies 
in their database. The extractor requires an input table with rows containing the ICO codes of the companies.
It returns the data from described in the premium API documentation.



# Functionality notes

This extractor requires a Premium API account in finstat described here : https://finstat.sk/api

You will need a Finstat API code and a private key.

The input table requires a column with rows of ICO codes.
If a column is named "ico" it will use this, otherwise it will use the
first column of the input table

The extractor will output a list of ICO codes that were not possible to be 
retrieved from Finstat.

# Configuration

Fill in these required paramters in you extractor configuration

## Param 1 : API code 
API code - received from your Finstat account

## Param 2 : Private key 
Private key - received from your Finstat account

## Param 2 : Request type 
Based on your Finstat API account you will have access to different API calls.

For example Premium API users can only use the "detail" API calls

Elite API users can use the "extended" calls that give more data

Ultimate API users can use "ultimate" calls that give more data.

The API data retrieved can be seen in the Finstat API documentation 


## Deployment in Keboola

While the component is not published, you must add it to your project by a link (Using EU connection):

https://connection.eu-central-1.keboola.com/admin/projects/{YOUR PROJECT CODE}/extractors/phalanx-ai.finstat-extractor

Then add the input table to the table input mapping

Then fill in your API key and Private key (It will get encrypted and hidden)

Then run the component, the output will be shown in storage, you can find a link to the output in the job run overview

