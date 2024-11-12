curl --request PUT \
  --url 'http://localhost:8108/personalization/models/1' \
  --header 'Content-Type: application/octet-stream' \
  --header 'X-TYPESENSE-API-KEY: xyz' \
  -T /home/hari/model.tar.gz


curl --request PUT \
  --url 'http://localhost:8108/personalization/models/1?name=ts/tyrec-2&collection=companies' \
  --header 'Content-Type: application/octet-stream' \
  --header 'X-TYPESENSE-API-KEY: xyz';

curl --request DELETE \
  --url http://localhost:8108/personalization/models/1 \
  --header 'X-TYPESENSE-API-KEY: xyz' \
  --header 'content-type: text/plain'


curl --request POST \
  --url http://localhost:8108/collections \
  --header 'Content-Type: application/json' \
  --header 'X-TYPESENSE-API-KEY: xyz' \
  --data '{
   "name": "companies",
   "fields": [
     {"name": "company_name", "type": "string" },
     {"name": "num_employees", "type": "int32" },
     {"name": "country", "type": "string", "facet": true }
   ],
   "default_sorting_field": "num_employees"
  
}'

curl --request POST \
  --url 'http://localhost:8108/personalization/models?name=ts/tyrec-1&collection=companies&id=1&type=recommendation' \
  --header 'Content-Type: application/octet-stream' \
  --header 'X-TYPESENSE-API-KEY: xyz' \
  -T /home/hari/model.tar.gz