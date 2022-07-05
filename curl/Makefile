base_url = http://localhost:8080

curl_list_jobs:
	curl -H 'Content-Type: application/json;charset=utf-8;' $(base_url)/jobs | jq

curl_post_job:
	curl -XPOST -H 'Content-Type: application/json;charset=utf-8;' -d @curl/publish_product.json $(base_url)/jobs

curl_delete_job:
	curl -XDELETE -H 'Content-Type: application/json;charset=utf-8;' $(base_url)/jobs/$(name)
