curl -v orion:1026/v2/subscriptions -s -S -H 'Content-Type: application/json' -d @- <<EOF
{
  "description": "A subscription to get ticket predictions",
  "subject": {
	"entities": [
  	{
    	"id": "ReqTicketPrediction1",
    	"type": "ReqTicketPrediction"
  	}
	],
	"condition": {
  	"attrs": [
      "predictionId",
      "socketId",
      "year",
      "month",
      "day",
      "time",
      "weekDay"
  	]
	}
  },
  "notification": {
	"http": {
  	"url": "http://spark-master:9001"
	},
	"attrs": [
      "predictionId",
      "socketId",
      "year",
      "month",
      "day",
      "time",
      "weekDay"
	]
  },
  "expires": "2040-01-01T14:00:00.00Z",
  "throttling": 5
}
EOF