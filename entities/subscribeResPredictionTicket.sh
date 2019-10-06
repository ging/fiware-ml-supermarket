curl -v localhost:1026/v2/subscriptions -s -S -H 'Content-Type: application/json' -d @- <<EOF
{
  "description": "A subscription to get ticket predictions",
  "subject": {
	"entities": [
  	{
    	"id": "ResTicketPrediction1",
    	"type": "ResTicketPrediction"
  	}
	],
	"condition": {
  	"attrs": [
      "predictionId",
      "socketId",
      "predictionValue"
  	]
	}
  },
  "notification": {
	"http": {
  	"url": "http://web:8080/notify"
	},
	"attrs": [
      "predictionId",
      "socketId",
      "predictionValue"
	]
  },
  "expires": "2040-01-01T14:00:00.00Z",
  "throttling": 5
}
EOF