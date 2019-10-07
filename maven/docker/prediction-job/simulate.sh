while true
do
    month=$(shuf -i 1-11 -n 1)
    day=$(shuf -i 1-28 -n 1)
    time=$(shuf -i 0-23 -n 1)
    weekDay=$(shuf -i 0-6 -n 1)

    curl -v -s -S X POST http://localhost:9001 \
    --header 'Content-Type: application/json; charset=utf-8' \
    --header 'Accept: application/json' \
    --header 'User-Agent: orion/0.10.0' \
    --header "Fiware-Service: demo" \
    --header "Fiware-ServicePath: /test" \
    -d  '{
         "data": [
             {
                 "id": "ReqTicketPrediction1","type": "ReqTicketPrediction",
                 "year": {"type": "Float","value": 2016,"metadata": {}},
                 "month": {"type": "Float","value": '$month',"metadata": {}},
                 "day": {"type": "Float","value": '$day',"metadata": {}},
                 "weekDay": {"type": "Float","value": '$weekDay',"metadata": {}},
                 "time": {"type": "Float","value": '$time',"metadata": {}},
                 "predictionId": {"type": "Float","value": "prediction1","metadata": {}},
                 "socketId": {"type": "String","value": "socket1","metadata": {}}
             }
         ],
         "subscriptionId": "57458eb60962ef754e7c0998"
     }'
	echo $temp

    sleep 1
done
