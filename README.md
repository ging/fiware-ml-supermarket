# fiware-global-summit-berlin-2019-ml

* Clone this project
```shell
git clone https://github.com/sonsoleslp/fiware-global-summit-berlin-2019-ml
cd fiware-global-summit-berlin-2019-ml
```

* Run the whole scenario
```shell
docker-compose up
```

* Create entites
```shell
sh entities/createPredictionEntities.sh
```

* Train prediction model
```
TODO: Right now run in IntelliJ TrainingJob.scala
```

* Submit job to Spark
```shell
TODO: Right now run in IntelliJ PredictionJob.scala
Once it's done change the notify url in entities/subscribeReqPredictionTicket.sh to the spark container name
Also, add packaging instructions and submit using spark-submit
```

* Subscribe to predictions 
```shell
sh entities/subscribeResPredictionTicket.sh
```

* Subscribe to prediction requests
```shell
sh entities/subscribeReqPredictionTicket.sh
```