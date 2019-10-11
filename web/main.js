const express = require('express');
const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);
const bodyParser = require('body-parser');
const fetch = require('node-fetch');
const mongoose = require('mongoose');
const URL_CB = process.env.URL_CB || "http://localhost:9001";
const PORT = process.env.PORT  ? (process.env.PORT) : 3000;
const MONGO_URI = process.env.MONGO_URI || "mongo://localhost:27017";
console.log("Orion URL: "+ URL_CB);

const connectWithRetry = () => {
	mongoose.connect(MONGO_URI, {useNewUrlParser: true, auth:{authdb:"admin"}}).then(()=>{
    console.log('MongoDB is connected to the web');
  }).catch(err=>{ 
    console.log('MongoDB connection with web unsuccessful, retry after 5 seconds.');
    setTimeout(connectWithRetry, 5000);
  })
}

const find = (name, query, cb)  => {
    mongoose.connection.db.collection(name, function (err, collection) {
    	if (err) {
    		cb(err);
    	} else {
    		collection.find(query).toArray(function(err, results){
    			console.log(err)
	       		cb(err,results);
	       	});
    	}
   	});
}

connectWithRetry()

const createAttr = (attr) => {
	return {"value": attr, "type": isNaN(attr) ? "String" : "Integer"};
}



const updateEntity = (data) => {
	console.log(data);
	fetch(URL_CB, {
		body: JSON.stringify(data),
		headers: { "Content-Type": "application/json" },
		method: "PATCH"
	})
	.then(res=> {
		console.log("Reply from Orion", res.ok)
		if (res.ok) {
			io.to(data.socketId.value).emit("messages",{type: "CONFIRMATION", payload:{ msg: "Your request is being processed"}});
			return;
		} 
		throw new Error("Error")
	})
	.catch(e=>{
		io.to(data.socketId.value).emit("messages",{type: "ERROR", payload:{ msg: "There has been a problem with your request"}});
		console.error(e);
	});
}

server.listen(PORT, function() {
	console.log("Listening on port " + PORT);
});


io.on('connection', function(socket) {
	console.log('New socket connection');
	socket.on('predict',(msg)=>{
		const { year, month, day, weekDay, time, predictionId } = msg;
		updateEntity({ 
			"year": createAttr(year),
			"month": createAttr(month),
			"day": createAttr(day),
			"weekDay": createAttr(weekDay),
			"time": createAttr(time),
			"predictionId": createAttr(predictionId),
			"socketId": createAttr(socket.id)
		});
	})
});

app.use(express.static('public'));
app.use(bodyParser.text());
app.use(bodyParser.json());

app.post("/notify",function(req,res){
	if (req.body && req.body.data) {
		req.body.data.map(({socketId, predictionId, predictionValue, year, month, day, time})=>{
			io.to(socketId.value).emit('messages', {type: "PREDICTION", payload: {
				socketId: socketId.value,
				year: year.value,
				month: month.value,
				day: day.value,
				time: time.value,
				predictionId: predictionId.value,
				predictionValue: predictionValue.value
			}});
		});
	}
	res.sendStatus(200);
});

const fromEntries = arr => Object.assign({}, ...Array.from(arr, ([k, v]) => ({[k]: v}) ));

app.get("/predictions",(req,res)=>{
	find("sth_x002f", undefined, (err, predictions)=>{
		if (err) {
			res.sendStatus(500);
		} else {
			var obj =predictions.reverse().reduce((acc,el,i)=>{
				return {...acc, [el.recvTime]: [...(acc[el.recvTime] || []),[el.attrName, el.attrValue]]}
			},{})

			for (let i in obj) {
				obj[i] = fromEntries(obj[i]);
			}
			res.json(obj);
		}
		
	});
})