var express = require('express');
var app = express();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var bodyParser = require('body-parser');
var fetch = require('node-fetch');
const URL_CB = process.env.URL_CB || "http://localhost:9001";
const PORT = process.env.PORT  ? parseInt(process.env.port) : 8080;
console.log("Orion URL: "+ URL_CB);

const createAttr = (attr) => {
	return {"value": attr, "type": isNaN(attr) ? "String" : "Integer"};
}

const updateEntity = (data) => {
	console.log(data);
	fetch(URL_CB, {
		body: JSON.stringify(data),
		headers: {
			"Content-Type": "application/json"
		},
		method: "PATCH"
	})
	.then(res=> {
		if (res.ok) {
			console.log()
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
	console.log('New connection');
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

// parse application/x-www-form-urlencoded
app.use(bodyParser.text());
 
// parse application/json
app.use(bodyParser.json());

app.post("/notify",function(req,res){
	if (req.body && req.body.data) {
		req.body.data.map(({socketId,predictionId,predictionValue})=>{
			io.to(socketId.value).emit('messages', {type: "PREDICTION", payload: {
				socketId: socketId.value,
				predictionId: predictionId.value,
				predictionValue: predictionValue.value}});
		})
	}
	res.send(200);
});
