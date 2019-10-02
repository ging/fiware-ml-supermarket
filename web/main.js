var express = require('express');
var app = express();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var bodyParser = require('body-parser')
var fetch = require('node-fetch');

const updateEntity = (data) => {
	console.log(data)
	fetch("http://localhost:1026/v2/entities/ReqTicketPrediction1/attrs", {
		body: JSON.stringify(data),
		headers: {
			"Content-Type": "application/json"
		},
		method: "PATCH"
	})
	.then(res=>res.json())
	.then(res=> {
		io.to(data.socketId).emit("messages",{type: "CONFIRMATION", payload:{ msg: "Your request is being processed"}});
		console.log(res)
	})
	.catch(e=>{
		io.to(data.socketId).emit("messages",{type: "ERROR", payload:{ msg: "There has been a problem with your request"}});
		console.log(e);
	})
}

server.listen(8080, function() {
});


io.on('connection', function(socket) {
	console.log('New connection');
	socket.on('predict',(msg)=>{
		updateEntity({...msg, socketId: socket.id})
	})
});

app.use(express.static('public'));

// parse application/x-www-form-urlencoded
app.use(bodyParser.text())
 
// parse application/json
app.use(bodyParser.json())

app.post("/notify",function(req,res){
	if (req.body && req.body.socketId) {
		io.to(req.body.socketId).emit('messages', {type: "PREDICTION", payload: req.body});
	}
	res.send(200)	
})
