$(function(){
	function parseTime(time) {
		return time + ":00-" + time +":59"
	}
	function createElement(prediction)  {
		let {year, month, day, time, predictionValue} = prediction;
		var options = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
		let date = new Date(year, month - 1, day).toLocaleDateString('en-US', options);
		let className= "predictions-low";
		if(predictionValue > 20 && predictionValue < 60) {
			className= "predictions-medium";
		} else if( predictionValue >= 60 ) {
			className= "predictions-high";
		}
		return `
		<tr>
		  <td>${date}</td>
		  <td>${parseTime(time)}</td>
		  <td class="prediction-value ${className}">${predictionValue}</td>
		</tr>`;
	}

	fetch("/predictions")
		.then(res=>res.json())
		.then(predictions => {
			var elements = $();
			for(let x in predictions) {
				let prediction = predictions[x];
				if (prediction.year != 0) {
			    	elements = elements.add(createElement(prediction));
				}
			}
			$('tbody').append(elements);
		});



})