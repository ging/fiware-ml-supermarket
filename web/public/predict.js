const {$} = window;
let predictionId = Date.now();
$(function () {
	var socket = io.connect('/', { 'forceNew': true });
	function renderSpinner(){
		var html = `<img src="spinner.gif" className="spinner"/>`;
		document.getElementById('messages').innerHTML = html;

	}
	function renderAlert(data, type = "primary") {
		var html =  `<div class="alert alert-dismissible alert-${type}">
		  <button type="button" class="close" data-dismiss="alert">&times;</button>
		  ${data}
		</div>` ;
		document.getElementById('messages').innerHTML = html;
	}

	function renderResult(data) {
		var html =  `<div id="result-container">
			<div id="result">0</div>
			<div id="purchases">purchases</div>
		</div>` ;
		document.getElementById('messages').innerHTML = html;
		let countdown = 0;
		let interval = setInterval(()=>{
			if (countdown < data){
				countdown++;
				document.getElementById('messages').innerHTML = `<div id="result-container">
					<div id="result">${countdown}</div>
					<div id="purchases">purchases</div>
				</div>`;
			} else {
				interval = null;
			}
		}, 10);
		$('#result');
	}

	socket.on('messages', function(action) {
		try{
			switch(action.type) {
				case "CONFIRMATION":
					renderSpinner();
					break;
				case "ERROR":
					renderAlert(action.payload.msg, "danger");
					break;
				case "PREDICTION":
				console.log(action.payload.predictionId,predictionId)
					if (predictionId === (action.payload.predictionId)) {
						renderResult(action.payload.predictionValue);
					}
					break;
				default:
					console.error("Unrecognized message type");
			}
		} catch (e) {
			console.error(e)
		}
	});


	const updateDate = (date) => {
	    date.setHours(0,0,0,0)
	    $("#date").val(date);
	    $("#start").focus();
	};
	const getQueryStringValue = (key) => decodeURIComponent(window.location.search.replace(new RegExp(`^(?:.*[&\\?]${encodeURIComponent(key).replace(/[\.\+\*]/g, "\\$&")}(?:\\=([^&]*))?)?.*$`, "i"), "$1"));

    $.datepicker.setDefaults({
        "firstDay": 1,
        "isRTL": false,
        "showMonthAfterYear": false,
        "yearSuffix": "",
        "dateFormat": "dd/mm/yy",
    });
    $.datepicker.setDefaults($.datepicker.regional.es);
    $("#datepicker").datepicker({
        "onSelect" () {
            const date = $("#datepicker").datepicker("getDate");
            updateDate(date);
        },
        "beforeShowDay" (date) {
            if ($.inArray(date.getTime(), window.selectedDates) !== -1) {
                return [
                    true,
                    "turn-day-highlight"
                ];
            }
            return [
                true,
                "",
                ""
            ];
        }
    });
    const myDate = new Date(getQueryStringValue("date"));
    if (myDate && !isNaN(myDate.getDate())) {
        $("#datepicker").datepicker("setDate", new Date(myDate));
    }
    const date = $("#datepicker").datepicker("getDate");
    updateDate(date);
    $("#newForm").on("submit", function () {
        const dateSubmitted = new Date($("#date").val());
        const time = parseInt($('#hourSelect').val());
        predictionId = "p-" + Date.now();
        const msg = {
        	year: dateSubmitted.getUTCFullYear(),
        	month: dateSubmitted.getUTCMonth()+1,
        	day: dateSubmitted.getUTCDate()+1,
        	weekDay: dateSubmitted.getDay(),
        	time: time,
        	predictionId: predictionId
        }
        socket.emit("predict", msg);
        return false;
    });
});