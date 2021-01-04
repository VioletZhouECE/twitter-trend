import subscribeToHashtagData from "/api.js";

window.onload = initDashboard();

let hashtagChart;

const hashtagChartContainer = document.getElementById("hashtag-chart");
const loadingIcon = document.getElementById("loading");

function initDashboard(){
    subscribeToHashtagData(updateHashtagDashboard);
}

//re-render the chart everytime new data is received 
function updateHashtagDashboard(dataList){
	if (dataList == ""){
		return;
	}

	loadingIcon.style.display = "none";

    //for now only take the first 10 items in the dataList
    let hashtags = [];
    let values = [];
    dataList.slice(0,15).map(data=>{
        hashtags.push(data.split(":")[0]);
        values.push(data.split(":")[1]);
    })
	
	//destroy the old chart if it exists
	if (hashtagChart){
		hashtagChart.destroy();
	}
	
	renderChart(hashtags, values);
}

function renderChart(hashtags, values){
    hashtagChart = new Chart(hashtagChartContainer, {
		plugins: [ChartDataLabels],
    	type: 'horizontalBar',
    	data: {
        	labels: hashtags,
        	datasets: [{
            	label: '# of Mentions',
            	data: values,
            	backgroundColor: [
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
          	        'rgba(75, 192, 192, 0.2)',
                	'rgba(153, 102, 255, 0.2)',
                	'rgba(255, 159, 64, 0.2)',
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
                	'rgba(75, 192, 192, 0.2)',
					'rgba(153, 102, 255, 0.2)',
					'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
					'rgba(75, 192, 192, 0.2)',
					'rgba(153, 102, 255, 0.2)'
            	],
            	borderColor: [
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
          	        'rgba(75, 192, 192, 0.2)',
                	'rgba(153, 102, 255, 0.2)',
                	'rgba(255, 159, 64, 0.2)',
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
                	'rgba(75, 192, 192, 0.2)',
					'rgba(153, 102, 255, 0.2)',
					'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
					'rgba(75, 192, 192, 0.2)',
					'rgba(153, 102, 255, 0.2)'
            	],
            	borderWidth: 1
        	}]
    	},
    	options: {
			plugins: {
				datalabels: {
					anchor: 'end',
					align: 'right'
				}
			},
        	scales: {
	            yAxes: [{
                	ticks: {
                    	beginAtZero:true
                	}
            	}]
        	}
    	}
   });
}