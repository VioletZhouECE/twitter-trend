import subscribeToHashtagData from "/api.js";

window.onload = initDashboard();

let myChart;

const hashtagChart = document.getElementById("hashtag-chart");

function initDashboard(){
    subscribeToHashtagData(updateHashtagDashboard);
}

//re-render the chart everytime new data is received 
function updateHashtagDashboard(dataList){
    //for now only take the first 10 items in the dataList
    let hashtags = [];
    let values = [];
    dataList.slice(0,10).map(data=>{
        hashtags.push(data.split(":")[0]);
        values.push(data.split(":")[1]);
    })
    
    //rerender the chart
    myChart = new Chart(hashtagChart, {
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
                	'rgba(153, 102, 255, 0.2)'
            	],
            	borderColor: [
                	'rgba(255,99,132,1)',
                	'rgba(54, 162, 235, 1)',
        	        'rgba(255, 206, 86, 1)',
                	'rgba(75, 192, 192, 1)',
                	'rgba(153, 102, 255, 1)',
                	'rgba(255, 159, 64, 1)',
                	'rgba(255,99,132,1)',
                	'rgba(54, 162, 235, 1)',
                	'rgba(255, 206, 86, 1)',
                	'rgba(75, 192, 192, 1)',
                	'rgba(153, 102, 255, 1)'
            	],
            	borderWidth: 1
        	}]
    	},
    	options: {
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