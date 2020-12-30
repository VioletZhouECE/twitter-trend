import subscribeToHashtagData from "/api.js";

window.onload = initDashboard();

const dashboardContainer = document.getElementById("dashboard-container")

function initDashboard(){
    subscribeToHashtagData(updateHashtagDashboard);
}

function updateHashtagDashboard(data){
    console.log("data received:", data)
    dashboardContainer.innerText = data;
}