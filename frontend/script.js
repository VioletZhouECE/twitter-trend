import {subscribeToHashtagData} from "./api";

window.onload = initDashboard();

const dashboardContainer = document.getElementById("dashboard-container")

function initDashboard(){
    subscribeToHashtagData(updateHashtagDashboard);
}

function updateHashtagDashboard(data){
    dashboardContainer.innerText = data;
}