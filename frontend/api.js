function subscribeToHashtagData(cb){
    //connect to the hashtag namespace
    const socket = io('/hashtag');
    socket.emit("getInitData")
    socket.on("hashtagData", data => cb(data));
}

export default subscribeToHashtagData;