activeServer = ""


window.addEventListener("DOMContentLoaded", async () => {

  
    const ServerList = document.querySelector(".demo-servers")
    const servers = []
    try {
        const response = await fetch("/get_servers");
        const data = await response.json();
        servers.push(...data);
    } catch (error) {
        console.error("Error fetching servers:", error);
    }
    console.log(servers)

    servers.forEach((server)=>{
        var div = document.createElement("div")
        div.className = "demo-server"
        div.textContent = server
        div.addEventListener("click",(e)=>{
            changeActive(e)
        })
        ServerList.appendChild(div)
    })

    setField = document.querySelector("#set-key")
    setVal = document.querySelector("#set-value")
    setBtn = document.querySelector("#set-button")
    setRes = document.querySelector("#set-result")

    setBtn.addEventListener("click",async(e)=>{
        path = "/set/" + activeServer +  "/" + setField.value + "/" + setVal.value
        res = await fetch(path)

        if (res.ok) {
            setRes.textContent = "SUCCESS"
            setRes.classList.remove("error")
            setRes.classList.add("success")
        } else {
            setRes.textContent = `Error: ${res.status} ${res.statusText}`
            setRes.classList.remove("success")
            setRes.classList.add("error")
        }

        console.log(res)
    })

    getField = document.querySelector("#get-key")
    getRes = document.querySelector("#get-result-code")
    getBtn = document.querySelector("#get-button")

    getBtn.addEventListener("click",async()=>{
        console.log("yay")
        path = "/get/" +activeServer + "/" + getField.value 
        res = await fetch(path)

        if (res.ok){
            const data = await res.json()
            getRes.textContent = JSON.stringify(data,null,2)
        }
        else{
            getRes.textContent = `Error: ${res.status} ${res.statusText}`
        }
    })

    
});

const makeInactive = () => {
    const servers = document.querySelectorAll(".demo-server");
    servers.forEach((server) => {
        if (server.classList.contains("active")) {
            server.classList.remove("active");
        }
    });
};

const changeActive = (e)=>{
    pane = document.querySelector(".demo-interface")
    console.log(pane)
    if (pane.classList.contains("disabled")){

        pane.classList.remove("disabled")
    }
    makeInactive()
    e.target.classList.add("active")
    activeServer = e.target.textContent
}
