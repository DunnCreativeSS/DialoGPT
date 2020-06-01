	var ProxyLists = require('proxy-lists');
var rp = require('request-promise');
var fs = require('fs')
var SocksProxyAgent = require('socks-proxy-agent');
var HttpsProxyAgent = require('https-proxy-agent');


var finalProxies = []
var initialProxies = []
var tempProxies = []

setInterval( function() {
	console.log('done')
	tempProxies = initialProxies
	
		console.log(tempProxies.length)	

	for (var p in tempProxies){
		var proxy = tempProxies[p]
		var agent = new SocksProxyAgent(proxy);

		var options = {
			timeout: 3000,
		    uri: 'https://api.ipify.org?format=json',
		    headers: {
		        'User-Agent': 'Request-Promise'
		    },
			agent:agent
		}
	testProxy(options, tempProxies.length, proxy, 0)
	}
}, 15000);

function getProxies(){

var options = {
    protocols: ['socks4']
    

};


var gettingProxies = ProxyLists.getProxies(options);
 
gettingProxies.on('data', function(ps) {

for (var p in ps){
	for (var proto in ps[p].protocols){
	if(!initialProxies.includes(ps[p].protocols[proto] + '://' + ps[p].ipAddress + ':' + ps[p].port.toString())){
		initialProxies.push(ps[p].protocols[proto] + '://' + ps[p].ipAddress + ':' + ps[p].port.toString())
	}
}
}

});
 
gettingProxies.on('error', function(error) {
    // Some error has occurred.
 //   console.error(error);

});

gettingProxies.on('end', function(error) {
    // Some error has occurred.
 //   console.error(error);
 
    setTimeout(function(){
    	initialProxies = []
    	getProxies()
    }, 1 * 1000)
});
 


}
function testProxy(options, l, proxy, count){
setTimeout(async function(){
	try {	    
		var response = await rp(options)
	    //console.log('')
	    //console.log(proxy.split('://')[1].split(':')[0])
	    //console.log(JSON.parse(response).ip)
	    if (JSON.parse(response).ip == proxy.split('://')[1].split(':')[0]){
		
		count++
		console.log(count)
		if (count <= 5){
		setTimeout(function(){
		return testProxy(options, l, proxy, count)
	}, 500)
	} 
	
	else if (count > 5){
		finalProxies.push(proxy)
		console.log(finalProxies)

		return testProxy(options, l, proxy, count)
}else {
		count = 0
		finalProxies = finalProxies.splice(proxy, 1)
		
	}
}
	} catch(err) {
		count = 0
		finalProxies = finalProxies.splice(proxy, 1)
	    //console.log(err.message)
	}

}, Math.random * l * 1000)
}
getProxies()

/*
var hosts = ""; // HTTP proxies go here, in the format host:port separated by a single space.

function FindProxyForURL(url, host)
{
    var hostsArray = hosts.split(" ");
    var randomIndex = Math.floor((Math.random() * hostsArray.length));
    return "PROXY " + hostsArray[randomIndex] + "; DIRECT"; // DIRECT makes the browser use no proxy if the chosen proxy doesn't work
}

*/
var goagain = true

setInterval(function(){
	if (goagain == true){
		goagain = false
		var text = 'var hosts="' 
		
		for (var p in finalProxies){
			text+= finalProxies[p] + " "
		}
		text += '"\nfunction FindProxyForURL(url, host){'

	//	text += '\nvar hostsArray = hosts.split(" ").split("://")[1];'
	//	text+= '\nvar randomIndex = Math.floor((Math.random() * hostsArray.length));'
if (finalProxies.length>0){		
host = finalProxies[Math.floor((Math.random() * finalProxies.length))]
		if (host.split('://')[0] == 'http'){
			proto = 'PROXY'
		}
		else if (host.split('://')[0] == 'https'){
                        proto = 'HTTPS'
                }
                else if (host.split('://')[0].indexOf('socks') != -1){
                        proto = 'SOCKS'
                }
                host = host.split('://')[1]
text+= '\nreturn "' + proto + ' ' + host + ';" }' // DIRECT makes the browser use no proxy if the chosen proxy doesn't work


		fs.writeFileSync('/var/www/html/proxies_temp.PAC',text,{encoding:'utf8',flag:'w'})
		copyFile()
}		
goagain = true
	}
   	}, 50)


function copyFile(){
try {
		fs.copyFileSync('/var/www/html/proxies_temp.PAC', '/var/www/html/proxies.PAC')
	}
	catch (err){
		setTimeout(async function(){
		copyFile()
	}, 50)
	}
}