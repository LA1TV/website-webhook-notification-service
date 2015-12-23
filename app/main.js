var mysql = require("mysql");
var redis = require('redis');
var request = require('request');
var config = require("../config.json");

var redisClient = null;
var mysqlCon = null;

console.log("Loading...");

Promise.all([connectRedis(), connectMysql()]).then(function(results) {
	redisClient = results[0];
	mysqlCon = results[1];
	redisClient.on("message", function(channel, message) {
		var data = JSON.parse(message);
		if (channel === "mediaItemLiveChannel") {
			sendWebhooks(data.eventId, data.payload);
		}
		else if (channel === "testChannel") {
			getUserWebhookUrl(data.apiUserId).then(function(url) {
				sendWebhook(url, "test", {success: true});
			});
		}
	});

	redisClient.subscribe("mediaItemLiveChannel");
	redisClient.subscribe("testChannel");

	console.log("Loaded.");
});

function connectRedis() {
	var client = redis.createClient(config.redis.port, config.redis.host);
	return new Promise(function(resolve) {
		client.auth(config.redis.password, function() {
			resolve(client);
		});
	});
}

function connectMysql() {
	return new Promise(function(resolve) {
		var connection = mysql.createConnection({
			host: config.mysql.host,
			port: config.mysql.port,
			user: config.mysql.user,
			password: config.mysql.password,
			database: config.mysql.database
		});
		connection.connect(function(err) {
			if (err) throw(err);
			resolve(connection);
		});
	});
}

function sendWebhooks(eventId, payload) {
	return getWebhookUrls().then(function(urls) {
		return Promise.all(urls.map(function(url) {
			return sendWebhook(url, eventId, payload);
		}));
	});
}

function sendWebhook(url, eventId, payload) {
	var completeData = {
		eventId: eventId,
		payload: payload,
		time: Date.now()
	}
	console.log('Making request for event id "'+eventId+'" to "'+url+'".');
	return new Promise(function(resolve) {
		request({
			uri: url,
			method: "POST",
			body: completeData,
			json: true,
			timeout: 10000
		}, function(error, response, body) {
			if (error) {
				console.log('Error when making request for event id "'+eventId+'" to "'+url+'".');
			}
			else {
				console.log('Got response code '+response.statusCode+' when making request for event id "'+eventId+'" to "'+url+'".');
			}
			resolve();
		});
	});
}

function getWebhookUrls() {
	return new Promise(function(resolve) {
		mysqlCon.query('SELECT webhook_url FROM api_users WHERE enabled=1 AND can_use_webhooks=1 AND webhook_url IS NOT NULL', function(err, results) {
  			if (err) throw(err);
  			resolve(results.map(function(a) {
  				return a.webhook_url;
  			}));
  		});
  	});
}

function getUserWebhookUrl(id) {
	return new Promise(function(resolve, reject) {
		mysqlCon.query('SELECT webhook_url FROM api_users WHERE id=? AND enabled=1 AND can_use_webhooks=1 AND webhook_url IS NOT NULL', [id], function(err, results) {
  			if (err) throw(err);
  			if (results.length  > 0) {
  				resolve(results[0].webhook_url);
  			}
  			else {
  				reject();
  			}
  		});
  	});
}