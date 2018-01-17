_         = require "underscore"
duplexify = require "duplexify"

Consumer = require "./Consumer"
Producer = require "./Producer"

getDuplex = (options) ->
	cs = new Consumer options
	ps = new Producer _.extend {}, options, awaitPartitions: true

	duplexify.obj ps, cs

module.exports = {
	Consumer
	Producer
	getDuplex
}
