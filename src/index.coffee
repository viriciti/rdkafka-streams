_         = require "underscore"
duplexify = require "duplexify"

Consumer = require "./Consumer"
Producer = require "./Producer"

getDuplex = (options) ->
	cs = new Consumer options
	ps = new Producer _.extend {}, options, awaitPartitions: true

	cs.on "partitions", ps.setPartitions

	duplexify.obj ps, cs

module.exports = {
	Consumer
	Producer
	getDuplex
}
