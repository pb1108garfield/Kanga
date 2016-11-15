/* Copyright 2015-2016 Samsung Electronics Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var kangaBaseNode = require('../common/kanga-base-node');
var extend = require('../../utils/kanga-common').extend;
var KANGA_EVENT = require('../../constants/kanga-event-type');
var splitByCommaAndDot = require('../utils/str-splitter').splitByCommaAndDot;
var getJSONValue = require('../utils/json-util').getJSONValue;
var putJSONValue = require('../utils/json-util').putJSONValue;
var clone = require('../../utils/kanga-common').clone;

var NO_GROUP = 'NO_GROUP';
/**
 * Initialize the node parameters
 * @constructor
 */
function Calculate (params) {
	kangaBaseNode.call(this, params);
	this.windowType = params.window_type;
	this.windowUnit = params.window_unit;
	this.windowSize = Number(params.window_size);
	this.groupByFields = splitByCommaAndDot(params.groupby_fields);
	this.inputFieldKeys = splitByCommaAndDot(params.input_field_name);
	this.outputEventType = params.output_event_type; // aggkeyList
	this.calculateOptions = params.calculate_options.split(',');

	this.eventCount = 0;
	this.eventQueue = [];
	this.output = [];
	this.groupKeyValue = {};

	this.aggregators = {};
	this.poppedEvents = {};
	this.timeUnit = 0;
	if (this.windowUnit === 'SECOND') {
		this.timeUnit = 1000;
	} else if (this.windowUnit === 'MINUTE') {
		this.timeUnit = 60000;
	} else {
		this.timeUnit = 3600000;
	}
}

extend(Calculate, kangaBaseNode);

/**
 * Handle the incoming event
 * @returns {Object}
 */
Calculate.prototype._execute = function () {
	var aggResult = [];
	switch (this.eventType) {
	case KANGA_EVENT.DATA:
		if (this.addNewEvent()) {
			var results = this.aggregate();
			if (results && results.length > 0) {
				for (var i = 0; i < results.length; i++) {
					this.output.push(results[i]);
				}
				aggResult = this.getResult();
				this.output = [];
			}
			if (this.windowType == 'BATCH' && this.windowUnit == 'TICK') {
				this.clear();
			}
		}
		break;
	case KANGA_EVENT.TIME_TICK:
		if (this.eventCount > 0 && this.windowType == 'BATCH'
				&& this.windowUnit != 'TICK') {
			var results = this.aggregate();
			if (results && results.length > 0) {
				for (var i = 0; i < results.length; i++) {
					this.output.push(results[i]);
				}
				aggResult = this.getResult();
				this.output = [];
			}
			this.clear();
		}
		break;
	case KANGA_EVENT.EOF:
		if (this.eventCount > 0 && this.windowType == 'BATCH'
				&& this.windowUnit != 'TICK') {
			var results = this.aggregate();
			if (results && results.length > 0) {
				for (var i = 0; i < results.length; i++) {
					this.output.push(results[i]);
				}
				aggResult = this.getResult();
				aggResult.push(this.event);
				this.output = [];
			}
			this.clear();
		} else {
			aggResult.push(this.event);
		}
		break;
	case KANGA_EVENT.NONE:
	case KANGA_EVENT.SYSTEM_LOG:
		aggResult = [];
		aggResult.push(this.event);
		break;
	default:
		return null;
	}
	return aggResult;
};

/**
 * Get aggregate result
 * @returns {Array}
 */
Calculate.prototype.aggregate = function () {
	var results = [];

	var timestamp = Date.now();
	for ( var groupKey in this.aggregators) {
		var message = this.aggregators[groupKey].aggregate();
		if (!this.isEmpty(message)) {
			var no_group = this.groupKeyValue[this.NO_GROUP];
			if (no_group && no_group == '') {
				// do nothing
			} else {
				var values = this.groupKeyValue[groupKey];
				var i = 0;
				for ( var key in this.groupByFields) {
					putJSONValue(message, this.groupByFields[key], values[i++]);
				}
			}
			results.push(message);
		}
	}
	return results;
};

/**
 * Generate result in JSON format
 * @returns {Array}
 */
Calculate.prototype.getResult = function () {
	var result = [];
	var obj = new Object();
	var root = new Object();
	var header = new Object();

	header.log = '';
	header.timestamp = Date.now();
	header.name = this.outputName;
	root._header_ = header;

	switch (this.outputEventType) {
	case 'COLLECTION':
		header.type = KANGA_EVENT.COLLECTION;
		var message = [];
		for (var i = 0; i < this.output.length; i++) {
			message.push(this.output[i]);
		}
		root[header.name] = message;
		obj.root = root;
		result.push(obj);
		break;
	case 'DATA':
		header.type = KANGA_EVENT.DATA;
		for (var j = 0; j < this.output.length; j++) {
			root[header.name] = this.output[j];
			obj.root = root;
			result.push(clone(obj));
		}
		break;
	default:
		break;
	}
	return result;
};

/**
 * Check if object is empty
 * @param {Object} obj
 * @returns {Boolean}
 */
Calculate.prototype.isEmpty = function (obj) {
	if (obj == null)
		return true;
	for ( var prop in obj) {
		if (obj.hasOwnProperty(prop))
			return false;
	}
	return true;
};

/**
 * Handle new coming event and return whether window size is full
 * @returns {Boolean}
 */
Calculate.prototype.addNewEvent = function () {
	this.eventCount++;
	var keys = this.getGroupByKeys(this.message);
	this.eventOnPushed(keys, this.message);
	if (this.windowType === 'SLIDING') {
		this.eventQueue.push(this.event);
		if (this.windowUnit === 'TICK') {
			if (this.eventQueue.length > this.windowSize) {
				var popped = this.eventQueue.shift();
				this.eventOnPopped(this
						.getGroupByKeys(popped.root[popped.root._header_.name]),
						popped.root[popped.root._header_.name]);
			}
		} else {
			var firstTimestamp = this.eventQueue[0].root._header_.timestamp;
			var curTimestamp = this.header.timestamp;
			while ((curTimestamp - firstTimestamp) > (this.windowSize * this.timeUnit)) {
				var popped = this.eventQueue.shift();
				this.eventOnPopped(this
						.getGroupByKeys(popped.root[popped.root._header_.name]),
						popped.root[popped.root._header_.name]);
				firstTimestamp = this.eventQueue[0].root._header_.timestamp;
			}
		}
		this.popEvents();
		this.poppedEvents = {};
		return true;
	} else {
		if (this.windowUnit === 'TICK' && this.eventCount >= this.windowSize)
			return true;
		return false;
	}
};

/**
 * pop the popped events
 */
Calculate.prototype.popEvents = function () {
	for ( var key in this.poppedEvents) {
		var agg = this.aggregators[key];
		agg.eventsOnPopped(this.poppedEvents[key]);
		if (agg.isEmpty()) {
			delete this.aggregators[key];
		}
	}
};

/**
 * Add popped message to the map
 * @param {string} keys
 * @param {Object} message
 */
Calculate.prototype.eventOnPopped = function (keys, message) {
	var popList = this.poppedEvents[keys];
	if (!popList) {
		popList = [];
		this.poppedEvents[keys] = popList;
	}
	popList.push(message);
};

/**
 * Push message to the map
 * @param {string} keys
 * @param {Object} message
 */
Calculate.prototype.eventOnPushed = function (keys, message) {
	var aggregator = this.aggregators[keys];
	if (!aggregator) {
		aggregator = new CalculateImp(this.inputFieldKeys, this.windowType,
				this.calculateOptions);
		this.aggregators[keys] = aggregator;
	}
	aggregator.eventOnPush(message);
};

/**
 * Generate keys for message
 * @param {Object} message
 * @returns {string} - the generated key
 */
Calculate.prototype.getGroupByKeys = function (message) {
	var key = '';
	if (this.groupByFields.length == 0) {
		key = NO_GROUP;
		this.groupKeyValue[key] = '';
		return key;
	}

	var values = [];
	this.groupByFields.forEach(function (item, index, array) {
		var value = getJSONValue(message, item);
		key = key + value + "$";
		values.push(value);
	});

	this.groupKeyValue[key] = values;
	return key;
};

/**
 * reset node status
 */
Calculate.prototype.clear = function () {
	this.eventCount = 0;
	this.eventQueue = [];
	for ( var key in this.aggregators) {
		this.aggregators[key].eventsOnCleared();
	}
	this.aggregators = {};
}
/* ###################################################### */
var SUM = 1;
var COUNT = 1 << 1;
var MAX = 1 << 2;
var MIN = 1 << 3;
var AVERAGE = 1 << 4;
var STDEV = 1 << 5;
var _ALL = 1 | SUM | COUNT | MAX | MIN | AVERAGE | STDEV;
/**
 * Initialize parameters
 */
var CalculateImp = function (aggkeyList, windowType, calculateOptions) {
	this.aggkeyList = aggkeyList;
	this.aggSize = aggkeyList.length;
	this.sumList = new Array(this.aggSize);
	this.countList = new Array(this.aggSize);
	this.powerSumList = new Array(this.aggSize);
	this.minList = new Array(this.aggSize);
	this.maxList = new Array(this.aggSize);

	this.calculateOption = 0;

	for (var i = 0; i < calculateOptions.length; i++) {
		switch (calculateOptions[i].toUpperCase()) {
		case 'SUM':
			this.calculateOption |= SUM;
			break;
		case 'COUNT':
			this.calculateOption |= COUNT;
			break;
		case 'MAX':
			this.calculateOption |= MAX;
			break;
		case 'MIN':
			this.calculateOption |= MIN;
			break;
		case 'AVERAGE':
			this.calculateOption |= AVERAGE;
			break;
		case 'STDEV':
			this.calculateOption |= STDEV;
			break;
		default:
			break;
		}
	}

	if (this.calculateOption == 0) {
		this.calculateOption = _ALL;
	}

	this.eventStored = false;
	if (windowType == 'SLIDING'
			&& ((this.calculateOption & MIN) > 0 || (this.calculateOption & MAX) > 0)) {
		this.eventStored = true;
	}

	this.reset();
	if (this.eventStored) {
		this.events = [];
	}
};

/**
 * get aggregate result in JSON format
 * @returns {Object}
 */
CalculateImp.prototype.aggregate = function () {
	var message = {};
	for (var i = 0; i < this.aggSize; i++) {
		var result = 0.0;
		var keys = this.aggkeyList[i].slice(0, this.aggkeyList[i].length);
		var lastName = keys[keys.length - 1];
		if (this.countList[i] > 0) {
			if (this.needToCalculate(SUM)) {
				keys[keys.length - 1] = lastName + '_sum';
				putJSONValue(message, keys, this.sumList[i]);
			}

			if (this.needToCalculate(AVERAGE)) {
				keys[keys.length - 1] = lastName + '_avg';
				putJSONValue(message, keys, this.getAverage(this.sumList[i],
						this.countList[i]));
			}

			if (this.needToCalculate(STDEV)) {
				keys[keys.length - 1] = lastName + '_stdev';
				putJSONValue(message, keys, this.getStdev(this.sumList[i],
						this.powerSumList[i], this.countList[i]));
			}

			if (this.needToCalculate(MIN)) {
				keys[keys.length - 1] = lastName + '_min';
				putJSONValue(message, keys, this.minList[i]);
			}

			if (this.needToCalculate(MAX)) {
				keys[keys.length - 1] = lastName + '_max';
				putJSONValue(message, keys, this.maxList[i]);
			}
		}

		if (this.needToCalculate(COUNT)) {
			keys[keys.length - 1] = lastName + '_count';
			putJSONValue(message, keys, this.countList[i]);
		}
	}
	return message;
};

/**
 * reset aggregate results
 */
CalculateImp.prototype.reset = function () {
	for (var i = 0; i < this.aggSize; i++) {
		this.sumList[i] = 0.0;
		this.powerSumList[i] = 0.0;
		this.countList[i] = 0;
		this.minList[i] = Number.POSITIVE_INFINITY;
		this.maxList[i] = Number.NEGATIVE_INFINITY;
	}
};

/**
 * push the message and calculate the aggregate result 
 * @param {Object} message
 */
CalculateImp.prototype.eventOnPush = function (message) {
	if (this.eventStored) {
		this.events.push(message);
	}
	for (var i = 0; i < this.aggSize; i++) {
		var inputFieldName = this.aggkeyList[i];
		var inputValue = Number(getJSONValue(message, inputFieldName));
		this.sumList[i] += inputValue;
		this.powerSumList[i] += Math.pow(inputValue, 2);
		this.countList[i] += 1;
		if (this.maxList[i] < inputValue) {
			this.maxList[i] = inputValue;
		}
		if (this.minList[i] > inputValue) {
			this.minList[i] = inputValue;
		}
	}
};

/**
 * check if the option should be calculated
 * @param {Object} option
 * @returns {Boolean}
 */
CalculateImp.prototype.needToCalculate = function (option) {
	return (this.calculateOption & option) > 0;
};

/**
 * pop the popped messages and calculate the aggregate result
 * @param {Array} messageList
 */
CalculateImp.prototype.eventsOnPopped = function (messageList) {
	var needToRescan = false;
	for (var j = 0; j < messageList.length; j++) {
		for (var i = 0; i < this.aggSize; i++) {
			if (this.countList[i] == 0)
				break;
			var inputFieldName = this.aggkeyList[i];
			var inputValue = Number(getJSONValue(messageList[j], inputFieldName));
			this.sumList[i] -= inputValue;
			this.powerSumList[i] -= Math.pow(inputValue, 2);
			this.countList[i] -= 1;
			if (this.maxList[i] <= inputValue || this.minList[i] >= inputValue) {
				needToRescan = true;
				break;
			}
		}

		if (this.eventStored) {
			var index = this.indexOf(this.events, messageList[j]);
			if (index > -1)
				this.events.splice(index, 1);
		}
	}

	if (needToRescan && this.eventStored) {
		this.reset();
		for (var m = 0; m < this.events.length; m++) {
			for (var i = 0; i < this.aggSize; i++) {
				var inputFieldName = this.aggkeyList[i];
				var inputValue = Number(getJSONValue(this.events[m], inputFieldName));
				this.sumList[i] += inputValue;
				this.powerSumList[i] += Math.pow(inputValue, 2);
				this.countList[i] += 1;
				if (this.maxList[i] < inputValue) {
					this.maxList[i] = inputValue;
				}
				if (this.minList[i] > inputValue) {
					this.minList[i] = inputValue;
				}
			}
		}
	}
};

/**
 * Get the value index in the array
 * @param {Arrary} array
 * @param {Object} val
 * @returns {Number}
 */
CalculateImp.prototype.indexOf = function (array, val) {
	for (var i = 0; i < array.length; i++) {
		if (array[i] == val)
			return i;
	}
	return -1;
};

CalculateImp.prototype.eventsOnCleared = function () {
	this.reset();
	if (this.eventStored) {
		this.events = [];
	}
};

/**
 * check if the aggregate result is empty
 * @returns {Boolean}
 */
CalculateImp.prototype.isEmpty = function () {
	for (var i = 0; i < this.aggSize; i++) {
		if (this.countList[i] > 0)
			return false;
	}
	return true;
};

/**
 * Get average result
 * @param {Number} sum
 * @param {Number} count
 * @returns {Number}
 */
CalculateImp.prototype.getAverage = function (sum, count) {
	if (count == 0)
		return 0;
	return sum / count;
};

/**
 * Get stdev result
 * @param {Number} sum
 * @param {Number} powerSum
 * @param {Number}  count
 * @returns {Number}
 */
CalculateImp.prototype.getStdev = function (sum, powerSum, count) {
	if (count == 0)
		return 0;
	var variance = powerSum / count - Math.pow(this.getAverage(sum, count), 2);
	return Math.sqrt(variance);
};

module.exports = Calculate;