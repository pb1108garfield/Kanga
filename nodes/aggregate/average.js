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
function Average (params) {
	kangaBaseNode.call(this, params);
	this.windowType = params.window_type;
	this.windowUnit = params.window_unit;
	this.windowSize = Number(params.window_size);
	this.groupByFields = splitByCommaAndDot(params.groupby_fields);
	this.inputFieldKeys = splitByCommaAndDot(params.input_field_name);
	this.outputEventType = params.output_event_type; // aggkeyList

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

extend(Average, kangaBaseNode);

/**
 * Handle the incoming event
 * @returns {Object}
 */
Average.prototype._execute = function () {
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
Average.prototype.aggregate = function () {
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
Average.prototype.getResult = function () {
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
Average.prototype.isEmpty = function (obj) {
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
Average.prototype.addNewEvent = function () {
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
Average.prototype.popEvents = function () {
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
Average.prototype.eventOnPopped = function (keys, message) {
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
Average.prototype.eventOnPushed = function (keys, message) {
	var aggregator = this.aggregators[keys];
	if (!aggregator) {
		aggregator = new AverageImp(this.inputFieldKeys);
		this.aggregators[keys] = aggregator;
	}
	aggregator.eventOnPush(message);
};

/**
 * Generate keys for message
 * @param {Object} message
 * @returns {string} - the generated key
 */
Average.prototype.getGroupByKeys = function (message) {
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
Average.prototype.clear = function () {
	this.eventCount = 0;
	this.eventQueue = [];
	for ( var key in this.aggregators) {
		this.aggregators[key].eventsOnCleared();
	}
	this.aggregators = {};
}
/* ###################################################### */
/**
 * Initialize parameters
 */
var AverageImp = function (aggkeyList) {
	this.aggkeyList = aggkeyList;
	this.aggSize = aggkeyList.length;
	this.sumList = new Array(this.aggSize);
	this.countList = new Array(this.aggSize);
	this.reset();
};

/**
 * get aggregate result in JSON format
 * @returns {Object}
 */
AverageImp.prototype.aggregate = function () {
	var message = {};
	for (var i = 0; i < this.aggSize; i++) {
		var result = 0.0;
		if (this.countList[i] > 0) {
			var keys = this.aggkeyList[i].slice(0, this.aggkeyList[i].length);
			keys[keys.length - 1] = keys[keys.length - 1] + '_average';
			putJSONValue(message, keys, (this.sumList[i] / this.countList[i]));
		}
	}
	return message;
};

/**
 * reset aggregate results
 */
AverageImp.prototype.reset = function () {
	for (var i = 0; i < this.aggSize; i++) {
		this.sumList[i] = 0.0;
		this.countList[i] = 0;
	}
};

/**
 * push the message and calculate the aggregate result 
 * @param {Object} message
 */
AverageImp.prototype.eventOnPush = function (message) {
	for (var i = 0; i < this.aggSize; i++) {
		var inputFieldName = this.aggkeyList[i];
		var inputValue = Number(getJSONValue(message, inputFieldName));
		this.sumList[i] += inputValue;
		this.countList[i] += 1;
	}
};

/**
 * pop the popped messages and calculate the aggregate result
 * @param {Array} messageList
 */
AverageImp.prototype.eventsOnPopped = function (messageList) {
	for (var j = 0; j < messageList.length; j++) {
		for (var i = 0; i < this.aggSize; i++) {
			if (this.countList[i] == 0)
				break;
			var inputFieldName = this.aggkeyList[i];
			var inputValue = Number(getJSONValue(messageList[j], inputFieldName));
			this.sumList[i] -= inputValue;
			this.countList[i] -= 1;
		}
	}
};

AverageImp.prototype.eventsOnCleared = function () {
	this.reset();
};

/**
 * check if the aggregate result is empty
 * @returns {Boolean}
 */
AverageImp.prototype.isEmpty = function () {
	for (var i = 0; i < this.aggSize; i++) {
		if (this.countList[i] > 0)
			return false;
	}
	return true;
};

module.exports = Average;