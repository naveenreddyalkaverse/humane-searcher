// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Agent from 'agentkeepalive';
import Promise from 'bluebird';
import Request from 'request';
import Config from 'config';
import md5 from 'md5';

const BeaconConfig = Config.get('Beacon');
const BeaconTypes = BeaconConfig.types;
const BeaconDefaultProperties = BeaconConfig.defaultProperties;
const Url = BeaconConfig.url;

const SEARCH_QUERY_EVENT = 'search_query';
const SEARCH_RESULT_EVENT = 'search_result';

export default class Beacon {
    constructor() {
        const keepAliveAgent = new Agent({
            maxSockets: BeaconConfig.maxSockets || 10,
            maxFreeSockets: BeaconConfig.maxFreeSockets || 5,
            timeout: BeaconConfig.timeout || 60000,
            keepAliveTimeout: BeaconConfig.keepAliveTimeout || 30000
        });

        this.request = Promise.promisify(Request.defaults({
            json: true,
            agent: keepAliveAgent,
            baseUrl: Url,
            gzip: true,
            headers: {
                'Content-Type': 'application/vnd.kafka.binary.v1+json'
            },
            method: 'POST',
            uri: ''
        }));
    }

    static wrapEventProperties(headers, eventType, event) {
        let headerObj = headers;
        if (!headerObj) {
            headerObj = {};
        }

        const beaconTypeConfig = BeaconTypes[eventType];

        return {
            client_id: headerObj.get('client_id') || Math.round(Math.random() * 1000000000),
            event_section: beaconTypeConfig.event_section,
            event_name: beaconTypeConfig.event_name,
            epoch_time: Date.now(),
            properties: _.extend({
                session_source: headerObj.get('session_source') || beaconTypeConfig.session_source || BeaconDefaultProperties.session_source,
                user_handset_maker: headerObj.get('user_handset_maker') || BeaconDefaultProperties.user_handset_maker,
                user_app_ver: headerObj.get('user_app_ver') || BeaconDefaultProperties.user_app_ver,
                user_connection: headerObj.get('user_connection') || BeaconDefaultProperties.user_connection,
                user_os_platform: headerObj.get('user_os_platform') || BeaconDefaultProperties.user_os_platform,
                user_handset_model: headerObj.get('user_handset_model') || BeaconDefaultProperties.user_handset_model,
                user_os_ver: headerObj.get('user_os_ver') || BeaconDefaultProperties.user_os_ver,
                user_os_name: headerObj.get('user_os_name') || BeaconDefaultProperties.user_os_name,
                event_attribution: headerObj.get('event_attribution') || beaconTypeConfig.event_name || BeaconDefaultProperties.event_attribution,
                pv_event: beaconTypeConfig.pv_event
            }, event)
        };
    }

    send(headers, eventType, event) {
        const eventRecord = Beacon.wrapEventProperties(headers, eventType, event);

        const pingObj = {
            records: [
                {value: new Buffer(JSON.stringify(eventRecord)).toString('base64')}
            ]
        };

        this.request({body: pingObj})
          .then(response => {
              let _response = response;
              if (_.isArray(_response)) {
                  _response = response[0];
              }

              const result = _response.statusCode === 200 ? _response.body : null;
              const errorCode = result && result.offsets && result.offsets.length > 0 && result.offsets[0].error_code;
              if (errorCode && !_.isUndefined(errorCode) && !_.isNull(errorCode)) {
                  console.warn('Error while sending beacon: ', errorCode, JSON.stringify(eventRecord));
              }
          })
          .catch(error => {
              console.warn('Error while sending beacon: ', error, JSON.stringify(eventRecord));
          });
    }

    static searchQueryEventProperties(queryData, queryLanguages) {
        return {
            user_language_primary: queryData.filter.lang.primary || null,
            user_language_secondary: queryData.filter.lang.secondary || null,
            filters: _.omit(queryData.filter, 'lang'),
            sort_field: queryData.sort && queryData.sort.field || 'sort',
            sort_order: queryData.sort && queryData.sort.order || 'DESC',
            unicode: queryLanguages && !_.isEmpty(queryLanguages) ? 'yes' : 'no',
            original_search_input: queryData.originalInput,
            original_search_input_length: queryData.originalInput && queryData.originalInput.length,
            search_query: queryData.text,
            search_query_language: queryLanguages || 'en',
            search_query_key: md5(_.trim(queryData.text)),
            search_mode: queryData.mode,
            search_entity: queryData.type,
            page_num: queryData.page
        };
    }

    sendSearchQuery(headers, queryData, queryLanguages) {
        this.send(headers, SEARCH_QUERY_EVENT, Beacon.searchQueryEventProperties(queryData, queryLanguages));
    }

    sendSearchResult(headers, queryData, queryLanguages, queryResult) {
        const eventProperties = {
            item_ids: _.map(queryResult.results, result => result._id),
            total_num_of_items: queryResult.totalResults
        };

        this.send(headers, SEARCH_RESULT_EVENT, _.assign(Beacon.searchQueryEventProperties(queryData, queryLanguages), eventProperties));
    }
}