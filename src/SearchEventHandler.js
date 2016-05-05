// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import buildRequest from 'humane-node-commons/lib/Request';
import md5 from 'md5';

export default class SearchEventHandler {
    constructor(instanceName) {
        const url = `http://localhost:3000/${instanceName}/indexer/api`;

        // this.request = Promise.promisify(Request.defaults({
        //     json: true,
        //     agent: keepAliveAgent,
        //     baseUrl: url,
        //     gzip: true,
        //     method: 'POST',
        //     uri: '/searchQuery'
        // }));

        this.request = buildRequest({baseUrl: url, method: 'POST', uri: '/searchQuery'});
    }

    send(data) {
        this.request({body: {doc: data}})
          .then(response => {
              if (_.isArray(response)) {
                  response = response[0];
              }

              const result = response.statusCode === 200 ? response.body : null;
              if (!result) {
                  console.warn('Error while sending search query: ', response.statusCode, response.body);
              }
          })
          .catch(error => {
              console.warn('Error while sending search query: ', error);
          });
    }

    handle(data) {
        const {queryData, queryLanguages, queryResult} = data;

        const queryTime = Date.now();
        const hasResults = queryResult && queryResult.totalResults || false;
        const query = _.lowerCase(queryData.text);
        let unicodeQuery = null;

        if (queryLanguages && !_.isEmpty(queryLanguages)) {
            unicodeQuery = query;
        }

        let languages = _.union(queryLanguages || [], queryData.filter.lang.primary || [], queryData.filter.lang.secondary || []);
        if (_.isEmpty(languages)) {
            languages = ['en'];
        }

        _.forEach(languages, lang => {
            const key = md5(`${lang}/${query}`);

            this.send({key, query, unicodeQuery, queryTime, hasResults, lang});
        });
    }
}