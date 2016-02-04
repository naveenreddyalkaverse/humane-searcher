import _ from 'lodash';
import Config from 'config';
import Agent from 'agentkeepalive';
import Promise from 'bluebird';
import Request from 'request';

const ESConfig = Config.get('ES');
const Url = ESConfig.url;

//const logLevel = config.has('ES.logLevel') ? config.get('ES.logLevel') : 'info';

export default class ESClient {
    constructor() {
        const keepAliveAgent = new Agent({
            maxSockets: ESConfig.maxSockets || 10,
            maxFreeSockets: ESConfig.maxFreeSockets || 5,
            timeout: ESConfig.timeout || 60000,
            keepAliveTimeout: ESConfig.keepAliveTimeout || 30000
        });

        this.request = Promise.promisify(Request.defaults({
            json: true,
            agent: keepAliveAgent,
            baseUrl: Url,
            gzip: true
        }));
    }

    static processResponse(response) {
        let _response = response;
        if (_.isArray(_response)) {
            _response = response[0];
        }

        return _response.statusCode === 200 ? _response.body : null;
    }

    // queries will be in following format:
    //      index or indices
    //      type
    //      search
    static bulkFormat(queries) {
        let ret = '';
        _.forEach(queries, (query) => {
            ret += JSON.stringify({index: (query.indices || [query.index]).join(','), type: (query.types || [query.type]).join(',')});
            ret += '\n';
            ret += JSON.stringify(query.search);
            ret += '\n';
        });

        //console.log('Multi searching: ', JSON.stringify(queries, null, 2));

        return ret;
    }

    allPages(index, type, query, size, cb) {
        const _this = this;

        let totalResults = 0;

        function recursiveFetch(page) {
            return _this.search({index, type, search: _.assign({from: page * size, size}, query)})
              .then((response) => {
                  if (response && response.hits) {
                      if (page === 0) {
                          totalResults = response.hits.total;
                      }

                      // process response...
                      cb(response);

                      const hits = response.hits.hits;
                      if (hits && totalResults > (page * size) + hits.length) {
                          // recursively fetch next page
                          return recursiveFetch(page + 1);
                      }
                  }

                  return true;
              });
        }

        return recursiveFetch(0);
    }

    search(queryOrPromise) {
        return Promise.resolve(queryOrPromise)
          .then(query => {
              const uri = `/${query.index}/${query.type}/_search`;

              console.log('Search: ', uri, JSON.stringify(query, null, 2));

              return this.request({method: 'POST', uri, body: query.search})
                .then(ESClient.processResponse);
          });
    }

    explain(id, query) {
        const uri = `/${query.index}/${query.type}/${id}/_explain`;

        console.log('Explain: ', uri, JSON.stringify(query.search, null, 2));

        return this.request({method: 'POST', uri, body: query.search}).then(ESClient.processResponse);
    }

    termVectors(index, type, id) {
        const uri = `/${index}/${type}/${id}/_termvectors?fields=*`;

        return this.request({method: 'GET', uri}).then(ESClient.processResponse);
    }

    multiSearch(queriesOrPromise) {
        return Promise.all(queriesOrPromise)
          .then((queries) => {
              const bulkQuery = ESClient.bulkFormat(queries);
              return this.request({method: 'POST', uri: `/_msearch`, body: bulkQuery, json: false})
                .then(ESClient.processResponse)
                .then((response) => !!response ? JSON.parse(response) : null);
          });
    }

    analyze(index, analyzer, text) {
        const uri = `/${index}/_analyze?analyzer=${analyzer}&text=${encodeURIComponent(text)}`;
        return this.request({method: 'GET', uri})
          .then(ESClient.processResponse);
    }

//    curl -XGET 'http://localhost:9200/imdb/movies/_validate/query?rewrite=true' -d '
//{
//    "query": {
//      "fuzzy": {
//        "actors": "kyle"
//        }
//      }
//}'
//
//{
//    "valid": true,
//    "_shards": {
//      "total": 1,
//      "successful": 1,
//      "failed": 0
//      },
//    "explanations": [
//    {
//        "index": "imdb",
//        "valid": true,
//        "explanation": "plot:kyle plot:kylie^0.75 plot:kyne^0.75 plot:lyle^0.75 plot:pyle^0.75 #_type:movies"
//    }
//      ]
//}
}