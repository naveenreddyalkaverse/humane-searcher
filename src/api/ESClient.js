import _ from 'lodash';
import Agent from 'agentkeepalive';
import Promise from 'bluebird';
import Request from 'request';

import md5 from 'md5';
import performanceNow from 'performance-now';

import redisClient from './RedisClient';

export default class ESClient {
    constructor(config) {
        const keepAliveAgent = new Agent({
            maxSockets: config.esConfig && config.esConfig.maxSockets || 10,
            maxFreeSockets: config.esConfig && config.esConfig.maxFreeSockets || 5,
            timeout: config.esConfig && config.esConfig.timeout || 60000,
            keepAliveTimeout: config.esConfig && config.esConfig.keepAliveTimeout || 30000
        });

        this.request = Promise.promisify(Request.defaults({
            json: true,
            agent: keepAliveAgent,
            baseUrl: `${config.esConfig && config.esConfig.url || 'http://localhost:9200'}`,
            gzip: true
        }));

        this.redisClient = redisClient(_.pick(config, ['redisConfig', 'redisSentinelConfig']));
    }

    storeInCache(key, data) {
        // TODO: pack data with MessagePack
        return this.redisClient.setAsync(key, JSON.stringify(data))
          .then(() => data);
    }

    retrieveFromCache(key) {
        // TODO: pack data with MessagePack
        return this.redisClient.getAsync(key)
          .then((data) => !!data ? JSON.parse(data) : null);
    }

    removeFromCache(key) {
        return this.redisClient.delAsync(key);
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
        const startTime = performanceNow();

        return Promise.resolve(queryOrPromise)
          .then(query => {
              const uri = `/${query.index}/${query.type}/_search`;

              //console.log('Search: ', uri, JSON.stringify(query.search));

              const queryKey = md5(JSON.stringify(query.search));
              const cacheKey = `${uri}:${queryKey}`;

              return this.retrieveFromCache(cacheKey)
                .then(cacheResponse => {
                    if (cacheResponse) {
                        cacheResponse.took = _.round(performanceNow() - startTime, 3);

                        console.log('search: Retrieved from cache in (ms): ', cacheResponse.took);

                        return cacheResponse;
                    }

                    return this.request({method: 'POST', uri, body: query.search})
                      .then(ESClient.processResponse)
                      .then(queryResponse => {
                          if (queryResponse) {
                              return this.storeInCache(cacheKey, queryResponse);
                          }

                          return null;
                      });
                });
          });
    }

    explain(id, query) {
        const uri = `/${query.index}/${query.type}/${id}/_explain`;

        //console.log('Explain: ', uri, JSON.stringify(query.search));

        return this.request({method: 'POST', uri, body: query.search}).then(ESClient.processResponse);
    }

    termVectors(index, type, id) {
        const uri = `/${index}/${type}/${id}/_termvectors?fields=*`;

        return this.request({method: 'GET', uri}).then(ESClient.processResponse);
    }

    multiSearch(queriesOrPromise) {
        const startTime = performanceNow();

        return Promise.all(queriesOrPromise)
          .then((queries) => {
              const uri = '/_msearch';
              const bulkQuery = ESClient.bulkFormat(queries);

              const queryKey = md5(bulkQuery);
              const cacheKey = `${uri}:${queryKey}`;

              return this.retrieveFromCache(cacheKey)
                .then(cacheResponse => {
                    if (cacheResponse) {
                        cacheResponse.took = _.round(performanceNow() - startTime, 3);

                        if (cacheResponse.responses) {
                            // set response times
                            _.forEach(cacheResponse.responses, response => {
                                response.took = cacheResponse.took;
                            });
                        }

                        console.log('multiSearch: Retrieved from cache in (ms): ', cacheResponse.took);
                        return cacheResponse;
                    }

                    return this.request({method: 'POST', uri, body: bulkQuery, json: false})
                      .then(ESClient.processResponse)
                      .then((response) => !!response ? JSON.parse(response) : null)
                      .then(queryResponse => {
                          if (queryResponse) {
                              return this.storeInCache(cacheKey, queryResponse);
                          }

                          return null;
                      });
                });
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