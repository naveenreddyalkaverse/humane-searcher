// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import {EventEmitter} from 'events';

import ESClient from './ESClient';
import LanguageDetector from './LanguageDetector';

import * as Constants from './Constants';
import buildApiSchema from './ApiSchemaBuilder';

import ValidationError from 'humane-node-commons/lib/ValidationError';

class SearcherInternal {
    constructor(config) {
        // TODO: compile config, so searcher logic has lesser checks, extend search config with default configs
        this.logLevel = config.logLevel || 'info';
        this.searchConfig = SearcherInternal.validateSearchConfig(config.searchConfig);
        this.apiSchema = buildApiSchema(config.searchConfig);
        this.esClient = new ESClient(_.pick(config, ['logLevel', 'esConfig', 'redisConfig', 'redisSentinelConfig']));
        this.transliterator = config.transliterator;
        this.languageDetector = new LanguageDetector();

        this.eventEmitter = new EventEmitter();

        // todo: default registry of event handler for storing search queries in DB.
        if (config.searchConfig.eventHandlers) {
            _.forEach(config.searchConfig.eventHandlers, (handlerOrArray, eventName) => {
                if (_.isArray(handlerOrArray)) {
                    _.forEach(handlerOrArray, handler => this.eventEmitter.addListener(eventName, handler));
                } else {
                    this.eventEmitter.addListener(eventName, handlerOrArray);
                }
            });
        }
    }

    // TODO: validate it through Joi
    // TODO: provide command line tool to validate config
    static validateSearchConfig(searchConfig) {
        if (!searchConfig.inputAnalyzer) {
            throw new ValidationError('InputAnalyzer must be defined in search config', {details: {code: 'INPUT_ANALYZER_NOT_DEFINED'}});
        }

        return searchConfig;
    }

    static validateInput(input, schema) {
        if (!input) {
            throw new ValidationError('No input provided', {details: {code: 'NO_INPUT'}});
        }

        // validate it is valid type...
        const validationResult = Joi.validate(input, schema);
        if (validationResult.error) {
            let errorDetails = null;

            if (validationResult.error.details) {
                errorDetails = validationResult.error.details;
                if (_.isArray(errorDetails) && errorDetails.length === 1) {
                    errorDetails = errorDetails[0];
                }
            } else {
                errorDetails = validationResult.error;
            }

            throw new ValidationError('Non conforming format', {details: errorDetails});
        }

        return validationResult.value;
    }

    //noinspection JSMethodCanBeStatic
    constantScoreQuery(fieldConfig, query, boostMultiplier, noWrapIntoConstantScore) {
        if (noWrapIntoConstantScore) {
            return query;
        }

        const boost = (boostMultiplier || 1.0) * (fieldConfig.weight || 1.0);

        return {constant_score: {query, boost}};
    }

    wrapQuery(fieldConfig, query, boostMultiplier = this.searchConfig.matchTypeBoosts.exact, noWrapIntoConstantScore) {
        if (fieldConfig.filter) {
            return query;
        }

        if (fieldConfig.nestedPath) {
            query = {nested: {path: fieldConfig.nestedPath, query}};
        }

        return this.constantScoreQuery(fieldConfig, query, boostMultiplier, noWrapIntoConstantScore);
    }

    matchQuery(fieldConfig, text, boostMultiplier, fuzziness = undefined, fieldName) {
        const query = {
            match: {[fieldName || fieldConfig.field]: {query: text, fuzziness, prefix_length: fuzziness && 2 || undefined}}
        };

        return this.wrapQuery(fieldConfig, query, boostMultiplier);
    }

    termQuery(fieldConfig, term, boostMultiplier, fieldName) {
        const queryType = _.isArray(term) ? 'terms' : 'term';
        const query = {
            [queryType]: {[fieldName || fieldConfig.field]: term}
        };

        return this.wrapQuery(fieldConfig, query, boostMultiplier);
    }

    query(fieldConfig, query, boostMultiplier, fuzziness, fieldName) {
        if (fieldConfig.termQuery) {
            return this.termQuery(fieldConfig, query, boostMultiplier, fieldName);
        }

        return this.matchQuery(fieldConfig, query, boostMultiplier, fuzziness, fieldName);
    }

    fuzzyQueries(fieldConfig, text) {
        const field = fieldConfig.field;

        const queries = [
            this.matchQuery(fieldConfig, text),

            this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.edgeGram, 0, `${field}.edgeGram`),

            this.constantScoreQuery(fieldConfig, {
                bool: {
                    should: [
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phonetic, 0, `${field}.phonetic_soundex`, true),
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phonetic, 0, `${field}.phonetic_dm`, true),
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phonetic, 0, `${field}.phonetic_bm`, true)
                    ],
                    minimum_should_match: 2
                }
            }, this.searchConfig.matchTypeBoosts.phonetic),

            this.constantScoreQuery(fieldConfig, {
                bool: {
                    should: [
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phoneticEdgeGram, 0, `${field}.phonetic_edgeGram_soundex`, true),
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phoneticEdgeGram, 0, `${field}.phonetic_edgeGram_dm`, true),
                        this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.phoneticEdgeGram, 0, `${field}.phonetic_edgeGram_bm`, true)
                    ],
                    minimum_should_match: 2
                }
            }, this.searchConfig.matchTypeBoosts.phonetic)

            //SearcherInternal.matchQuery(`${field}.phonetic_bm`, query, 'AUTO', baseBoost * 2.0 * 1.20),
            //SearcherInternal.matchQuery(`${field}.phonetic_edgeGram_bm`, query, 'AUTO', baseBoost * 2.0)
        ];

        if (text && text.length >= 3 && text.length <= 4) {
            queries.push(this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.exact_edit, 1));
        }

        if (text && text.length > 4 && text.length <= 7) {
            queries.push(this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.exact_edit, 2));
            queries.push(this.matchQuery(fieldConfig, text, this.searchConfig.matchTypeBoosts.edgeGram_edit, 1, `${field}.edgeGram`));
        }

        return queries;
    }

    buildFieldQuery(fieldConfig, englishTerm, vernacularTerm, queries) {
        let query = null;
        if (vernacularTerm && fieldConfig.vernacularOnly) {
            query = this.query(fieldConfig, vernacularTerm);
        } else if (fieldConfig.noFuzzy || fieldConfig.termQuery) {
            query = this.query(fieldConfig, englishTerm);
        } else {
            query = this.fuzzyQueries(fieldConfig, englishTerm);
        }

        if (queries) {
            if (_.isArray(query)) {
                _.forEach(query, (singleQuery) => queries.push(singleQuery));
            } else {
                queries.push(query);
            }
        }

        return query;
    }

    getIndexTypeConfigFromType(type) {
        const typeConfig = this.searchConfig.types[type];
        if (!typeConfig) {
            throw new ValidationError(`No index type config found for: ${type}`, {details: {code: 'INDEX_TYPE_NOT_FOUND', type}});
        }

        return typeConfig;
    }

    buildTypeQuery(searchTypeConfig, term) {
        const languages = this.languageDetector.detect(term);

        let englishTerm = term;
        let vernacularTerm = null;
        if (!(!languages || languages.length === 1 && languages[0].code === 'en') && this.transliterator) {
            // it's vernacular
            vernacularTerm = term;
            englishTerm = this.transliterator.transliterate(vernacularTerm);
        }

        const indexTypeConfig = searchTypeConfig.indexType;

        const queries = [];
        _.forEach(searchTypeConfig.queryFields || indexTypeConfig.queryFields, fieldConfig => this.buildFieldQuery(fieldConfig, englishTerm, vernacularTerm, queries));

        return {
            query: queries.length > 1 ? {dis_max: {queries}} : queries[0],
            language: languages && _.map(languages, lang => lang.code)
        };
    }

    filterPart(searchTypeConfig, input, termLanguages) {
        const filterConfigs = searchTypeConfig.filters || searchTypeConfig.indexType.filters;

        if (!filterConfigs) {
            return undefined;
        }

        const filterQueries = [];

        _.forEach(filterConfigs, (filterConfig, key) => {
            if (filterConfig.type && filterConfig.type === 'post') {
                // skip post filters
                return true;
            }

            let filterValue = null;

            if (input.filter[key]) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (filterValue) {
                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                this.buildFieldQuery(_.extend({filter: true}, filterConfigs[key]), filterValue, null, filterQueries);
            }

            return true;
        });

        if (input.lang && !_.isEmpty(input.lang)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), input.lang, null, filterQueries);
        }

        if (termLanguages && !_.isEmpty(termLanguages)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), termLanguages, null, filterQueries);
        }

        if (filterQueries.length === 1) {
            return filterQueries[0];
        }

        return {
            and: {
                filters: _.map(filterQueries, filter => ({query: filter}))
            }
        };
    }

    postFilters(searchTypeConfig, input) {
        const filterConfigs = searchTypeConfig.filters || searchTypeConfig.indexType.filters;

        if (!filterConfigs) {
            return undefined;
        }

        const postFilters = [];

        _.forEach(filterConfigs, (filterConfig, key) => {
            if (!filterConfig.type || filterConfig.type !== 'post') {
                // skip non filters
                return true;
            }

            let filterValue = null;

            if (input.filter[key]) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (filterValue) {
                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                postFilters.push(filterConfig.filter);
            }

            return true;
        });

        return postFilters;
    }

    // todo: handle case of filtering only score based descending order, as it is default anyways
    //noinspection JSMethodCanBeStatic
    buildSort(value, defaultSortOrder) {
        // array of string
        if (_.isString(value)) {
            return {[value]: _.lowerCase(defaultSortOrder)};
        } else if (_.isObject(value)) {
            // array of sort objects
            return {[value.field]: _.lowerCase(value.order || defaultSortOrder)};
        }

        return null;
    }

    buildDefaultSort(config, defaultSortOrder) {
        if (_.isObject(config)) {
            return _(config)
              .map((value, key) => {
                  if (value && (_.isBoolean(value) || _.isObject(value) && value.default)) {
                      // include this key
                      return this.buildSort(key, defaultSortOrder);
                  }

                  return null;
              })
              .filter(value => !!value)
              .value();
        }

        return null;
    }

    sortPart(searchTypeConfig, input) {
        const defaultSortOrder = this.searchConfig.defaultSortOrder || Constants.DESC_SORT_ORDER;

        // build sort
        if (input.sort) {
            if (_.isArray(input.sort)) {
                return _(input.sort).map(value => this.buildSort(value, defaultSortOrder)).filter(value => !!value).value();
            }

            return this.buildSort(input.sort, defaultSortOrder);
        }

        const sortConfigs = searchTypeConfig.sort || searchTypeConfig.indexType.sort;
        if (!sortConfigs) {
            return undefined;
        }

        // pick default from sort config
        if (_.isArray(sortConfigs)) {
            return _(sortConfigs).map(config => this.buildDefaultSort(config, defaultSortOrder)).filter(config => !!config).value();
        }

        if (_.isObject(sortConfigs)) {
            return this.buildDefaultSort(sortConfigs, defaultSortOrder);
        }

        return undefined;
    }

    analyzeInput(text) {
        return this.esClient.analyze(this.searchConfig.inputAnalyzer.index, this.searchConfig.inputAnalyzer.name, text);
    }

    // todo: calculate minimum should match dynamically
    searchQuery(searchTypeConfig, input, tokens) {
        const queryParts = [];
        const queryLanguages = {};

        const promise = !!tokens ? tokens : this.analyzeInput(input.text).then((response) => response && response.tokens && _.map(response.tokens, token => token.token));

        return Promise.resolve(promise)
          .then((response) => response && _.map(response, token => this.buildTypeQuery(searchTypeConfig, token)))
          .then((parts) => {
              _.forEach(parts, part => {
                  if (part.language) {
                      if (_.isArray(part.language)) {
                          _.forEach(part.language, lang => {
                              queryLanguages[lang] = true;
                          });
                      } else {
                          queryLanguages[part.language] = true;
                      }
                  }

                  queryParts.push(part.query);
              });

              return queryParts;
          })
          .then(() => {
              let must = undefined;
              let should = undefined;
              if (!_.isArray(queryParts)) {
                  must = queryParts;
              } else if (queryParts.length === 1) {
                  must = queryParts[0];
              } else {
                  should = queryParts;
              }

              const filter = this.filterPart(searchTypeConfig, input, _.keys(queryLanguages));

              const indexTypeConfig = searchTypeConfig.indexType;

              let sort = this.sortPart(searchTypeConfig, input) || undefined;
              if (sort && _.isEmpty(sort)) {
                  sort = undefined;
              }

              return {
                  index: indexTypeConfig.index,
                  type: indexTypeConfig.type,
                  search: {
                      from: (input.page || 0) * (input.count || 0),
                      size: input.count || undefined,
                      sort,
                      query: {
                          function_score: {
                              query: {
                                  bool: {must, should, filter, minimum_should_match: searchTypeConfig.minimumShouldMatch}
                              },
                              field_value_factor: {
                                  field: 'weight',
                                  factor: 2.0,
                                  missing: 1
                              }
                          }
                      }
                  },
                  queryLanguages
              };
          });
    }

    processMultipleSearchResponse(responses) {
        if (!responses) {
            return null;
        }

        const result = {
            multi: true,
            totalResults: 0,
            results: {}
        };

        _.forEach(responses.responses, (response) => {
            result.queryTimeTaken = Math.max(result.queryTimeTaken || 0, response.took);

            if (response.hits && response.hits.hits) {
                _.forEach(response.hits.hits, hit => {
                    const typeConfig = this.searchConfig.types[hit._type];
                    const name = !!typeConfig ? typeConfig.name || typeConfig.type : hit._type;

                    let resultGroup = result.results[name];
                    if (!resultGroup) {
                        resultGroup = result.results[name] = {
                            results: [],
                            totalResults: response.hits.total || 0,
                            name,
                            type: hit._type
                        };
                        result.totalResults += resultGroup.totalResults;
                    }

                    resultGroup.results.push(_.extend(hit._source, {_id: hit._id, _score: hit._score, _type: hit._type, _name: name}));
                });
            }
        });

        return result;
    }

    processSingleSearchResponse(response) {
        if (!response) {
            return null;
        }

        const type = response.hits && response.hits.hits && response.hits.hits.length > 0 && response.hits.hits[0]._type;
        const typeConfig = this.searchConfig.types[type];
        const name = !!typeConfig ? typeConfig.name || typeConfig.type : type;

        return {
            queryTimeTaken: response.took,
            totalResults: response.hits && response.hits.total || 0,
            type,
            name,
            results: response.hits && _.map(response.hits.hits, (hit) => _.extend(hit._source, {_id: hit._id, _score: hit._score, _type: hit._type, _name: name})) || []
        };
    }

    _searchInternal(headers, input, searchTypeConfigs, eventName) {
        let queryLanguages = null;

        let multiSearch = false;

        return this.analyzeInput(input.text)
          .then((response) => response && response.tokens && _.map(response.tokens, token => token.token))
          .then((tokens) => {
              if (!tokens) {
                  return null;
              }

              if (!input.type || input.type === '*') {
                  const searchQueries = _(searchTypeConfigs).values().map(typeConfig => this.searchQuery(typeConfig, input, tokens)).value();

                  multiSearch = _.isArray(searchQueries) || false;

                  return Promise.all(searchQueries);
              }

              const searchTypeConfig = searchTypeConfigs[input.type];
              if (!searchTypeConfig) {
                  throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'SEARCH_CONFIG_NOT_FOUND', type: input.type}});
              }

              return this.searchQuery(searchTypeConfig, input, tokens);
          })
          .then(queryOrArray => {
              if (multiSearch) {
                  queryLanguages = _.head(queryOrArray).queryLanguages;
              } else {
                  queryLanguages = queryOrArray.queryLanguages;
              }

              return queryOrArray;
          })
          .then(queryOrArray => multiSearch ? this.esClient.multiSearch(queryOrArray) : this.esClient.search(queryOrArray))
          .then((response) => multiSearch ? this.processMultipleSearchResponse(response) : this.processSingleSearchResponse(response))
          .then(response => {
              this.eventEmitter.emit(eventName, {headers, queryData: input, queryLanguages, queryResult: response});

              return response;
          });
    }

    autocomplete(headers, input) {
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete.types, Constants.AUTOCOMPLETE_EVENT);
    }

    search(headers, input) {
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.search);

        return this._searchInternal(headers, validatedInput, this.searchConfig.search.types, Constants.SEARCH_EVENT);
    }

    suggestedQueries(headers, input) {
        // same type as autocomplete
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete.types, Constants.SUGGESTED_QUERIES_EVENT)
          .then(response => {
              // merge
              if (response.multi) {
                  // calculate scores
                  const relevancyScores = [];
                  _.forEach(response.results, resultGroup => {
                      _.forEach(resultGroup.results, result => {
                          result._relevancyScore = result._score / (result.weight || 1.0);
                          relevancyScores.push(result._relevancyScore);
                      });
                  });

                  // order scores in descending order
                  relevancyScores.sort((scoreA, scoreB) => scoreB - scoreA);

                  // find deflection point
                  let previousScore = 0;
                  let deflectionScore = 0;
                  _.forEach(relevancyScores, score => {
                      if (previousScore && score < 0.5 * previousScore) {
                          deflectionScore = previousScore;
                          return false;
                      }

                      previousScore = score;

                      return true;
                  });

                  // consider items till the deflection point
                  const results = [];
                  _.forEach(response.results, resultGroup => {
                      _.forEach(resultGroup.results, result => {
                          if (result._relevancyScore >= deflectionScore) {
                              results.push(result);
                          }
                      });
                  });

                  results.sort((resultA, resultB) => resultB._score - resultA._score);

                  response.results = results;
              }

              return response;
          });
    }

    _explain(api, input) {
        let apiConfig = null;
        if (api === Constants.AUTOCOMPLETE_API) {
            apiConfig = this.searchConfig.autocomplete;
        } else if (api === Constants.SEARCH_API) {
            apiConfig = this.searchConfig.search;
        }

        return Promise.resolve(this.searchQuery(apiConfig.types[input.type], input))
          .then(query => {
              delete query.search.from;
              delete query.search.size;
              delete query.search.sort;
              return query;
          })
          .then(query => this.esClient.explain(input.id, query))
          .then((response) => response && response.explanation || null);
    }

    explainAutocomplete(headers, input) {
        return this._explain(Constants.AUTOCOMPLETE_API, SearcherInternal.validateInput(input, this.apiSchema.explainAutocomplete));
    }

    explainSearch(headers, input) {
        return this._explain(Constants.SEARCH_API, SearcherInternal.validateInput(input, this.apiSchema.explainSearch));
    }

    termVectors(headers, input) {
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.termVectors);

        const typeConfig = this.getIndexTypeConfigFromType(validatedInput.type);

        return Promise.resolve(this.esClient.termVectors(typeConfig.index, typeConfig.type, validatedInput.id))
          .then((response) => response && response.term_vectors || null);
    }

    // TODO: create schema to validate view input
    view(headers, input) {
        const type = input.type;

        const viewConfig = this.searchConfig.views.types[type];
        const indexTypeConfig = viewConfig.indexType;

        const filter = this.filterPart(viewConfig, input);
        const postFilters = this.postFilters(viewConfig, input);

        const query = {
            sort: this.sortPart(viewConfig, input) || undefined,
            query: {
                bool: {filter}
            }
        };

        const finalResponse = {
            totalResults: 0,
            results: []
        };

        return this.esClient.allPages(indexTypeConfig.index, indexTypeConfig.type, query, 100,
          (response) => {
              if (response && response.hits && response.hits.hits) {
                  const hits = response.hits.hits;
                  if (hits) {
                      _.forEach(hits, (hit) => {
                          const doc = hit._source;
                          if (!postFilters || _.every(postFilters, postFilter => postFilter(doc))) {
                              finalResponse.totalResults++;
                              finalResponse.results.push(doc);
                          }
                      });
                  }
              }
          })
          .then(() => finalResponse);
    }
}

export default class Searcher {
    constructor(searchConfig) {
        this.internal = new SearcherInternal(searchConfig);
    }

    search(headers, request) {
        return this.internal.search(headers, request);
    }

    autocomplete(headers, request) {
        return this.internal.autocomplete(headers, request);
    }

    suggestedQueries(headers, request) {
        return this.internal.suggestedQueries(headers, request);
    }

    explainAutocomplete(headers, request) {
        return this.internal.explainAutocomplete(headers, request);
    }

    explainSearch(headers, request) {
        return this.internal.explainSearch(headers, request);
    }

    termVectors(headers, request) {
        return this.internal.termVectors(headers, request);
    }

    view(headers, request) {
        return this.internal.view(headers, request);
    }

    registry() {
        return {
            autocomplete: [
                {handler: this.autocomplete},
                {handler: this.autocomplete, method: 'get'}
            ],
            search: [
                {handler: this.search},
                {handler: this.search, method: 'get'}
            ],
            suggestedQueries: [
                {handler: this.suggestedQueries},
                {handler: this.suggestedQueries, method: 'get'}
            ],
            'explain/search': [
                {handler: this.explainSearch},
                {handler: this.explainSearch, method: 'get'}
            ],
            'explain/autocomplete': [
                {handler: this.explainAutocomplete},
                {handler: this.explainAutocomplete, method: 'get'}
            ],
            termVectors: {handler: this.termVectors, method: 'get'},
            view: [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/autocomplete': [
                {handler: this.autocomplete},
                {handler: this.autocomplete, method: 'get'}
            ],
            ':type/search': [
                {handler: this.search},
                {handler: this.search, method: 'get'}
            ],
            ':type/suggestedQueries': [
                {handler: this.suggestedQueries},
                {handler: this.suggestedQueries, method: 'get'}
            ],
            ':type/view': [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/:id/termVectors': {handler: this.termVectors, method: 'get'}
        };
    }
}