// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import {EventEmitter} from 'events';

import ESClient from './ESClient';
import LanguageDetector from './LanguageDetector';

import * as Constants from './Constants';
import buildApiSchema from './ApiSchemaBuilder';

import ValidationError from './ValidationError';

class SearcherInternal {
    constructor(searchConfig) {
        // TODO: compile config, so searcher logic has lesser checks, extend search config with default configs
        this.searchConfig = SearcherInternal.validateSearchConfig(searchConfig);
        this.apiSchema = buildApiSchema(searchConfig);
        this.esClient = new ESClient();
        this.transliterator = searchConfig.transliterator;
        this.languageDetector = new LanguageDetector();

        this.eventEmitter = new EventEmitter();

        // todo: default registry of event handler for storing search queries in DB.

        if (searchConfig.eventHandlers) {
            _.forEach(searchConfig.eventHandlers, (handlerOrArray, eventName) => {
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
            throw new ValidationError('InputAnalyzer must be defined in search config');
        }

        return searchConfig;
    }

    static validateInput(input, schema) {
        if (!input) {
            throw new ValidationError('No input provided', {code: 'NO_INPUT_ERROR'});
        }

        // validate it is valid type...
        const validationResult = Joi.validate(input, schema);
        if (validationResult.error) {
            console.error('Error: ', validationResult.error);
            throw new ValidationError('Non conforming format', {code: 'INVALID_FORMAT_ERROR', details: validationResult.error});
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
            throw new ValidationError(`No index type config found for: ${type}`);
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
            _.map(sortConfigs, config => this.buildDefaultSort(config, defaultSortOrder));
        } else {
            if (_.isObject(sortConfigs)) {
                return this.buildDefaultSort(sortConfigs, defaultSortOrder);
            }
        }
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
                          _.forEach(part.language, lang => queryLanguages[lang] = true);
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

              return {
                  index: indexTypeConfig.index,
                  type: indexTypeConfig.type,
                  search: {
                      from: (input.page || 0) * (input.count || 0),
                      size: input.count || undefined,
                      sort: this.sortPart(searchTypeConfig, input) || undefined,
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

    processAutocompleteResponse(responses) {
        if (!responses) {
            return null;
        }

        const result = {
            results: {}
        };

        _.forEach(responses.responses, (response) => {
            result.queryTimeTaken = Math.max(result.queryTimeTaken || 0, response.took);

            if (response.hits && response.hits.hits) {
                _.forEach(response.hits.hits, hit => {
                    const typeConfig = this.searchConfig.types[hit._type];
                    const shortName = !!typeConfig ? typeConfig.name || typeConfig.type : hit._type;
                    (result.results[shortName] || (result.results[shortName] = [])).push(_.extend(hit._source, {_id: hit._id, _score: hit._score, _type: hit._type}));
                });
            }
        });

        return result;
    }

    autocomplete(headers, input) {
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.autocomplete);

        let queryLanguages = null;

        return this.analyzeInput(input.text)
          .then((response) => response && response.tokens && _.map(response.tokens, token => token.token))
          .then((tokens) => {
              if (!tokens) {
                  return null;
              }

              if (!validatedInput.type || validatedInput.type === '*') {
                  const searchQueries = _(this.searchConfig.autocomplete.types).values().map(typeConfig => this.searchQuery(typeConfig, input, tokens)).value();

                  return Promise.all(searchQueries);
              }

              const autocompleteTypeConfig = this.searchConfig.autocomplete.types[validatedInput.type];
              if (!autocompleteTypeConfig) {
                  throw new ValidationError(`No autocomplete type config found for: ${validatedInput.type}`);
              }

              return this.searchQuery(autocompleteTypeConfig, input, tokens);
          })
          .then(queryOrArray => {
              if (_.isArray(queryOrArray)) {
                  queryLanguages = _.head(queryOrArray).queryLanguages;
              } else {
                  queryLanguages = queryOrArray.queryLanguages;
              }

              return queryOrArray;
          })
          .then(queryOrArray => {
              if (_.isArray(queryOrArray)) {
                  return this.esClient.multiSearch(queryOrArray);
              }

              return this.esClient.search(queryOrArray);
          })
          .then((response) => this.processAutocompleteResponse(response))
          .then(result => {
              this.eventEmitter.emit(Constants.AUTOCOMPLETE_EVENT, {headers, queryData: input, queryLanguages, result});

              return result;
          });
    }

    static processSearchResponse(response) {
        if (!response) {
            return null;
        }

        return {
            queryTimeTaken: response.took,
            totalResults: response.hits && response.hits.total || 0,
            results: response.hits && _.map(response.hits.hits, (hit) => _.extend(hit._source, {_id: hit._id, _score: hit._score, _type: hit._type})) || []
        };
    }

    search(headers, input) {
        const validatedInput = SearcherInternal.validateInput(input, this.apiSchema.search);

        let queryLanguages = null;

        const searchTypeConfig = this.searchConfig.search.types[validatedInput.type];
        if (!searchTypeConfig) {
            throw new ValidationError(`No search type config found for: ${validatedInput.type}`);
        }

        return Promise.resolve(this.searchQuery(searchTypeConfig, validatedInput))
          .then(query => {
              queryLanguages = query.queryLanguages;

              return query;
          })
          .then(query => this.esClient.search(query))
          .then(SearcherInternal.processSearchResponse)
          .then(result => {
              this.eventEmitter.emit(Constants.SEARCH_EVENT, {headers, queryData: input, queryLanguages, result});

              return result;
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
            ':type/view': [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/:id/termVectors': {handler: this.termVectors, method: 'get'}
        };
    }
}