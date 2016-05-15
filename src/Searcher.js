// jscs:disable requireCamelCaseOrUpperCaseIdentifiers
import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import ESClient from './ESClient';
import * as Constants from './Constants';
import buildApiSchema from './ApiSchemaBuilder';
import SearchEventHandler from './SearchEventHandler';
import LanguageDetector from 'humane-node-commons/lib/LanguageDetector';
import ValidationError from 'humane-node-commons/lib/ValidationError';

/* eslint-disable no-underscore-dangle */

class SearcherInternal {
    constructor(config) {
        this.logLevel = config.logLevel || 'info';
        this.instanceName = config.instanceName;

        const langFilter = {
            field: '_lang',
            termQuery: true,
            value: (value) => {
                if (value.secondary) {
                    return _.union([value.primary], value.secondary);
                }

                return value.primary;
            }
        };

        const DefaultTypes = {
            searchQuery: {
                type: 'searchQuery',
                index: `${_.toLower(this.instanceName)}:search_query_store`,
                filters: {
                    lang: langFilter,
                    hasResults: {
                        field: 'hasResults',
                        termQuery: true,
                        defaultValue: true
                    }
                }
            }
        };

        const DefaultAutocomplete = {
            defaultType: '*',
            types: {
                searchQuery: {
                    indexType: DefaultTypes.searchQuery,
                    queryFields: [
                        {
                            field: 'unicodeQuery',
                            vernacularOnly: true,
                            weight: 10
                        },
                        {
                            field: 'query',
                            weight: 9.5
                        }
                    ]
                }
            }
        };

        const DefaultSearch = {
            defaultType: '*'
        };

        const DefaultViews = {
            types: {
                searchQuery: {
                    indexType: DefaultTypes.searchQuery,
                    sort: {count: true},
                    filters: {
                        hasResults: {
                            field: 'hasResults',
                            termQuery: true
                        }
                    }
                }
            }
        };

        const DefaultEventHandlers = {
            search: data => new SearchEventHandler(this.instanceName).handle(data)
        };

        // TODO: compile config, so searcher logic has lesser checks
        // this.searchConfig = SearcherInternal.validateSearchConfig(config.searchConfig);
        this.searchConfig = _.defaultsDeep(config.searchConfig, {
            types: DefaultTypes,
            autocomplete: DefaultAutocomplete,
            search: DefaultSearch,
            views: DefaultViews
        });

        const indices = this.searchConfig.indices || {};

        _.forEach(this.searchConfig.types, (type, key) => {
            if (!type.type) {
                type.type = key;
            }

            if (!type.index) {
                let index = indices[type.type];
                if (!index) {
                    // we build index
                    indices[type.type] = index = {
                        store: `${_.toLower(this.instanceName)}:${_.snakeCase(type.type)}_store`
                    };
                }

                type.index = index.store;
            }

            if (!type.sort) {
                type.sort = [];
            }

            // add push by default
            type.sort.push('score');

            if (!type.filters) {
                type.filters = {};
            }

            if (!type.filters.lang) {
                type.filters.lang = langFilter;
            }
        });

        this.apiSchema = buildApiSchema(config.searchConfig);
        this.esClient = new ESClient(_.pick(config, ['logLevel', 'esConfig', 'redisConfig', 'redisSentinelConfig']));
        this.transliterator = config.transliterator;
        this.languageDetector = new LanguageDetector();

        this.eventEmitter = new EventEmitter();

        this.registerEventHandlers(DefaultEventHandlers);
        this.registerEventHandlers(config.searchConfig.eventHandlers);

        console.log('Final search config for instance: ', this.instanceName, JSON.stringify(this.searchConfig));
    }

    registerEventHandlers(eventHandlers) {
        if (!eventHandlers) {
            return;
        }

        _.forEach(eventHandlers, (handlerOrArray, eventName) => {
            if (_.isArray(handlerOrArray)) {
                _.forEach(handlerOrArray, handler => this.eventEmitter.addListener(eventName, handler));
            } else {
                this.eventEmitter.addListener(eventName, handlerOrArray);
            }
        });
    }

    // TODO: validate it through Joi
    // TODO: provide command line tool to validate config
    // validateSearchConfig(searchConfig) {
    //     return searchConfig;
    // }

    validateInput(input, schema) {
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

    constantScoreQuery(fieldConfig, query) {
        if (fieldConfig.filter) {
            return query;
        }

        const boost = (fieldConfig.weight || 1.0);

        if (boost === 1.0) {
            return query;
        }

        return {constant_score: {query, boost}};
    }

    wrapQuery(fieldConfig, query) {
        return this.constantScoreQuery(fieldConfig, fieldConfig.nestedPath ? {nested: {path: fieldConfig.nestedPath, query}} : query);
    }

    humaneQuery(fieldConfig, text) {
        return {
            humane_query: {
                [fieldConfig.field]: {
                    query: text,
                    boost: fieldConfig.weight,
                    vernacularOnly: fieldConfig.vernacularOnly,
                    path: fieldConfig.nestedPath,
                    noFuzzy: fieldConfig.noFuzzy
                }
            }
        };
    }

    termQuery(fieldConfig, text) {
        const queryType = _.isArray(text) ? 'terms' : 'term';
        return {
            [queryType]: {
                [fieldConfig.field]: text
            }
        };
    }

    query(fieldConfig, text) {
        // only for filter we allow termQuery
        if (fieldConfig.termQuery && fieldConfig.filter) {
            return this.termQuery(fieldConfig, text);
        }

        return this.humaneQuery(fieldConfig, text);
    }

    buildFieldQuery(fieldConfig, englishTerm, queries) {
        const query = this.wrapQuery(fieldConfig, this.query(fieldConfig, englishTerm));

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

    buildTypeQuery(searchTypeConfig, text, fuzzySearch) {
        // console.log('Fuzzy Search: ', fuzzySearch, !fuzzySearch || undefined);

        // // TODO: language detection is not needed immediately, but shall be moved to esplugin
        // const languages = this.languageDetector.detect(text);
        //
        // let englishTerm = text;
        // let vernacularTerm = null;
        // if (!(!languages || languages.length === 1 && languages[0].code === 'en') && this.transliterator) {
        //     // it's vernacular
        //     vernacularTerm = text;
        //     englishTerm = this.transliterator.transliterate(vernacularTerm);
        // }
        //
        // const indexTypeConfig = searchTypeConfig.indexType;
        //
        // const queries = [];
        // _.forEach(searchTypeConfig.queryFields || indexTypeConfig.queryFields, fieldConfig => this.buildFieldQuery(fieldConfig, englishTerm, vernacularTerm, queries));
        //
        // return {
        //     query: queries.length > 1 ? {dis_max: {queries}} : queries[0],
        //     language: languages && _.map(languages, lang => lang.code)
        // };

        const indexTypeConfig = searchTypeConfig.indexType;
        const queryFields = _(searchTypeConfig.queryFields || indexTypeConfig.queryFields).filter(queryField => !queryField.vernacularOnly).value();

        if (!queryFields) {
            throw new ValidationError('No query fields defined', {details: {code: 'NO_QUERY_FIELDS_DEFINED'}});
        } else if (queryFields.length === 1) {
            const queryField = queryFields[0];

            return {
                query: this.wrapQuery(queryField, {
                    humane_query: {
                        [queryField.field]: {
                            query: text,
                            boost: queryField.weight,
                            vernacularOnly: queryField.vernacularOnly,
                            noFuzzy: !fuzzySearch || queryField.noFuzzy
                        }
                    }
                })
            };
        }
        return {
            query: {
                multi_humane_query: {
                    query: text,
                    fields: _(queryFields)
                      .map(queryField => ({
                          field: queryField.field,
                          boost: queryField.weight,
                          vernacularOnly: queryField.vernacularOnly,
                          path: queryField.nestedPath,
                          noFuzzy: !fuzzySearch || queryField.noFuzzy
                      }))
                      .value()

                }
            }
        };
    }

    filterPart(searchTypeConfig, input, termLanguages, facetFilter) {
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

            if (input.filter && input.filter[key]) {
                filterValue = input.filter[key];
            } else if (filterConfig.defaultValue) {
                filterValue = filterConfig.defaultValue;
            }

            if (filterValue) {
                const filterType = filterValue.type;
                if (filterValue.values && filterType) {
                    filterValue = filterValue.values;
                }

                if (facetFilter && (!filterType || filterType !== 'facet')
                  || !facetFilter && filterType && filterType === 'facet') {
                    return true;
                }

                if (filterConfig.value && _.isFunction(filterConfig.value)) {
                    filterValue = filterConfig.value(filterValue);
                }

                this.buildFieldQuery(_.extend({filter: true}, filterConfigs[key]), filterValue, filterQueries);
            }

            return true;
        });

        if (input.lang && !_.isEmpty(input.lang)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), input.lang, filterQueries);
        }

        if (termLanguages && !_.isEmpty(termLanguages)) {
            this.buildFieldQuery(_.extend({filter: true}, filterConfigs.lang), termLanguages, filterQueries);
        }

        if (filterQueries.length === 0) {
            return undefined;
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

    // todo: see the usage of it...
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

            if (input.filter && input.filter[key]) {
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

    defaultSortOrder() {
        return this.searchConfig.defaultSortOrder || Constants.DESC_SORT_ORDER;
    }

    // todo: handle case of filtering only score based descending order, as it is default anyways
    buildSort(value) {
        // array of string
        if (_.isString(value)) {
            return {[value]: _.lowerCase(this.defaultSortOrder())};
        } else if (_.isObject(value)) {
            // array of sort objects
            return {[value.field]: _.lowerCase(value.order || this.defaultSortOrder())};
        }

        return null;
    }

    buildDefaultSort(config) {
        if (_.isObject(config)) {
            return _(config)
              .map((value, key) => {
                  if (value && (_.isBoolean(value) || _.isObject(value) && value.default)) {
                      // include this key
                      return this.buildSort(key);
                  }

                  return null;
              })
              .filter(value => !!value)
              .value();
        }

        return null;
    }

    sortPart(searchTypeConfig, input) {
        // build sort
        if (input.sort) {
            if (_.isArray(input.sort)) {
                return _(input.sort)
                  .map(value => this.buildSort(value))
                  .filter(value => !!value)
                  .value();
            }

            return this.buildSort(input.sort);
        }

        const sortConfigs = searchTypeConfig.sort || searchTypeConfig.indexType.sort;
        if (!sortConfigs) {
            return undefined;
        }

        // pick default from sort config
        if (_.isArray(sortConfigs)) {
            return _(sortConfigs)
              .map(config => this.buildDefaultSort(config))
              .filter(config => !!config)
              .value();
        }

        if (_.isObject(sortConfigs)) {
            return this.buildDefaultSort(sortConfigs);
        }

        return undefined;
    }

    facet(facetConfig) {
        if (!facetConfig.key) {
            throw new ValidationError('No name defined for facet', {details: {code: 'NO_FACET_NAME_DEFINED'}});
        }

        if (!facetConfig.type) {
            throw new ValidationError('No facet type defined', {details: {code: 'NO_FACET_TYPE_DEFINED', facetName: facetConfig.key}});
        }

        if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && !facetConfig.field) {
            throw new ValidationError('No facet field defined', {details: {code: 'NO_FACET_FIELD_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
        }

        const facetKey = facetConfig.key;
        let facetValue = null;

        if (facetConfig.type === 'field') {
            facetValue = {
                terms: {
                    field: facetConfig.field
                }
            };
        } else if (facetConfig.type === 'ranges') {
            if (!facetConfig.ranges) {
                throw new ValidationError('No ranges defined for range type facet', {details: {code: 'NO_RANGES_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
            }

            facetValue = {
                range: {
                    field: facetConfig.field,
                    ranges: _.map(facetConfig.ranges, range => {
                        if (!range.key) {
                            throw new ValidationError('No range facet key defined', {details: {code: 'NO_RANGE_FACET_KEY_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
                        }

                        if (!range.from && !range.to) {
                            throw new ValidationError('None of range from & to defined', {details: {code: 'NO_RANGE_ENDS_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
                        }

                        return {
                            key: range.key,
                            from: range.from,
                            to: range.to
                        };
                    })
                }
            };
        } else if (facetConfig.type === 'filters') {
            if (!facetConfig.filters) {
                throw new ValidationError('No filters defined for filters type facet', {details: {code: 'NO_FILTERS_DEFINED', facetName: facetConfig.key, facetType: facetConfig.type}});
            }

            const filters = {};

            _.forEach(facetConfig.filters, filter => {
                filters[filter.key] = filter.filter;
            });

            facetValue = {
                filters: {filters}
            };
        } else {
            // throw error here
            throw new ValidationError('Unknown facet type', {details: {code: 'UNKNOWN_FACET_TYPE', facetName: facetConfig.key, facetType: facetConfig.type}});
        }

        if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && facetConfig.nestedPath) {
            facetValue = {
                nested: {
                    path: facetConfig.nestedPath
                },
                aggs: {
                    nested: facetValue
                }
            };
        }

        return {
            key: facetKey,
            value: facetValue
        };
    }

    facetsPart(searchTypeConfig) {
        if (!searchTypeConfig.facets) {
            return null;
        }

        let facetConfigs = searchTypeConfig.facets;
        if (!_.isArray(facetConfigs)) {
            facetConfigs = [facetConfigs];
        }

        const facets = {};

        _.forEach(facetConfigs, facetConfig => {
            const facet = this.facet(facetConfig);
            facets[facet.key] = facet.value;
        });

        return facets;
    }

    searchQuery(searchTypeConfig, input, text) {
        if (this.instanceName === '1mg') {
            // fix text
            text = _(text)
              .replace(/(^|[\s]|[^0-9]|[^a-z])([0-9]+)[\s]+(mg|mcg|ml|%)/gi, '$1$2$3')
              .replace(/(^|[\s]|[^0-9]|[^a-z])\.([0-9]+)[\s]*(mg|mcg|ml|%)/gi, '$10.$2$3')
              .trim();
        }

        return Promise.resolve(this.buildTypeQuery(searchTypeConfig, text, input.fuzzySearch))
          .then(({query, queryLanguages}) => {
              const indexTypeConfig = searchTypeConfig.indexType;

              let sort = this.sortPart(searchTypeConfig, input) || undefined;
              if (sort && _.isEmpty(sort)) {
                  sort = undefined;
              }

              let facets = this.facetsPart(searchTypeConfig) || undefined;
              if (facets && _.isEmpty(facets)) {
                  facets = undefined;
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
                                  bool: {
                                      must: query,
                                      filter: this.filterPart(searchTypeConfig, input, _.keys(queryLanguages), false)
                                  }
                              },
                              field_value_factor: {
                                  field: '_weight',
                                  factor: 2.0,
                                  missing: 1
                              }
                          }
                      },
                      post_filter: this.filterPart(searchTypeConfig, input, _.keys(queryLanguages), true),
                      aggs: facets
                  },
                  queryLanguages
              };
          });
    }

    _processResponse(response, searchTypesConfig) {
        let type = null;
        let name = null;

        const results = [];

        if (response.hits && response.hits.hits) {
            let first = true;
            _.forEach(response.hits.hits, hit => {
                if (first || !type) {
                    type = hit._type;
                    const typeConfig = this.searchConfig.types[type];
                    name = (typeConfig && (typeConfig.name || typeConfig.type)) || type;

                    first = false;
                }

                results.push(_.defaults(_.pick(hit, ['_id', '_score', '_type', '_weight']), {_name: name}, hit._source));
            });
        }

        const searchTypeConfig = type && searchTypesConfig[type];

        let facets;
        if (searchTypeConfig && searchTypeConfig.facets && response.aggregations) {
            facets = {};
            let facetConfigs = searchTypeConfig.facets;
            if (!_.isArray(facetConfigs)) {
                facetConfigs = [facetConfigs];
            }

            _.forEach(facetConfigs, facetConfig => {
                let facet = response.aggregations[facetConfig.key];

                if (!facet) {
                    return true;
                }

                if ((facetConfig.type === 'field' || facetConfig.type === 'ranges') && facetConfig.path) {
                    facet = facet[facetConfig.key];
                }

                const buckets = facet.buckets;

                facets[facetConfig.key] = _.map(buckets, bucket => ({
                    key: bucket.key,
                    count: bucket.doc_count,
                    from: facet.from,
                    from_as_string: facet.from_as_string,
                    to: facet.to,
                    to_as_string: facet.to_as_string
                }));

                return true;
            });
        }

        return {type, name, results, facets, queryTimeTaken: response.took, totalResults: _.get(response, 'hits.total', 0)};
    }

    processMultipleSearchResponse(responses, searchTypesConfig) {
        if (!responses) {
            return null;
        }

        const mergedResult = {
            multi: true,
            totalResults: 0,
            results: {}
        };

        _.forEach(responses.responses, (response) => {
            const result = this._processResponse(response, searchTypesConfig);

            if (!result || !result.type || !result.name || !result.results || result.results.length === 0) {
                return;
            }

            mergedResult.queryTimeTaken = Math.max(mergedResult.queryTimeTaken || 0, result.queryTimeTaken);
            mergedResult.results[result.name] = result;
            mergedResult.totalResults += result.totalResults;
        });

        return mergedResult;
    }

    processSingleSearchResponse(response, searchTypesConfig) {
        if (!response) {
            return null;
        }

        return this._processResponse(response, searchTypesConfig);
    }

    _searchInternal(headers, input, searchApiConfig, eventName) {
        let queryLanguages = null;

        let multiSearch = false;

        let promise = null;

        const searchTypeConfigs = searchApiConfig.types;

        let responsePostProcessor = null;

        if (!input.type || input.type === '*') {
            responsePostProcessor = searchApiConfig.multiResponsePostProcessor;

            const searchQueries = _(searchTypeConfigs)
              .values()
              .map(typeConfig => this.searchQuery(typeConfig, input, input.text))
              .value();

            multiSearch = _.isArray(searchQueries) || false;

            promise = Promise.all(searchQueries);
        } else {
            const searchTypeConfig = searchTypeConfigs[input.type];

            if (!searchTypeConfig) {
                throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'SEARCH_CONFIG_NOT_FOUND', type: input.type}});
            }

            responsePostProcessor = searchTypeConfig.responsePostProcessor;

            promise = this.searchQuery(searchTypeConfig, input, input.text);
        }

        return Promise.resolve(promise)
          .then(queryOrArray => {
              if (multiSearch) {
                  queryLanguages = _.head(queryOrArray).queryLanguages;
              } else {
                  queryLanguages = queryOrArray.queryLanguages;
              }

              return queryOrArray;
          })
          .then(queryOrArray => {
              if (multiSearch) {
                  return this.esClient.multiSearch(queryOrArray);
              }

              return this.esClient.search(queryOrArray);
          })
          .then((response) => {
              if (multiSearch) {
                  return this.processMultipleSearchResponse(response, searchTypeConfigs);
              }

              return this.processSingleSearchResponse(response, searchTypeConfigs);
          })
          .then(response => {
              this.eventEmitter.emit(eventName, {headers, queryData: input, queryLanguages, queryResult: response});

              if (responsePostProcessor && input.format === 'custom') {
                return responsePostProcessor(response);
              }

              return response;
          });
    }

    autocomplete(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete, Constants.AUTOCOMPLETE_EVENT);
    }

    search(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.search);

        return this._searchInternal(headers, validatedInput, this.searchConfig.search, Constants.SEARCH_EVENT);
    }

    didYouMean(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.didYouMean);

        const types = this.searchConfig.types;

        let index = null;
        if (!input.type || input.type === '*') {
            index = _(types)
              .map(typeConfig => typeConfig.index)
              .filter(indexName => !indexName.match(/search_query_store/))
              .join(',');
        } else {
            const typeConfig = types[input.type];
            if (!typeConfig) {
                throw new ValidationError(`No type config found for: ${input.type}`, {details: {code: 'TYPE_CONFIG_NOT_FOUND', type: input.type}});
            }

            index = typeConfig.index;
        }

        return Promise.resolve(this.esClient.didYouMean(index, validatedInput.text));
    }

    suggestedQueries(headers, input) {
        // same type as autocomplete
        const validatedInput = this.validateInput(input, this.apiSchema.autocomplete);

        return this._searchInternal(headers, validatedInput, this.searchConfig.autocomplete, Constants.SUGGESTED_QUERIES_EVENT)
          .then(response => {
              // merge
              if (response.multi) {
                  // calculate scores
                  const relevancyScores = [];
                  _.forEach(response.results, resultGroup => {
                      _.forEach(resultGroup.results, result => {
                          result._relevancyScore = result._score / (result._weight || 1.0);
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
        return this._explain(Constants.AUTOCOMPLETE_API, this.validateInput(input, this.apiSchema.explainAutocomplete));
    }

    explainSearch(headers, input) {
        return this._explain(Constants.SEARCH_API, this.validateInput(input, this.apiSchema.explainSearch));
    }

    termVectors(headers, input) {
        const validatedInput = this.validateInput(input, this.apiSchema.termVectors);

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

    didYouMean(headers, request) {
        return this.internal.didYouMean(headers, request);
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
            didYouMean: [
                // {handler: this.didYouMean},
                {handler: this.didYouMean, method: 'get'}
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
            ':type/didYouMean': [
                // {handler: this.didYouMean},
                {handler: this.didYouMean, method: 'get'}
            ],
            ':type/view': [
                {handler: this.view},
                {handler: this.view, method: 'get'}
            ],
            ':type/:id/termVectors': {handler: this.termVectors, method: 'get'}
        };
    }
}