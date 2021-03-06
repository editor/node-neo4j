# TODO many of these functions take a callback but, in some cases, call the
# callback immediately (e.g. if a value is cached). we should probably make
# sure to always call callbacks asynchronously, to prevent race conditions.
# this can be done in Streamline syntax by adding one line before cases where
# we're returning immediately: process.nextTick _

status = require 'http-status'
request = require 'request'

util = require './util_'
adjustError = util.adjustError

Relationship = require './Relationship_'
Node = require './Node_'

module.exports = class GraphDatabase
    constructor: (url) ->
        @url = url

        # Cache
        @_root = null
        @_services = null

    # Database
    _purgeCache: ->
        @_root = null
        @_services = null

    _getRoot: (_) ->
        if @_root?
            return @_root

        try
            response = request.get @url, _

            if response.statusCode isnt status.OK
                throw response

            @_root = JSON.parse response.body
            return @_root

        catch error
            throw adjustError error

    getServices: (_) ->
        if @_services?
            return @_services

        try
            root = @_getRoot _
            response = request.get root.data, _

            if response.statusCode isnt status.OK
                throw response

            @_services = JSON.parse response.body
            return @_services

        catch error
            throw adjustError error

    getVersion: (_) ->
        try
            services = @getServices _

            # Neo4j 1.5 onwards report their version number here;
            # if it's not there, assume Neo4j 1.4.
            parseFloat services['neo4j_version'] or '1.4'

        catch error
            throw adjustError

    listRelationshipTypes: (_) ->
        try
            services = @getServices _
            response = request.get services.relationship_types, _
            if response.statusCode isnt status.OK
                throw response			

            response = JSON.parse response.body
            return response

        catch error
            throw adjustError


    # Nodes
    createNode: (data) ->
        data = data || {}
        node = new Node this,
            data: data
        return node

    getNode: (url, _) ->
        try
            response = request.get url, _

            if response.statusCode isnt status.OK

                # Node not found
                if response.statusCode is status.NOT_FOUND
                    throw new Error "No node at #{url}"

                throw response

            node = new Node this, JSON.parse response.body
            return node

        catch error
            throw adjustError error

    deleteNodeEntry: (index, property, value, id, _) ->
      try
        services = @getServices _

        key = encodeURIComponent property
        val = encodeURIComponent value
        id = encodeURIComponent id
        url = "#{services.node_index}/#{index}/#{key}/#{val}/#{id}"
        console.log ( "node-neo4j > DELETE: " + url )

        response = request.del url, _

        if response.statusCode isnt 204
          throw response

      catch error
        throw adjustError error

    getIndexedNode: (index, property, value, _) ->
        try
            nodes = @getIndexedNodes index, property, value, _

            node = null
            if nodes and nodes.length > 0
                node = nodes[0]
            return node

        catch error
            throw adjustError error

    getIndexedNodes: (index, property, value, _) ->
        try
            services = @getServices _

            key = encodeURIComponent property
            val = encodeURIComponent value
            url = "#{services.node_index}/#{index}/#{key}/#{val}"

            response = request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            nodeArray = JSON.parse response.body
            nodes = nodeArray.map (node) =>
                new Node this, node
            return nodes

        catch error
            throw adjustError error

    getNodeById: (id, _) ->
        try
            services = @getServices _
            url = "#{services.node}/#{id}"
            node = @getNode url, _
            return node

        catch error
            throw adjustError error

    listNodeRelationships: (_, id, direction, types) ->
        try
            services = @getServices _
            console.log services
            direction = 'all' if ( direction isnt 'outgoing' or 'out' or  'incoming' or 'in' )
            types = [ types ] if ( 'string' is typeof types )
            types = if ( 'undefined' is typeof types or types.length < 1 ) then types = '' else types = types.join( '&' )
            url = "#{services.node}/#{id}/relationships/#{direction}/#{types}"
            console.log ( "listNodeRelationships() GET: " + url )

            response = request.get url, _
            if response.statusCode isnt status.OK
                throw response					

            console.log response.body
            return JSON.parse response.body

        catch error
            throw adjustError error

        # TODO: Implement

    getRelationship: (url, _) ->
        try
            response = request.get url, _

            if response.statusCode isnt status.OK
                # TODO: Handle 404
                throw response

            data = JSON.parse response.body

            # Construct relationship
            relationship = new Relationship this, data

            return relationship

        catch error
            throw adjustError error

    deleteRelationshipEntry: (index, property, value, id, _) ->
      try
        services = @getServices _

        key = encodeURIComponent property
        val = encodeURIComponent value
        id = encodeURIComponent id
        url = "#{services.relationship_index}/#{index}/#{key}/#{val}/#{id}"
        console.log ( "node-neo4j > DELETE: " + url )
        response = request.del url, _
        console.log ( "COFFEE CODE WAS " + ( response.statusCode ) )
        console.log ( "COFFEE RESPONSE WAS " + ( response.body ) )
        if response.statusCode isnt 204
          throw response

      catch error
        throw adjustError error


    getIndexedRelationship: (index, property, value, _) ->
        try
            relationships = @getIndexedRelationships index, property, value, _

            relationship = null
            if relationships and relationships.length > 0
                relationship = relationships[0]
            return relationship

        catch error
            throw adjustError error

    getIndexedRelationships: (index, property, value, _) ->
        try
            services = @getServices _

            key = encodeURIComponent property
            val = encodeURIComponent value
            url = "#{services.relationship_index}/#{index}/#{key}/#{val}"

            response = request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            relationshipArray = JSON.parse response.body
            relationships = relationshipArray.map (relationship) =>
                new Relationship this, relationship
            return relationships

        catch error
            throw adjustError error



    getRelationshipById: (id, _) ->
        services = @getServices _
        # FIXME: Neo4j doesn't expose the path to relationships
        relationshipURL = services.node.replace('node', 'relationship')
        url = "#{relationshipURL}/#{id}"
        @getRelationship url, _

    # wrapper around the Cypher plugin, which comes bundled w/ Neo4j.
    # pass in the Cypher query as a string (can be multi-line).
    # http://docs.neo4j.org/chunked/stable/cypher-query-lang.html
    # returns an array of "rows" (matches), where each row is a map from
    # variable name (as given in the passed in query) to value. any values
    # that represent nodes or relationships are transformed to instances.
    query: (_, query) ->
        try
            services = @getServices _
            endpoint = services.cypher or
                services.extensions?.CypherPlugin?['execute_query']

            if not endpoint
                throw new Error 'Cypher plugin not installed'

            response = request.post
                uri: endpoint
                json: {query}
            , _

            # XXX workaround for neo4j silent failures for invalid queries:
            if response.statusCode is status.NO_CONTENT
                throw new Error """
                    Unknown Neo4j error for query:

                    #{query}

                """

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success: build result maps, and transform nodes/relationships
            body = response.body    # JSON already parsed by request
            columns = body.columns
            results = for row in body.data
                map = {}
                for value, i in row
                    map[columns[i]] =
                        if value and typeof value is 'object' and value.self
                            if value.type then new Relationship this, value
                            else new Node this, value
                        else
                            value
                map
            return results

        catch error
            throw adjustError error

    # executes a query against the given node index. lucene syntax reference:
    # http://lucene.apache.org/java/3_1_0/queryparsersyntax.html
    queryNodeIndex: (index, query, _) ->
        try
            services = @getServices _
            url = "#{services.node_index}/#{index}?query=#{encodeURIComponent query}"
            console.log url

            response = request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            console.log( "THE CODE: " + response.statusCode )
            console.log response.body
            nodeArray = JSON.parse response.body
            nodes = nodeArray.map (node) =>
                new Node this, node
            return nodes

        catch error
            throw adjustError error

    queryRelationshipIndex: (index, query, _) ->
        try
            services = @getServices _
            url = "#{services.relationship_index}/#{index}?query=#{encodeURIComponent query}"

            console.log url
            response = request.get url, _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            console.log( "THE CODE: " + response.statusCode )
            console.log response.body
            relationshipArray = JSON.parse response.body
            relationships = relationshipArray.map (relationship) =>
                new Relationship this, relationship
            return relationships

        catch error
            throw adjustError error



    # executes multiple API calls through a single HTTP call.
    # jobs param should be array of objects containing path, method
    # and optionally body, id such as:
    # { 'method' : 'POST', 'to' : '/node', 'body' : { 'age' : 1 }, 'id' : 0 }
    # currently returns the raw json from response since jobs can
    # return different objects (nodes, relationships, etc)
    # more info at:
    # http://docs.neo4j.org/chunked/stable/rest-api-batch-ops.html
    batch: (jobs, _) ->
        try
            services = @getServices _
            url = services.batch

            response = request.post
                uri: url
                json: jobs
            , _

            if response.statusCode isnt status.OK
                # Database error
                throw response

            # Success
            res = response.body
            return res

        catch error
            throw adjustError error
