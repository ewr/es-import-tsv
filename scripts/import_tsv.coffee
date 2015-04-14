elasticsearch   = require "elasticsearch"
CSV             = require "csv"
debug           = require("debug")("es-import-tsv")
tz              = require "timezone"

argv = require('yargs')
    .demand(['index','type'])
    .describe
        server:     "Elasticsearch Server"
        index:      "Elasticsearch Index"
        type:       "ES Type"
        zone:       "Timezone"
        verbose:    "Show Debugging Logs"
        names:      "Field prefixes with NAMF, NAML"
    .boolean(['verbose'])
    .default
        server:     "localhost:9200"
        verbose:    false
        zone:       "America/Los_Angeles"
    .argv

if argv.verbose
    (require "debug").enable("es-import-tsv")
    debug = require("debug")("es-import-tsv")

zone = tz(require("timezone/#{argv.zone}"))

es = new elasticsearch.Client host:argv.server, apiVersion:"1.4"

csv = CSV.parse delimiter:"\t", columns:true, highWaterMark:1024

argv.names = argv.names.split(",") if argv.names

#----------

class Importer extends require("stream").Writable
    constructor: ->
        super objectMode:true, highWaterMark:100
        @_count = 0

    _write: (batch,encoding,cb) ->
        bulk = []

        for obj in batch
            bulk.push index:{}
            bulk.push obj

        es.bulk index:argv.index, type:argv.type, body:bulk, (err,resp) =>
            if err
                console.error "Error inserting into ES: #{err}"

            @_count += batch.length

            console.log "Rows inserted: #{@_count}"

            cb()

#----------

class CleanAndBatch extends require("stream").Transform
    constructor: ->
        super objectMode:true, highWaterMark:100

        @DATE_MATCH = /_DATE$/
        @NUMBER_MATCH = /(?:AMOUNT|_YTD)$/
        @BLANK_MATCH = /^\s?$/

        @_batch = []

    _transform: (obj,encoding,cb) ->
        for k,v of obj
            obj[k] = v = null if @BLANK_MATCH.test(v)

            if v && @DATE_MATCH.test k
                # parse as JS date, but then pass into tz for timezone. we insert UTC into ES
                d = new Date(v)
                new_v = tz(
                    zone(
                        [d.getFullYear(),d.getMonth(),d.getDate(),d.getHours(),d.getMinutes()]
                        ,argv.zone
                    ),
                    "UTC",
                    "%Y-%m-%dT%H:%M"
                )
                obj[k] = new_v

            else if v && @NUMBER_MATCH.test k
                obj[k] = parseFloat(v)

            else
                # assume a string

        for prefix in argv.names||[]
            if obj["#{prefix}_NAMF"] || obj["#{prefix}_NAML"]
                obj["#{prefix}_NAME"] = [obj["#{prefix}_NAMF"],obj["#{prefix}_NAML"]].join(" ")

        @_batch.push obj

        if @_batch.length >= 5000
            b = @_batch.splice(0)
            @push b

        cb()

#----------

importer    = new Importer
cleaner     = new CleanAndBatch

# create the index/type if it doesn't exist and apply our dynamic mapping
es.indices.existsType index:argv.index, type:argv.type, (err,exists) ->
    throw err if err

    _go = ->
        process.stdin.pipe(csv).pipe(cleaner).pipe(importer)

    if !exists
        mapping =
            dynamic_templates: [
                strings:
                    match:              "*"
                    match_mapping_type: "string",
                    mapping:
                        type: "string",
                        fields:
                            raw:   { type:"string", index: "not_analyzed" }
            ]

        es.indices.create index:argv.index, (err,res) ->
            throw err if err

            es.indices.putMapping index:argv.index, type:argv.type, body:mapping, (err,res) ->
                throw err if err

                _go()
    else
        _go()

