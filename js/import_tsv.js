var CSV, CleanAndBatch, Importer, argv, cleaner, csv, debug, elasticsearch, es, importer, tz, zone,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

elasticsearch = require("elasticsearch");

CSV = require("csv");

debug = require("debug")("caldata");

tz = require("timezone");

argv = require('yargs').demand(['index', 'type']).describe({
  index: "Elasticsearch Index",
  type: "ES Type",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  names: "Field prefixes with NAMF, NAML"
}).boolean(['verbose'])["default"]({
  verbose: false,
  zone: "America/Los_Angeles"
}).argv;

if (argv.verbose) {
  (require("debug")).enable("caldata");
  debug = require("debug")("caldata");
}

zone = tz(require("timezone/" + argv.zone));

es = new elasticsearch.Client({
  host: "localhost:9200",
  apiVersion: "1.4"
});

csv = CSV.parse({
  delimiter: "\t",
  columns: true,
  highWaterMark: 1024
});

if (argv.names) {
  argv.names = argv.names.split(",");
}

Importer = (function(_super) {
  __extends(Importer, _super);

  function Importer() {
    Importer.__super__.constructor.call(this, {
      objectMode: true,
      highWaterMark: 100
    });
    this._count = 0;
  }

  Importer.prototype._write = function(batch, encoding, cb) {
    var bulk, obj, _i, _len;
    bulk = [];
    for (_i = 0, _len = batch.length; _i < _len; _i++) {
      obj = batch[_i];
      bulk.push({
        index: {}
      });
      bulk.push(obj);
    }
    return es.bulk({
      index: argv.index,
      type: argv.type,
      body: bulk
    }, (function(_this) {
      return function(err, resp) {
        if (err) {
          console.error("Error inserting into ES: " + err);
        }
        _this._count += batch.length;
        console.log("Rows inserted: " + _this._count);
        return cb();
      };
    })(this));
  };

  return Importer;

})(require("stream").Writable);

CleanAndBatch = (function(_super) {
  __extends(CleanAndBatch, _super);

  function CleanAndBatch() {
    CleanAndBatch.__super__.constructor.call(this, {
      objectMode: true,
      highWaterMark: 100
    });
    this.DATE_MATCH = /_DATE$/;
    this.NUMBER_MATCH = /(?:AMOUNT|_YTD)$/;
    this.BLANK_MATCH = /^\s?$/;
    this._batch = [];
  }

  CleanAndBatch.prototype._transform = function(obj, encoding, cb) {
    var b, d, k, new_v, prefix, v, _i, _len, _ref;
    for (k in obj) {
      v = obj[k];
      if (this.BLANK_MATCH.test(v)) {
        obj[k] = v = null;
      }
      if (v && this.DATE_MATCH.test(k)) {
        d = new Date(v);
        new_v = tz(zone([d.getFullYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes()], argv.zone), "UTC", "%Y-%m-%dT%H:%M");
        obj[k] = new_v;
      } else if (v && this.NUMBER_MATCH.test(k)) {
        obj[k] = parseFloat(v);
      } else {

      }
    }
    _ref = argv.names || [];
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      prefix = _ref[_i];
      if (obj["" + prefix + "_NAMF"] || obj["" + prefix + "_NAML"]) {
        obj["" + prefix + "_NAME"] = [obj["" + prefix + "_NAMF"], obj["" + prefix + "_NAML"]].join(" ");
      }
    }
    this._batch.push(obj);
    if (this._batch.length >= 5000) {
      b = this._batch.splice(0);
      this.push(b);
    }
    return cb();
  };

  return CleanAndBatch;

})(require("stream").Transform);

importer = new Importer;

cleaner = new CleanAndBatch;

es.indices.existsType({
  index: argv.index,
  type: argv.type
}, function(err, exists) {
  var mapping, _go;
  if (err) {
    throw err;
  }
  _go = function() {
    return process.stdin.pipe(csv).pipe(cleaner).pipe(importer);
  };
  if (!exists) {
    mapping = {
      dynamic_templates: [
        {
          strings: {
            match: "*",
            match_mapping_type: "string",
            mapping: {
              type: "string",
              fields: {
                raw: {
                  type: "string",
                  index: "not_analyzed"
                }
              }
            }
          }
        }
      ]
    };
    return es.indices.create({
      index: argv.index
    }, function(err, res) {
      if (err) {
        throw err;
      }
      return es.indices.putMapping({
        index: argv.index,
        type: argv.type,
        body: mapping
      }, function(err, res) {
        if (err) {
          throw err;
        }
        return _go();
      });
    });
  } else {
    return _go();
  }
});

//# sourceMappingURL=import_tsv.js.map
