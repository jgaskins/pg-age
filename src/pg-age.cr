require "db"
require "pg"
require "uuid/json"
require "uri/json"

# [AGE](https://age.apache.org) is a Postgres extension allowing storage and
# querying of graph data inside of Postgres using the Cypher query language, an
# initiative of [the openCypher project](http://opencypher.org). The `AGE::Client`
# client wraps a [`DB::Database`](http://crystal-lang.github.io/crystal-db/api/0.11.0/DB/Database.html)
# and provides an API to query graph databases inside of Postgres. You must have
# the `age` extension installed inside the Postgres server in order to use this
# shard.
module AGE
  VERSION = "0.1.0"

  class Error < ::Exception
  end

  class InvalidAGEResult < Error
  end

  class InvalidNode < Error
  end

  class InvalidRelationship < Error
  end

  class SerializableError < Error
  end

  class Client
    def initialize(@db : DB::Database)
      @db.setup_connection do |c|
        c.exec "LOAD 'age'"
        c.exec %{SET search_path = ag_catalog, "$user", public}
      end
    end

    def graph(name : String) : Graph
      Graph.new(@db, name)
    end

    def create_graph(name : String) : Graph
      @db.exec "SELECT 1 FROM create_graph($1)", name
      graph(name)
    end

    def drop_graph(name : String, cascade : Bool = true) : Nil
      @db.exec "SELECT 1 FROM drop_graph($1, $2)", name, cascade
    end
  end

  struct Graph
    def initialize(@db : DB::Database, @name : String)
    end

    def query(cypher : String, as types : T, **args) forall T
      {% begin %}
        {% if T < Tuple %}
          results = Array({ {{T.type_vars.map(&.instance).join(", ").id}} }).new
        {% else %}
          results = Array({{T.instance}}).new
        {% end %}

        query(cypher, types, **args) do |result|
          results << result
        end

        results
      {% end %}
    end

    def query(cypher : String, as types : T, **args) forall T
      sql = String.build do |sql|
        sql << <<-SQL
          SELECT * FROM cypher(
            '#{@name}',
            $CYPHER_QUERY$
              #{cypher}
            $CYPHER_QUERY$,
            $1
          ) AS (
          SQL

        {% if T < Tuple %}
          types.each_with_index do |_, index|
            sql << "  result#{index} agtype"
            if index < types.size - 1
              sql << ",\n"
            end
          end
        {% else %}
          sql << "  result agtype\n"
        {% end %}

        sql << "\n)"
      end

      @db.query sql, args.to_json do |rs|
        rs.each do
          {% begin %}
            {% if T < Tuple %}
              yield({
                {% for type in T %}
                  AGE.deserialize(rs.read.as(Bytes), as: {{type.instance}}),
                {% end %}
              })
            {% else %}
              yield AGE.deserialize(rs.read.as(Bytes), as: {{T.instance}})
            {% end %}
          {% end %}
        end
      end
    end

    def exec(cypher : String, **args)
      sql = <<-SQL
        SELECT * FROM cypher(
          '#{@name}',
          $$
            #{cypher}
          $$,
          $1
        ) AS (result agtype)
        SQL

      @db.exec sql, args.to_json
    end
  end

  def self.deserialize(bytes : Bytes, as type : T.class) : T forall T
    if bytes[0] != 1
      raise InvalidAGEResult.new("Value is not an AGE result: #{bytes.inspect}")
    end

    if bytes.size > 10 && bytes[-8..] == "::vertex".to_slice
      agtype = Type::Vertex
      slice = bytes[1...-8]
    elsif bytes.size > 8 && bytes[-6..] == "::edge".to_slice
      agtype = Type::Edge
      slice = bytes[1...-6]
    else
      slice = bytes[1...]
    end

    T.new(Value.new(String.new(slice.to_unsafe, slice.bytesize), agtype))
  end

  enum Type
    Vertex
    Edge
  end

  record Value,
    string : String,
    type : Type?

  annotation Field
  end

  module Serializable
    module Relationship
      record Metadata, id : Int64, label : String, start_id : Int64, end_id : Int64

      @[JSON::Field(ignore: true)]
      @[AGE::Field(ignore: true)]
      getter age_relationship_metadata : Metadata

      macro included
        def self.new(value : Value)
          unless value.type.try(&.edge?)
            raise ::AGE::InvalidRelationship.new("Not a relationship: #{value.inspect}")
          end

          instance = allocate
          instance.initialize(age_graph_relationship: value.string)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end
      end

      def initialize(*, age_graph_relationship : String)
        {% begin %}
          json = JSON::PullParser.new(age_graph_relationship)
          relationship_id = 0i64
          relationship_label = ""
          relationship_start_id = 0i64
          relationship_end_id = 0i64
          {% for ivar in @type.instance_vars %}
            %var{ivar.name} = nil
          {% end %}

          json.read_object do |key|
            location = json.location
            case key
            when "id"
              relationship_id = Int64.new(json)
            when "label"
              relationship_label = json.read_string
            when "start_id"
              relationship_start_id = Int64.new(json)
            when "end_id"
              relationship_end_id = Int64.new(json)
            when "properties"
              json.read_object do |property|
                case property
                  {% for ivar in @type.instance_vars %}
                    {% ann = ivar.annotation(::AGE::Field) %}
                    {% unless ann && ann[:ignore] %}
                      when "{{ivar.name}}"
                        %var{ivar.name} = {{ivar.type}}.new(json)
                    {% end %}
                  {% end %}
                else
                  on_unknown_age_relationship_property(json, key, location)
                end
              end
            end
          end
          @age_relationship_metadata = Metadata.new(
            id: relationship_id,
            label: relationship_label,
            start_id: relationship_start_id,
            end_id: relationship_end_id,
          )

          {% for ivar in @type.instance_vars %}
            {% ann = ivar.annotation(::AGE::Field) %}
            {% unless ann && ann[:ignore] %}
              if %var{ivar.name}
                @{{ivar.name}} = %var{ivar.name}
              else
                {% unless ivar.type.nilable? %}
                  raise SerializableError.new("{{ivar.name}} is expected to be {{ivar.type}}, got nil")
                {% end %}
              end
            {% end %}
          {% end %}
        {% end %}
      end

      private def on_unknown_age_relationship_property(json, key, location)
      end
    end

    module Node
      record Metadata, id : Int64, label : String

      @[AGE::Field(ignore: true)]
      getter age_node_metadata : Metadata

      macro included
        def self.new(value : Value)
          unless value.type.try(&.vertex?)
            raise ::AGE::InvalidNode.new("Not a node: #{value.inspect}")
          end

          instance = allocate
          instance.initialize(age_graph_node: value.string)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end
      end

      def initialize(*, age_graph_node : String)
        {% begin %}
          json = JSON::PullParser.new(age_graph_node)
          node_id = 0i64
          node_label = ""
          {% for ivar in @type.instance_vars %}
            %var{ivar.name} = nil
          {% end %}

          json.read_object do |key|
            location = json.location
            case key
            when "id"
              node_id = Int64.new(json)
            when "label"
              node_label = json.read_string
            when "properties"
              json.read_object do |property|
                case property
                  {% for ivar in @type.instance_vars %}
                    {% ann = ivar.annotation(::AGE::Field) %}
                    {% unless ann && ann[:ignore] %}
                      when "{{ivar.name}}"
                        %var{ivar.name} = {{ivar.type}}.new(json)
                    {% end %}
                  {% end %}
                else
                  on_unknown_age_node_property(json, key, location)
                end
              end
            end
          end
          @age_node_metadata = Metadata.new(node_id, node_label)

          {% for ivar in @type.instance_vars %}
            {% ann = ivar.annotation(::AGE::Field) %}
            {% unless ann && ann[:ignore] %}
              if %var{ivar.name}
                @{{ivar.name}} = %var{ivar.name}
              else
                {% unless ivar.type.nilable? %}
                  raise SerializableError.new("{{ivar.name}} is expected to be {{ivar.type}}, got nil")
                {% end %}
              end
            {% end %}
          {% end %}
        {% end %}
      end

      private def on_unknown_age_node_property(json, key, location)
      end
    end
  end
end

{% for type in %w[Int8 Int16 Int32 Int64 Int128 Float32 Float64 String UUID URI Time] %}
  def {{type.id}}.new(value : AGE::Value)
    from_json(value.string)
  end
{% end %}
