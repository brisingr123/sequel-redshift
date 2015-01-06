require 'sequel/adapters/postgres'

module Sequel
  module Redshift
    include Postgres

    class CreateTableGenerator < Postgres::CreateTableGenerator
      attr_reader :dist_key_name, :sort_key_name

      def dist_key(key)
        @dist_key_name = key
      end

      def sort_key(key)
        @sort_key_name = key
      end
    end

    class Database < Postgres::Database
      set_adapter_scheme :redshift

      def column_definition_primary_key_sql(sql, column)
        result = super
        result << ' IDENTITY' if result
        result
      end

      def serial_primary_key_options
        # redshift doesn't support serial type
        super.merge(serial: false)
      end

      def connection_configuration_sqls
        sqls = []
      end

      def create_table_generator_class
        ::Sequel::Redshift::CreateTableGenerator
      end

      def create_table(name, options=OPTS, &block)
        remove_cached_schema(name)
        options = {:generator=>options} if options.is_a?(Schema::CreateTableGenerator)
        if sql = options[:as]
          raise(Error, "can't provide both :as option and block to create_table") if block
          create_table_as(name, sql, options)
        else
          generator = options[:generator] || create_table_generator(&block)
          base_sql = create_table_sql(name, generator, options)
          base_sql += " distkey (#{generator.dist_key_name})" if generator.dist_key_name
          base_sql += " sortkey (#{generator.sort_key_name})" if generator.sort_key_name
          execute_ddl(base_sql)
          nil
        end

      end

    end

    class Dataset < Postgres::Dataset
      Database::DatasetClass = self

      # Redshift doesn't support RETURNING statement
      def insert_returning_sql(sql)
        # do nothing here
        sql
      end
    end

  end
end
