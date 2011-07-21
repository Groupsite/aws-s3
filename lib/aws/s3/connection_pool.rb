module AWS
  module S3
    class ConnectionPool
      DEFAULT_MAX_CONNECTION_WAIT = 5.0 # seconds
      DEFAULT_CONNECTION_WAIT = 0.25 # seconds

      class ConnectionUnavailable < StandardError; end

      cattr_accessor :mutex
      self.mutex = Mutex.new

      attr_accessor :connection_options, :pool_size
      attr_reader :max_connection_wait, :connection_wait, :max_connection_attempts
      def initialize(options = {})
        @connection_options = options.dup
        @pool_size = @connection_options.delete(:pool_size) || 5
        @max_connection_wait = @connection_options.delete(:pool_size) || DEFAULT_MAX_CONNECTION_WAIT
        @connection_wait = @connection_options.delete(:pool_size) || DEFAULT_CONNECTION_WAIT
        @max_connection_attempts = @max_connection_wait / @connection_wait

        @connections = []
        @available_connections = []

        @available_connections << new_connection
      end

      # Don't call this if you might have active connections!
      def disconnect!
        ConnectionPool.mutex.synchronize do
          @connections.each do |connection|
            connection.http.finish if connection.persistent?
          end
          @connections = []
        end
      end

      def connections?
        @connections.any?
      end

      def new_connection
        connection = Connection.new(connection_options)
        @connections << connection
        connection
      end

      def get_connection
        ConnectionPool.mutex.synchronize do
          if @available_connections.empty?
            new_connection if @connections.size < pool_size
          else
            @available_connections.shift
          end
        end
      end

      def checkout
        attempts = max_connection_attempts
        until (connection = get_connection) || attempts == 0
          attempts -= 1
          sleep(connection_wait)
        end
        raise ConnectionUnavailable, "Could not get connection from pool size of #{pool_size} in #{max_connection_wait} seconds." unless connection
        connection
      end

      def checkin(connection)
        ConnectionPool.mutex.synchronize do
          @available_connections << connection
        end
      end

      def with_connection(&block)
        connection = checkout
        block.call(connection)
      ensure
        checkin(connection) if connection
      end

      def subdomain
        @connections.first.subdomain
      end
    end
  end
end
