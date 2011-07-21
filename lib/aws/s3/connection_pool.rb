module AWS
  module S3
    class ConnectionPool
      cattr_accessor :mutex
      self.mutex = Mutex.new

      attr_accessor :connection_options, :pool_size
      def initialize(options = {})
        @connection_options = options.dup
        @pool_size = @connection_options.delete(:pool_size) || 5
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

      def checkout
        ConnectionPool.mutex.synchronize do
          if @available_connections.empty?
            new_connection
          else
            @available_connections.shift
          end
        end
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
