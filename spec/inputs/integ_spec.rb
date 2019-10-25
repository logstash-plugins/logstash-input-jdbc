require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"
require "sequel"
require "sequel/adapters/jdbc"

# This test requires: Firebird installed to Mac OSX, it uses the built-in example database `employee`

describe LogStash::Inputs::Jdbc, :integration => true do
  # This is a necessary change test-wide to guarantee that no local timezone
  # is picked up.  It could be arbitrarily set to any timezone, but then the test
  # would have to compensate differently.  That's why UTC is chosen.
  ENV["TZ"] = "Etc/UTC"
  # For Travis and CI based on docker, we source from ENV
  jdbc_connection_string = ENV.fetch("PG_CONNECTION_STRING",
                                     "jdbc:postgresql://postgresql:5432") + "/jdbc_input_db?user=postgres"

  let(:settings) do
    { "jdbc_driver_class" => "org.postgresql.Driver",
      "jdbc_connection_string" => jdbc_connection_string,
      "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
      "jdbc_user" => "postgres",
      "statement" => 'SELECT FIRST_NAME, LAST_NAME FROM "employee" WHERE EMP_NO = 2'
    }
  end

  let(:plugin) { LogStash::Inputs::Jdbc.new(settings) }
  let(:queue) { Queue.new }

  context "when connecting to a postgres instance" do
    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should populate the event with database entries" do
      plugin.run(queue)
      event = queue.pop
      expect(event.get('first_name')).to eq("Mark")
      expect(event.get('last_name')).to eq("Guckenheimer")
    end
  end

  context "when supplying a non-existent library" do
    let(:settings) do
      super.merge(
          "jdbc_driver_library" => "/no/path/to/postgresql.jar"
      )
    end

    it "should not register correctly" do
      plugin.register
      q = Queue.new
      expect do
        plugin.run(q)
      end.to raise_error(::LogStash::PluginLoadingError)
    end
  end

  context "when connecting to a non-existent server" do
    let(:settings) do
      super.merge(
          "jdbc_connection_string" => "jdbc:postgresql://localhost:65000/somedb"
      )
    end

    it "should not register correctly" do
      plugin.register
      q = Queue.new
      expect do
        plugin.run(q)
      end.to raise_error(::Sequel::DatabaseConnectionError)
    end
  end
end

